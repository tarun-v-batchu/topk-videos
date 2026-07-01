"""
Sharded SQLite View DB.

Each of the NUM_SHARDS shards is a separate SQLite file
(topk_shard_0.db … topk_shard_9.db) with its own write lock,
so the 10 Flink workers can flush in parallel without contention.

All write/read functions take a shard_id argument so callers are
explicit about which shard they target.
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta

from kafka_queue import NUM_SHARDS

_db_locks = [threading.Lock() for _ in range(NUM_SHARDS)]


def _db_path(shard_id: int) -> str:
    return f"topk_shard_{shard_id}.db"


@contextmanager
def get_db(shard_id: int):
    conn = sqlite3.connect(_db_path(shard_id), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


_SCHEMA = """
    CREATE TABLE IF NOT EXISTS hour_counts (
        video_id    TEXT NOT NULL,
        hour_bucket TEXT NOT NULL,
        count       INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (video_id, hour_bucket)
    );
    CREATE TABLE IF NOT EXISTS day_counts (
        video_id   TEXT NOT NULL,
        day_bucket TEXT NOT NULL,
        count      INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (video_id, day_bucket)
    );
    CREATE TABLE IF NOT EXISTS month_counts (
        video_id     TEXT NOT NULL,
        month_bucket TEXT NOT NULL,
        count        INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (video_id, month_bucket)
    );
    CREATE TABLE IF NOT EXISTS alltime_counts (
        video_id TEXT PRIMARY KEY,
        count    INTEGER NOT NULL DEFAULT 0
    );
"""


def init_db() -> None:
    """Initialise all shard databases."""
    for shard_id in range(NUM_SHARDS):
        with get_db(shard_id) as conn:
            conn.executescript(_SCHEMA)


# ---------------------------------------------------------------------------
# Write path (called by Flink workers)
# ---------------------------------------------------------------------------

def upsert_hour_count(video_id: str, hour_bucket: str, delta: int, shard_id: int) -> None:
    """Increment hour_counts and alltime_counts in the given shard."""
    with _db_locks[shard_id]:
        with get_db(shard_id) as conn:
            conn.execute(
                """
                INSERT INTO hour_counts (video_id, hour_bucket, count)
                VALUES (?, ?, ?)
                ON CONFLICT (video_id, hour_bucket)
                DO UPDATE SET count = count + excluded.count
                """,
                (video_id, hour_bucket, delta),
            )
            conn.execute(
                """
                INSERT INTO alltime_counts (video_id, count)
                VALUES (?, ?)
                ON CONFLICT (video_id)
                DO UPDATE SET count = count + excluded.count
                """,
                (video_id, delta),
            )


# ---------------------------------------------------------------------------
# Roll-up path (called by scheduler cron jobs)
# ---------------------------------------------------------------------------

def rollup_hour_to_day(hour_bucket: str, shard_id: int) -> None:
    """Sum a completed hour bucket into day_counts for one shard."""
    day_bucket = hour_bucket[:10]
    with _db_locks[shard_id]:
        with get_db(shard_id) as conn:
            rows = conn.execute(
                "SELECT video_id, count FROM hour_counts WHERE hour_bucket = ?",
                (hour_bucket,),
            ).fetchall()
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO day_counts (video_id, day_bucket, count)
                    VALUES (?, ?, ?)
                    ON CONFLICT (video_id, day_bucket)
                    DO UPDATE SET count = count + excluded.count
                    """,
                    (row["video_id"], day_bucket, row["count"]),
                )


def rollup_day_to_month(day_bucket: str, shard_id: int) -> None:
    """Sum a completed day bucket into month_counts for one shard."""
    month_bucket = day_bucket[:7]
    with _db_locks[shard_id]:
        with get_db(shard_id) as conn:
            rows = conn.execute(
                "SELECT video_id, count FROM day_counts WHERE day_bucket = ?",
                (day_bucket,),
            ).fetchall()
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO month_counts (video_id, month_bucket, count)
                    VALUES (?, ?, ?)
                    ON CONFLICT (video_id, month_bucket)
                    DO UPDATE SET count = count + excluded.count
                    """,
                    (row["video_id"], month_bucket, row["count"]),
                )


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def cleanup_old_hour_counts(keep_hours: int = 24, shard_id: int = 0) -> None:
    cutoff = (datetime.now() - timedelta(hours=keep_hours)).strftime("%Y-%m-%d-%H")
    with _db_locks[shard_id]:
        with get_db(shard_id) as conn:
            conn.execute("DELETE FROM hour_counts WHERE hour_bucket < ?", (cutoff,))


def cleanup_old_day_counts(keep_days: int = 31, shard_id: int = 0) -> None:
    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    with _db_locks[shard_id]:
        with get_db(shard_id) as conn:
            conn.execute("DELETE FROM day_counts WHERE day_bucket < ?", (cutoff,))


# ---------------------------------------------------------------------------
# Read path (called by top-k service, per shard)
# ---------------------------------------------------------------------------

def get_last_hour_video_counts(shard_id: int) -> list[tuple[str, int]]:
    """Counts from the current hour bucket (falls back to previous hour)."""
    current = datetime.now().strftime("%Y-%m-%d-%H")
    prev = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d-%H")
    with get_db(shard_id) as conn:
        rows = conn.execute(
            "SELECT video_id, count FROM hour_counts WHERE hour_bucket = ?",
            (current,),
        ).fetchall()
        if not rows:
            rows = conn.execute(
                "SELECT video_id, count FROM hour_counts WHERE hour_bucket = ?",
                (prev,),
            ).fetchall()
        return [(r["video_id"], r["count"]) for r in rows]


def get_last_day_video_counts(shard_id: int) -> list[tuple[str, int]]:
    """Sum of hour_counts across the last 24 hours for one shard."""
    cutoff = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d-%H")
    with get_db(shard_id) as conn:
        rows = conn.execute(
            """
            SELECT video_id, SUM(count) AS total
            FROM hour_counts
            WHERE hour_bucket >= ?
            GROUP BY video_id
            """,
            (cutoff,),
        ).fetchall()
        return [(r["video_id"], r["total"]) for r in rows]


def get_last_month_video_counts(shard_id: int) -> list[tuple[str, int]]:
    """Sum of day_counts for last 30 days plus today's in-progress hours."""
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")
    with get_db(shard_id) as conn:
        day_rows = conn.execute(
            """
            SELECT video_id, SUM(count) AS total
            FROM day_counts
            WHERE day_bucket >= ?
            GROUP BY video_id
            """,
            (thirty_days_ago,),
        ).fetchall()
        hour_rows = conn.execute(
            """
            SELECT video_id, SUM(count) AS total
            FROM hour_counts
            WHERE hour_bucket LIKE ?
            GROUP BY video_id
            """,
            (f"{today}-%",),
        ).fetchall()
        counts: dict[str, int] = {r["video_id"]: r["total"] for r in day_rows}
        for r in hour_rows:
            counts[r["video_id"]] = counts.get(r["video_id"], 0) + r["total"]
        return list(counts.items())


def get_alltime_video_counts(shard_id: int) -> list[tuple[str, int]]:
    """All-time counts for one shard (kept live by the Flink worker)."""
    with get_db(shard_id) as conn:
        rows = conn.execute("SELECT video_id, count FROM alltime_counts").fetchall()
        return [(r["video_id"], r["count"]) for r in rows]
