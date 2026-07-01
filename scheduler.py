"""Cron jobs: per-shard hourly/daily roll-ups and periodic top-k refresh."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler

from db import (
    NUM_SHARDS,
    cleanup_old_day_counts,
    cleanup_old_hour_counts,
    rollup_day_to_month,
    rollup_hour_to_day,
)
from topk_service import refresh_topk

logger = logging.getLogger(__name__)


def _hourly_rollup() -> None:
    """
    Roll the just-completed hour bucket into day_counts for every shard,
    then trim stale hour rows.  Runs at HH:01 so the previous hour is
    fully closed before aggregation.
    """
    prev_hour = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d-%H")
    logger.info("Hourly rollup: %s across %d shards", prev_hour, NUM_SHARDS)
    for shard_id in range(NUM_SHARDS):
        rollup_hour_to_day(prev_hour, shard_id)
        cleanup_old_hour_counts(keep_hours=24, shard_id=shard_id)
    refresh_topk()


def _daily_rollup() -> None:
    """
    Roll the just-completed day bucket into month_counts for every shard,
    then trim stale day rows.  Runs at 00:02 UTC.
    """
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info("Daily rollup: %s across %d shards", yesterday, NUM_SHARDS)
    for shard_id in range(NUM_SHARDS):
        rollup_day_to_month(yesterday, shard_id)
        cleanup_old_day_counts(keep_days=31, shard_id=shard_id)
    refresh_topk()


def _topk_refresh() -> None:
    refresh_topk()


def start_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler(timezone="UTC")

    # Refresh Redis every 30 s; first run is immediate so the cache is
    # warm before any GET request arrives.
    scheduler.add_job(
        _topk_refresh,
        "interval",
        seconds=30,
        id="topk_refresh",
        next_run_time=datetime.now(),
    )

    # Roll up completed hour at HH:01
    scheduler.add_job(_hourly_rollup, "cron", minute=1, id="hourly_rollup")

    # Roll up completed day at 00:02 UTC
    scheduler.add_job(_daily_rollup, "cron", hour=0, minute=2, id="daily_rollup")

    scheduler.start()
    logger.info("Scheduler started")
    return scheduler
