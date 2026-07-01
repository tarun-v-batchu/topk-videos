"""
YouTube Top-K API

POST /watched            – ingest a view event
GET  /top_videos         – return top-k videos for a given timeframe
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from db import init_db
from flink_worker import create_workers
from kafka_queue import ViewEvent, kafka_queue
from scheduler import start_scheduler
from topk_service import MAX_K, _TIMEFRAME_KEY, get_topk, refresh_topk

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

VALID_TIMEFRAMES = set(_TIMEFRAME_KEY.keys())


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    flush_interval = float(os.getenv("FLINK_FLUSH_INTERVAL", "60"))
    workers = create_workers(flush_interval=flush_interval, on_flush=refresh_topk)
    for w in workers:
        w.start()

    scheduler = start_scheduler()
    yield

    for w in workers:
        w.stop()
    scheduler.shutdown(wait=False)


app = FastAPI(title="YouTube Top-K API", version="2.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Request model
# ---------------------------------------------------------------------------

class WatchedRequest(BaseModel):
    videoId: str
    timestamp: str  # ISO 8601, e.g. "2024-06-01T14:32:00"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/watched", status_code=202)
def post_watched(body: WatchedRequest):
    """
    Record a video view.

    The event is published to the Kafka-like queue and routed to the
    partition for hash(videoId) % NUM_SHARDS.  The assigned Flink worker
    will aggregate it and flush to its DB shard within flush_interval seconds.
    """
    video_id = body.videoId.strip()
    if not video_id:
        raise HTTPException(status_code=400, detail="videoId must not be empty")

    try:
        ts = datetime.fromisoformat(body.timestamp)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid timestamp. Use ISO 8601, e.g. '2024-06-01T14:32:00'",
        )

    kafka_queue.publish(ViewEvent(video_id=video_id, timestamp=ts))
    return {"status": "accepted", "videoId": video_id, "timestamp": body.timestamp}


@app.get("/top_videos")
def get_top_videos(
    k: int = Query(
        ...,
        ge=1,
        lt=MAX_K,
        description=f"Number of results to return (1–{MAX_K - 1})",
    ),
    timeframe: str = Query(
        ...,
        description='One of: "last hour", "last day", "last month", "all time"',
    ),
):
    """
    Return the top-k most-viewed videos for the requested timeframe.

    Results come from a Redis cache that is rebuilt by a k-way merge of
    per-shard top-k lists.  The cache is refreshed after every Flink flush
    and at most 30 seconds stale otherwise.
    """
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe. Must be one of: {sorted(VALID_TIMEFRAMES)}",
        )

    try:
        videos = get_topk(timeframe, k)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {"timeframe": timeframe, "k": k, "videos": videos}
