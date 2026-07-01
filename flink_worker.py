"""
Sharded Flink simulation.

One FlinkWorker per Kafka partition/DB shard. Each worker:
  1. Consumes only from its assigned partition queue
  2. Buffers (video_id, hour_bucket) → count in memory
  3. Flushes to its dedicated shard DB every flush_interval seconds
  4. Fires an optional on_flush callback after each non-empty flush

Workers are staggered by shard_id * (flush_interval / NUM_SHARDS) seconds
so they don't all hit the disk simultaneously (thundering-herd avoidance).
"""
from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from typing import Callable, Optional

from db import upsert_hour_count
from kafka_queue import NUM_SHARDS, ViewEvent, kafka_queue

logger = logging.getLogger(__name__)


class FlinkWorker:
    def __init__(
        self,
        shard_id: int,
        flush_interval: float = 60.0,
        batch_size: int = 200,
        on_flush: Optional[Callable[[], None]] = None,
    ):
        self.shard_id = shard_id
        self.flush_interval = flush_interval
        self.batch_size = batch_size
        self.on_flush = on_flush
        self._buffer: dict[tuple[str, str], int] = defaultdict(int)
        self._buffer_lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    @staticmethod
    def _hour_bucket(ts) -> str:
        return ts.strftime("%Y-%m-%d-%H")

    def _ingest(self, event: ViewEvent) -> None:
        key = (event.video_id, self._hour_bucket(event.timestamp))
        with self._buffer_lock:
            self._buffer[key] += 1

    def _flush(self) -> None:
        with self._buffer_lock:
            if not self._buffer:
                return
            snapshot = dict(self._buffer)
            self._buffer.clear()

        for (video_id, hour_bucket), delta in snapshot.items():
            try:
                upsert_hour_count(video_id, hour_bucket, delta, shard_id=self.shard_id)
            except Exception:
                logger.exception(
                    "shard %d: DB write failed for %s @ %s",
                    self.shard_id, video_id, hour_bucket,
                )

        if self.on_flush:
            try:
                self.on_flush()
            except Exception:
                logger.exception("shard %d: on_flush callback failed", self.shard_id)

    def _run(self) -> None:
        # Stagger flush times across shards to spread disk I/O
        stagger = self.shard_id * (self.flush_interval / NUM_SHARDS)
        last_flush = time.monotonic() - stagger

        while self._running:
            for event in kafka_queue.consume_batch(
                partition=self.shard_id,
                max_size=self.batch_size,
                timeout=0.1,
            ):
                self._ingest(event)

            if time.monotonic() - last_flush >= self.flush_interval:
                self._flush()
                last_flush = time.monotonic()

        self._flush()  # drain buffer on shutdown

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(
            target=self._run,
            name=f"flink-worker-{self.shard_id}",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "Flink worker %d started (flush_interval=%.0fs, stagger=%.1fs)",
            self.shard_id,
            self.flush_interval,
            self.shard_id * (self.flush_interval / NUM_SHARDS),
        )

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Flink worker %d stopped", self.shard_id)


def create_workers(
    flush_interval: float = 60.0,
    on_flush: Optional[Callable[[], None]] = None,
) -> list[FlinkWorker]:
    return [
        FlinkWorker(shard_id=i, flush_interval=flush_interval, on_flush=on_flush)
        for i in range(NUM_SHARDS)
    ]
