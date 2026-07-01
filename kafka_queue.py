"""
Partitioned Kafka simulation.

Events are routed to one of NUM_SHARDS partition queues by
hash(video_id) % NUM_SHARDS, so all views for a given video always
land in the same partition (and therefore the same DB shard).
"""
from __future__ import annotations

import hashlib
import queue
from dataclasses import dataclass
from datetime import datetime

NUM_SHARDS = 10


@dataclass
class ViewEvent:
    video_id: str
    timestamp: datetime


class KafkaQueue:
    def __init__(self, num_partitions: int = NUM_SHARDS):
        self.num_partitions = num_partitions
        self._partitions: list[queue.Queue[ViewEvent]] = [
            queue.Queue() for _ in range(num_partitions)
        ]

    def _partition_for(self, video_id: str) -> int:
        # MD5 gives a stable, process-independent partition assignment so the
        # demo script and the server always agree on which shard owns a video.
        digest = int(hashlib.md5(video_id.encode()).hexdigest(), 16)
        return digest % self.num_partitions

    def publish(self, event: ViewEvent) -> None:
        self._partitions[self._partition_for(event.video_id)].put(event)

    def consume_batch(
        self,
        partition: int,
        max_size: int = 200,
        timeout: float = 0.1,
    ) -> list[ViewEvent]:
        """Drain up to max_size events from a single partition."""
        events: list[ViewEvent] = []
        q = self._partitions[partition]
        try:
            events.append(q.get(timeout=timeout))
            while len(events) < max_size:
                events.append(q.get_nowait())
        except queue.Empty:
            pass
        return events


kafka_queue = KafkaQueue()
