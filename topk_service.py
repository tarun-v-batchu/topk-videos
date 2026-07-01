"""
Top-K computation: per-shard heap + k-way merge.

Flow:
  1. For each of the NUM_SHARDS shards, compute the local top-MAX_K using
     a min-heap (O(n_shard * log MAX_K)).
  2. Merge the NUM_SHARDS sorted lists into a single global top-MAX_K list
     using a k-way merge with a max-heap (O(MAX_K * log NUM_SHARDS)).

Because events are routed by hash(video_id) % NUM_SHARDS, a given video
lives in exactly one shard — so the merge never needs to deduplicate.
"""
from __future__ import annotations

import heapq
import logging

from db import (
    NUM_SHARDS,
    get_alltime_video_counts,
    get_last_day_video_counts,
    get_last_hour_video_counts,
    get_last_month_video_counts,
)
from redis_store import redis_store

logger = logging.getLogger(__name__)

MAX_K = 100

_TIMEFRAME_KEY = {
    "last hour": "last_hour",
    "last day": "last_day",
    "last month": "last_month",
    "all time": "all_time",
}

_KEY_FETCHER = {
    "last_hour": get_last_hour_video_counts,
    "last_day": get_last_day_video_counts,
    "last_month": get_last_month_video_counts,
    "all_time": get_alltime_video_counts,
}


def compute_topk(counts: list[tuple[str, int]], k: int) -> list[dict]:
    """
    Local top-k for a single shard using a min-heap.
    O(n log k) time, O(k) space.
    """
    heap: list[tuple[int, str]] = []
    for video_id, count in counts:
        if len(heap) < k:
            heapq.heappush(heap, (count, video_id))
        elif count > heap[0][0]:
            heapq.heapreplace(heap, (count, video_id))
    return [
        {"video_id": vid, "count": cnt}
        for cnt, vid in sorted(heap, key=lambda x: -x[0])
    ]


def merge_topk_lists(shard_results: list[list[dict]], k: int) -> list[dict]:
    """
    K-way merge of per-shard sorted (descending) top-k lists.

    Each shard list is already in descending count order. Videos don't
    repeat across shards (consistent-hash routing), so this is a pure
    merge with no deduplication.

    Uses a max-heap seeded with the first element of each non-empty shard
    list, then advances the pointer into that shard on each pop.

    Time: O(k log NUM_SHARDS).
    """
    # heap entries: (-count, video_id, shard_idx, next_pos_in_that_shard)
    heap: list[tuple[int, str, int, int]] = []

    for shard_idx, results in enumerate(shard_results):
        if results:
            first = results[0]
            heapq.heappush(heap, (-first["count"], first["video_id"], shard_idx, 1))

    output: list[dict] = []
    while heap and len(output) < k:
        neg_cnt, video_id, shard_idx, next_pos = heapq.heappop(heap)
        output.append({"video_id": video_id, "count": -neg_cnt})
        if next_pos < len(shard_results[shard_idx]):
            item = shard_results[shard_idx][next_pos]
            heapq.heappush(
                heap,
                (-item["count"], item["video_id"], shard_idx, next_pos + 1),
            )

    return output


def refresh_topk() -> None:
    """
    Recompute and cache top-MAX_K for every timeframe.

    For each timeframe:
      - Fetch top-MAX_K from every shard independently.
      - K-way merge the 10 sorted lists into one global top-MAX_K.
      - Write the result to Redis.
    """
    for redis_key, fetcher in _KEY_FETCHER.items():
        try:
            shard_results = [
                compute_topk(fetcher(shard_id), MAX_K)
                for shard_id in range(NUM_SHARDS)
            ]
            global_top = merge_topk_lists(shard_results, MAX_K)
            redis_store.set_topk(redis_key, global_top)
            logger.debug(
                "top-k refreshed: %s (%d videos across %d shards)",
                redis_key, len(global_top), NUM_SHARDS,
            )
        except Exception:
            logger.exception("Failed to refresh top-k for %s", redis_key)


def get_topk(timeframe: str, k: int) -> list[dict]:
    """Serve the top-k from the Redis cache for the requested timeframe."""
    redis_key = _TIMEFRAME_KEY.get(timeframe)
    if redis_key is None:
        raise ValueError(f"Unknown timeframe: {timeframe!r}")
    return redis_store.get_topk(redis_key)[:k]
