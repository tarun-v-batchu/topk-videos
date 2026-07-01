"""Redis client with transparent in-memory fallback."""
from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


class _InMemoryBackend:
    def __init__(self):
        self._data: dict[str, str] = {}

    def set(self, key: str, value: str) -> None:
        self._data[key] = value

    def get(self, key: str) -> "str | None":
        return self._data.get(key)

    def ping(self) -> bool:
        return True


class RedisStore:
    def __init__(self, host: str = "localhost", port: int = 6379):
        self._backend = None
        try:
            import redis

            client = redis.Redis(host=host, port=port, decode_responses=True)
            client.ping()
            self._backend = client
            logger.info("RedisStore: connected to Redis at %s:%d", host, port)
        except Exception as exc:
            logger.warning("RedisStore: Redis unavailable (%s), using in-memory fallback", exc)
            self._backend = _InMemoryBackend()

    def set_topk(self, timeframe: str, data: list[dict]) -> None:
        self._backend.set(f"topk:{timeframe}", json.dumps(data))

    def get_topk(self, timeframe: str) -> list[dict]:
        raw = self._backend.get(f"topk:{timeframe}")
        return json.loads(raw) if raw else []


redis_store = RedisStore()
