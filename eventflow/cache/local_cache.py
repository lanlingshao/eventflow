import time
from typing import Any

from eventflow.cache.cache import CacheProvider


class LocalCache(CacheProvider):
    """
    Local in-memory cache.

    Provides a Redis-like interface for the operations required
    by RetryTracker.
    """

    def __init__(self):
        self._data: dict[str, Any] = {}
        self._expires: dict[str, float] = {}

    def _is_expired(self, key: str) -> bool:
        expire_at = self._expires.get(key)

        if expire_at is None:
            return False

        if time.monotonic() >= expire_at:
            self._data.pop(key, None)
            self._expires.pop(key, None)
            return True

        return False

    async def get(self, name: str) -> Any:
        if self._is_expired(name):
            return None

        return self._data.get(name)

    async def incr(self, name: str) -> int:
        if self._is_expired(name):
            self._data.pop(name, None)

        value = int(self._data.get(name, 0)) + 1
        self._data[name] = value

        return value

    async def expire(self, name: str, ex: int) -> bool:
        if self._is_expired(name):
            return False

        if name not in self._data:
            return False

        self._expires[name] = time.monotonic() + ex

        return True

    async def delete(self, name: str) -> int:
        existed = name in self._data

        self._data.pop(name, None)
        self._expires.pop(name, None)

        return 1 if existed else 0
