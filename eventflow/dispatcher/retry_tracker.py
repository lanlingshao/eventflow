from eventflow.cache.cache import CacheProvider
from eventflow.cache.local_cache import LocalCache


# 重试计数器: 记录每个事件消费失败后的重试次数
class RetryTracker:
    def __init__(
        self,
        cache: CacheProvider = None,
        expire_seconds: int = 86400,
    ):
        self.cache = cache
        if self.cache is None:
            self.cache = LocalCache()
        self.expire_seconds = expire_seconds

    def _key(self, topic: str, partition: int, offset: int) -> str:
        return f"event_retry:{topic}:{partition}:{offset}"

    async def get(self, topic: str, partition: int, offset: int) -> int:
        key = self._key(topic, partition, offset)
        count = await self.cache.get(key)
        if count is None:
            return 0
        return int(count)

    async def incr(self, topic: str, partition: int, offset: int) -> int:
        key = self._key(topic, partition, offset)
        count = await self.cache.incr(key)
        if count == 1:
            await self.cache.expire(key, self.expire_seconds)
        return count

    async def clear(self, topic: str, partition: int, offset: int):
        key = self._key(topic, partition, offset)
        await self.cache.delete(key)
