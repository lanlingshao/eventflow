from typing import Protocol


class CacheProvider(Protocol):

    async def get(self, name: str):
        ...

    async def incr(self, name: str) -> int:
        ...

    async def expire(self, name: str, time: int) -> bool:
        ...

    async def delete(self, name: str) -> int:
        ...
