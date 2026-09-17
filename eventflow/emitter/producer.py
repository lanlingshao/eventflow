from dataclasses import dataclass
from typing import Protocol


@dataclass
class MessageConfig:
    key: str | None = None
    partition: int | None = None


@dataclass
class ProducerMessage:
    topic: str
    payload: bytes
    config: MessageConfig | None = None


class Producer(Protocol):
    async def flush(self):
        pass

    async def close(self):
        pass

    async def send(self, message: ProducerMessage):
        pass
