import uuid
from dataclasses import dataclass, field
from typing import Protocol
from collections.abc import Callable

from confluent_kafka import Message


@dataclass(slots=True)
class ConsumerMessage:
    """Represents a raw message received from a message broker."""
    topic: str
    partition: int
    offset: int

    timestamp_ms: int  # 时间戳(毫秒)

    payload: bytes

    raw_message: Message # 原始消息对象，用于提交offset

    key: str | None
    msg_id: uuid.UUID = field(default_factory=uuid.uuid4)
    headers: dict | None = None
    decoded_payload: dict | None = None # 反序列化后的payload


class Consumer(Protocol):
    def set_topics(self, topics: list[str]):
        pass

    def register_on_assign(self, func: Callable):
        pass

    def register_on_revoke(self, func: Callable):
        pass

    def register_on_lost(self, func: Callable):
        pass

    def register_on_throttle(self, func: Callable):
        pass

    def register_on_stats(self, func: Callable):
        pass

    def register_on_error(self, func: Callable):
        pass

    async def kafka_available(self) -> bool:
        pass

    async def listen(self) -> list[ConsumerMessage]:
        pass

    async def seek(self, msg: ConsumerMessage):
        pass

    async def pause_partition(self, msg):
        pass

    async def resume_partition(self, msg):
        pass

    async def store_offsets(self, msgs: list[ConsumerMessage]):
        pass

    async def commit(self):
        pass

    async def start(self):
        pass

    async def stop(self):
        pass

