import asyncio

from .producer import Producer, ProducerMessage, MessageConfig


# 为什么不直接操作Producer来发送消息，而是封装一个EventEmitter?
# 1. 应用代理者设计模式，作为代理可以在发送消息时，对消息进行序列化、签名、加密、压缩、限流、重试、超时等；
# 2. 封装Producer，隐藏内部细节，提供更简单的API，如emit()
# 3、后续可以增加链路追踪
class EventEmitter:
    """
    事件发射器
    Used for sending events using the provided producer.

    Wraps a low-level producer and emits structured EventBase instances
    to the appropriate topic with metadata and payload.

    Args:
        producer (Producer): The message producer responsible for sending events.
    """
    def __init__(
        self,
        producer: Producer,
    ):
        self.producer = producer
        self._stop_event = asyncio.Event()

    async def emit(self, topic: str, payload: bytes, message_config: MessageConfig = None):
        producer_msg = ProducerMessage(
            topic=topic,
            payload=payload,
            config=message_config,
        )
        await self.producer.send(producer_msg)

    async def stop(self):
        await self.producer.flush()
        await self.producer.close()
