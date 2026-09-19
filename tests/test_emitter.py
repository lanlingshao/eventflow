import unittest
from unittest.mock import AsyncMock

from eventflow.emitter.emitter import EventEmitter
from eventflow.emitter.producer import MessageConfig, ProducerMessage


class EventEmitterTests(unittest.IsolatedAsyncioTestCase):
    async def test_emit_builds_and_sends_a_producer_message(self):
        producer = AsyncMock()
        emitter = EventEmitter(producer)
        config = MessageConfig(key="customer-1", partition=2)

        await emitter.emit("events", b'{"id": 1}', config)

        producer.send.assert_awaited_once_with(
            ProducerMessage(topic="events", payload=b'{"id": 1}', config=config)
        )

    async def test_stop_flushes_before_closing_producer(self):
        producer = AsyncMock()
        emitter = EventEmitter(producer)

        await emitter.stop()

        self.assertEqual(
            [call[0] for call in producer.mock_calls], ["flush", "close"]
        )
        producer.flush.assert_awaited_once()
        producer.close.assert_awaited_once()

