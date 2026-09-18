import unittest
from unittest.mock import AsyncMock

from eventflow.cache.local_cache import LocalCache
from eventflow.dispatcher.retry_tracker import RetryTracker
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


class RetryTrackerTests(unittest.IsolatedAsyncioTestCase):
    async def test_counts_are_scoped_to_the_source_message_and_can_be_cleared(self):
        tracker = RetryTracker()

        self.assertEqual(await tracker.get("orders", 0, 12), 0)
        self.assertEqual(await tracker.incr("orders", 0, 12), 1)
        self.assertEqual(await tracker.incr("orders", 0, 12), 2)
        self.assertEqual(await tracker.get("orders", 1, 12), 0)

        await tracker.clear("orders", 0, 12)

        self.assertEqual(await tracker.get("orders", 0, 12), 0)

    async def test_local_cache_expires_entries(self):
        cache = LocalCache()

        await cache.incr("retry-count")
        self.assertTrue(await cache.expire("retry-count", 0))

        self.assertIsNone(await cache.get("retry-count"))
        self.assertEqual(await cache.incr("retry-count"), 1)

