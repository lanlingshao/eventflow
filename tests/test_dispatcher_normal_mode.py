import json
import unittest
from unittest.mock import AsyncMock, Mock

from eventflow.dispatcher.consumer import ConsumerMessage
from eventflow.dispatcher.dispatcher import ConsumeResult, EventDispatcher
from eventflow.dispatcher.retry_tracker import RetryTracker
from eventflow.emitter.emitter import EventEmitter
from eventflow.util.partition import get_partition


class RecordingProducer:
    def __init__(self):
        self.messages = []

    async def send(self, message):
        self.messages.append(message)

    async def flush(self):
        pass

    async def close(self):
        pass


class RecordingConsumer:
    def __init__(self):
        self.topics = None
        self.assigned_callback = None
        self.revoked_callback = None
        self.lost_callback = None
        self.stored_messages = []
        self.commit_count = 0
        self.stop_count = 0

    def set_topics(self, topics):
        self.topics = topics

    def register_on_assign(self, callback):
        self.assigned_callback = callback

    def register_on_revoke(self, callback):
        self.revoked_callback = callback

    def register_on_lost(self, callback):
        self.lost_callback = callback

    async def store_offsets(self, messages):
        self.stored_messages.append(messages)

    async def commit(self):
        self.commit_count += 1

    async def stop(self):
        self.stop_count += 1


class NormalDispatcher(EventDispatcher):
    topics = ["orders"]
    retry_topic = "orders.retry"
    dlq_topic = "orders.dlq"

    async def _batch_handler_message(self, msgs):
        return self.results


class KeyedNormalDispatcher(NormalDispatcher):
    def get_partition_key(self, payload, msg):
        return payload["customer_id"]


def make_message(
    *,
    topic="orders",
    partition=3,
    offset=9,
    payload=None,
):
    return ConsumerMessage(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp_ms=1_700_000_000_000,
        payload=b"{}",
        raw_message=None,
        key=None,
        decoded_payload=payload or {"order_id": "o-1", "customer_id": "c-1"},
    )


class NormalModeDispatcherTests(unittest.IsolatedAsyncioTestCase):
    def make_dispatcher(self, dispatcher_type=NormalDispatcher):
        self.producer = RecordingProducer()
        self.consumer = RecordingConsumer()
        dispatcher = dispatcher_type(
            partition_count=8,
            event_emitter=EventEmitter(self.producer),
            consumer=self.consumer,
            retry_tracker=RetryTracker(),
        )
        dispatcher.results = []
        return dispatcher

    async def test_configure_consumer_adds_retry_topic_and_callbacks(self):
        dispatcher = self.make_dispatcher()
        on_assign, on_revoke, on_lost = Mock(), Mock(), Mock()
        dispatcher.on_assign_callback = Mock(return_value=on_assign)
        dispatcher.on_revoke_callback = Mock(return_value=on_revoke)
        dispatcher.on_lost_callback = Mock(return_value=on_lost)

        dispatcher.configure_consumer()

        self.assertEqual(self.consumer.topics, ["orders", "orders.retry"])
        self.assertIs(self.consumer.assigned_callback, on_assign)
        self.assertIs(self.consumer.revoked_callback, on_revoke)
        self.assertIs(self.consumer.lost_callback, on_lost)

    async def test_success_and_failure_are_committed_and_failure_goes_to_retry(self):
        dispatcher = self.make_dispatcher()
        succeeded = make_message(offset=10)
        failed = make_message(offset=11)

        await dispatcher._process_normal_results(
            [
                ConsumeResult(msg=succeeded, success=True),
                ConsumeResult(msg=failed, success=False, error=ValueError("invalid order")),
            ]
        )

        self.assertEqual(self.consumer.stored_messages, [[succeeded, failed]])
        self.assertEqual(self.consumer.commit_count, 1)
        self.assertEqual(len(self.producer.messages), 1)
        retry = self.producer.messages[0]
        self.assertEqual(retry.topic, "orders.retry")
        self.assertEqual(retry.config.partition, failed.partition)
        retry_payload = json.loads(retry.payload)
        self.assertEqual(retry_payload["payload"], failed.decoded_payload)
        self.assertEqual(retry_payload["meta"]["retry_count"], 1)
        self.assertEqual(retry_payload["meta"]["origin_offset"], failed.offset)
        self.assertEqual(retry_payload["meta"]["error"], "ValueError('invalid order')")

    async def test_message_at_retry_limit_is_sent_to_dlq(self):
        dispatcher = self.make_dispatcher()
        retry_message = make_message(
            payload={
                "meta": {"retry_count": dispatcher.max_retries - 1},
                "payload": {"order_id": "o-2", "customer_id": "c-2"},
            }
        )

        await dispatcher._process_normal_results(
            [ConsumeResult(msg=retry_message, success=False, error=RuntimeError("failed again"))]
        )

        dlq = self.producer.messages[0]
        self.assertEqual(dlq.topic, "orders.dlq")
        self.assertEqual(dlq.config.partition, retry_message.partition)
        dlq_payload = json.loads(dlq.payload)
        self.assertEqual(dlq_payload["payload"], {"order_id": "o-2", "customer_id": "c-2"})
        self.assertEqual(dlq_payload["meta"]["retry_count"], dispatcher.max_retries)
        self.assertEqual(self.consumer.stored_messages, [[retry_message]])
        self.assertEqual(self.consumer.commit_count, 1)

    async def test_partition_key_overrides_source_partition_for_retry_routing(self):
        dispatcher = self.make_dispatcher(KeyedNormalDispatcher)
        message = make_message(partition=6)

        await dispatcher._send_to_retry(message, ValueError("bad"), retry_count=1)

        retry = self.producer.messages[0]
        self.assertEqual(
            retry.config.partition,
            get_partition(message.decoded_payload["customer_id"], dispatcher.partition_count),
        )

    async def test_stop_is_idempotent_and_stops_consumer_once(self):
        dispatcher = self.make_dispatcher()

        await dispatcher.stop()
        await dispatcher.stop()

        self.assertTrue(dispatcher.stopped)
        self.assertEqual(self.consumer.stop_count, 1)
