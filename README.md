# eventflow

[中文文档](README_CN.md)

`eventflow` is an extensible, asynchronous event-dispatching framework for Python and Kafka. It separates event production, broker consumption, batch handling, retry delivery, and dead-letter handling so applications can focus on their business handlers.

> The framework currently supports **normal consumption mode** only. When a message fails, the original offset is committed after the message is delivered to a retry topic or dead-letter topic. Therefore, it does not provide strict in-order processing after a failure.

## Features

- Async Kafka producer and consumer built on `confluent-kafka`.
- Batch message handling through a single dispatcher extension point.
- Retry envelopes that retain the source topic, partition, offset, timestamp, error, and retry count.
- Dead-letter routing after the configured retry limit.
- Per-message result handling: successful and failed records in the same batch are handled independently.
- Consumer rebalance hooks and graceful shutdown support.
- A local in-memory retry counter implementation for custom workflows.

## Requirements

- Python 3.11+
- A reachable Kafka cluster
- The source topic, retry topic, and dead-letter topic must use the same partition count when preserving source-partition routing.

## Installation

Install the project dependencies with [uv](https://docs.astral.sh/uv/):

```bash
uv sync
```

Or install the package dependencies with your preferred Python environment manager:

```bash
pip install confluent-kafka==2.13.0 mmh3==5.2.0
```

## Quick start

The runnable normal-mode example is in [`examples/normal_mode`](examples/normal_mode).

1. Update the Kafka connection settings in `examples/normal_mode/conf.py`.
2. Create the three topics defined in `examples/normal_mode/constant.py` with the same number of partitions:

   ```bash
   kafka-topics --bootstrap-server 127.0.0.1:9092 --create --topic test.event --partitions 24 --replication-factor 1
   kafka-topics --bootstrap-server 127.0.0.1:9092 --create --topic test.retry --partitions 24 --replication-factor 1
   kafka-topics --bootstrap-server 127.0.0.1:9092 --create --topic test.dlq --partitions 24 --replication-factor 1
   ```

3. Start the dispatcher in one terminal:

   ```bash
   uv run python -m examples.normal_mode.dispatcher
   ```

4. Start the emitter in another terminal:

   ```bash
   uv run python -m examples.normal_mode.emitter
   ```

The sample emitter publishes JSON events. The sample dispatcher deliberately fails the event whose `event_id` is `1`, which lets you observe retry and dead-letter behavior.

## Implementing a dispatcher

Subclass `EventDispatcher`, declare the input, retry, and dead-letter topics, and implement `_batch_handler_message`. The handler must return one `ConsumeResult` for every input message.

```python
from eventflow.dispatcher.dispatcher import ConsumeResult, EventDispatcher


class OrderDispatcher(EventDispatcher):
    topics = ["orders"]
    retry_topic = "orders.retry"
    dlq_topic = "orders.dlq"

    async def _batch_handler_message(self, messages):
        results = []
        for message in messages:
            try:
                payload = self._get_business_payload(message)
                await handle_order(payload)
                results.append(ConsumeResult(msg=message, success=True))
            except Exception as exc:
                results.append(ConsumeResult(msg=message, success=False, error=exc))
        return results
```

Create the runtime components and run the dispatcher:

```python
from eventflow.broker.kafka.consumer import KafkaConsumer
from eventflow.broker.kafka.producer import KafkaProducer
from eventflow.emitter.emitter import EventEmitter

producer = KafkaProducer(producer_config)
emitter = EventEmitter(producer)
consumer = KafkaConsumer(consumer_config)

dispatcher = OrderDispatcher(
    partition_count=24,
    event_emitter=emitter,
    consumer=consumer,
)
await dispatcher.run()
```

`partition_count` must match the Kafka topic partition count used for the dispatcher topics.

## Message format and retries

Application messages are JSON payloads. `KafkaConsumer` decodes each message body using `json.loads`, so a non-JSON payload will fail before it reaches the batch handler.

When a handler reports a failure, the dispatcher sends this envelope to the retry topic until the retry count reaches `max_retries` (default: `3`):

```json
{
  "meta": {
    "origin_topic": "orders",
    "origin_partition": 3,
    "origin_offset": 42,
    "retry_count": 1,
    "timestamp": 1700000000000,
    "error": "ValueError('invalid order')"
  },
  "payload": {
    "order_id": "o-42"
  }
}
```

The retry topic is automatically included in the consumer subscription. `_get_business_payload(message)` unwraps this envelope, allowing the same handler to process original and retried messages.

At the retry limit, the same envelope is sent to `dlq_topic`. The original source offset is stored and committed after each result has been processed.

## Partition routing

By default, retry and dead-letter messages use the source message's partition. Override `get_partition_key` to route them by a stable business key instead:

```python
class OrderDispatcher(EventDispatcher):
    # topics, retry_topic, dlq_topic, and handler omitted
    def get_partition_key(self, payload, message):
        return payload["customer_id"]
```

The key is hashed with `mmh3` and reduced modulo `partition_count`.

## Lifecycle hooks

Dispatchers may override these asynchronous hooks:

- `before_start()` for initialization before the Kafka consumer starts.
- `start_background_tasks()` for long-running supporting tasks; use `add_background_task(coro)` to register them for shutdown.
- `before_batch_handler(messages)` and `after_batch_handler(results)` for batch-level instrumentation or setup.
- `on_assign_callback()`, `on_revoke_callback()`, and `on_lost_callback()` for consumer-group rebalance notifications.

## Testing

The test suite is Kafka-free and uses fakes for broker-facing components:

```bash
uv run python -m unittest discover -s tests -v
```

## License

[MIT](LICENSE)
