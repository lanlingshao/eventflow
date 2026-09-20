# eventflow

[中文文档](README_CN.md)

`eventflow` is an extensible, asynchronous event-dispatching framework for Python and Kafka. It separates event production, broker consumption, batch handling, and failure handling so applications can focus on their business handlers.

> The framework currently supports **normal consumption mode** only. A `FailureHandlingStrategy` determines whether a failed message is acknowledged, sent to a retry topic, or sent to a dead-letter topic. When that action completes, the original offset is committed; therefore, failures do not preserve strict processing order.

## Features

- Async Kafka producer and consumer built on `confluent-kafka`.
- Batch message handling through a single dispatcher extension point.
- Pluggable `FailureHandlingStrategy` implementations for per-message failure decisions.
- Built-in acknowledge-only and bounded-retry/dead-letter strategies.
- Retry and dead-letter envelopes that retain the source topic, partition, offset, timestamp, error, and retry count.
- Per-message result handling: successful and failed records in the same batch are handled independently.
- Consumer rebalance hooks and graceful shutdown support.

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

Subclass `EventDispatcher` and implement `_batch_handler_message`. The handler must return one `ConsumeResult` for every input message. Configure input and failure-routing topics, plus the failure strategy, when creating the dispatcher.

```python
from eventflow.dispatcher.dispatcher import ConsumeResult, EventDispatcher


class OrderDispatcher(EventDispatcher):
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
from eventflow.dispatcher.failure import MaxRetryStrategy
from eventflow.emitter.emitter import EventEmitter

producer = KafkaProducer(producer_config)
emitter = EventEmitter(producer)
consumer = KafkaConsumer(consumer_config)

dispatcher = OrderDispatcher(
    partition_count=24,
    event_emitter=emitter,
    consumer=consumer,
    topics=["orders"],
    retry_topic="orders.retry",
    dlq_topic="orders.dlq",
    failure_strategy=MaxRetryStrategy(max_retries=3),
)
await dispatcher.run()
```

`partition_count` must match the Kafka topic partition count used for the dispatcher topics.

## Failure handling

`failure_strategy` receives a `FailureContext` for every failed `ConsumeResult`. It returns a `FailureDecision` with one of these actions:

- `ACK`: acknowledge and commit the failed source message without publishing another message.
- `RETRY`: publish a retry envelope and then commit the failed source message.
- `DLQ`: publish a dead-letter envelope and then commit the failed source message.

`NoopFailureStrategy` is the default and always returns `ACK`. Use `MaxRetryStrategy` to retry messages and route them to a dead-letter topic after the limit:

```python
from eventflow.dispatcher.failure import MaxRetryStrategy

failure_strategy = MaxRetryStrategy(max_retries=3)
```

With this strategy, a message whose current retry count is lower than `max_retries` is sent to the retry topic with its count incremented. Once its current count reaches the limit, it is sent to the DLQ with the count incremented.

Strategies declare the actions they may return through `actions`. Dispatcher configuration is validated before startup: a strategy that can return `RETRY` requires `retry_topic`; one that can return `DLQ` requires `dlq_topic`. Implement `FailureHandlingStrategy` to define custom policies.

## Message format for retry and DLQ

Application messages are JSON payloads. `KafkaConsumer` decodes each message body using `json.loads`, so a non-JSON payload will fail before it reaches the batch handler.

When a strategy chooses `RETRY` or `DLQ`, the dispatcher publishes this envelope to the corresponding topic:

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

The original source offset is stored and committed only after the selected failure action has completed.

## Partition routing

By default, retry and dead-letter messages use the source message's partition. Override `get_partition_key` to route them by a stable business key instead:

```python
class OrderDispatcher(EventDispatcher):
    # Handler omitted
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
