# eventflow

[English](README.md)

`eventflow` 是一个面向 Python 与 Kafka 的可扩展异步事件分发框架。它将事件生产、Broker 消费、批量处理和失败处理拆分开，使应用可以专注于业务处理逻辑。

> 当前框架仅支持**普通消费模式**。`FailureHandlingStrategy` 决定失败消息是确认、投递到重试主题，还是投递到死信主题。相应操作完成后会提交原始 offset，因此消费失败后不保证严格顺序。

## 功能

- 基于 `confluent-kafka` 的异步 Kafka 生产者和消费者。
- 通过单个 dispatcher 扩展点批量处理消息。
- 可插拔的 `FailureHandlingStrategy`，可按消息决定失败处理动作。
- 内置仅确认及限定次数重试/死信策略。
- 重试和死信消息保留来源 topic、分区、offset、时间戳、错误和重试次数。
- 同一批次中成功和失败的消息分别处理。
- 支持消费者再均衡回调和优雅停机。

## 环境要求

- Python 3.11+
- 可连接的 Kafka 集群
- 采用源分区路由时，业务 topic、重试 topic 与死信 topic 必须拥有相同的分区数。

## 安装

使用 [uv](https://docs.astral.sh/uv/) 安装项目依赖：

```bash
uv sync
```

也可以使用其他 Python 环境管理工具安装依赖：

```bash
pip install confluent-kafka==2.13.0 mmh3==5.2.0
```

## 快速开始

可直接运行的普通模式示例位于 [`examples/normal_mode`](examples/normal_mode)。

1. 在 `examples/normal_mode/conf.py` 中修改 Kafka 连接配置。
2. 按照 `examples/normal_mode/constant.py` 的定义创建三个分区数相同的 topic：

   ```bash
   kafka-topics --bootstrap-server 127.0.0.1:9092 --create --topic test.event --partitions 24 --replication-factor 1
   kafka-topics --bootstrap-server 127.0.0.1:9092 --create --topic test.retry --partitions 24 --replication-factor 1
   kafka-topics --bootstrap-server 127.0.0.1:9092 --create --topic test.dlq --partitions 24 --replication-factor 1
   ```

3. 在一个终端启动 dispatcher：

   ```bash
   uv run python -m examples.normal_mode.dispatcher
   ```

4. 在另一个终端启动 emitter：

   ```bash
   uv run python -m examples.normal_mode.emitter
   ```

示例 emitter 会发布 JSON 事件；示例 dispatcher 会故意让 `event_id` 为 `1` 的事件失败，以便观察重试和死信行为。

## 编写 Dispatcher

继承 `EventDispatcher` 并实现 `_batch_handler_message`。处理器必须为每个输入消息返回一个 `ConsumeResult`。在创建 dispatcher 时配置输入与失败路由 topic，以及失败处理策略。

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

创建运行所需组件并启动 dispatcher：

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

`partition_count` 必须与 dispatcher 使用的 Kafka topic 分区数一致。

## 消费失败处理

每个失败的 `ConsumeResult` 都会由 `failure_strategy` 接收对应的 `FailureContext`，并返回包含以下动作之一的 `FailureDecision`：

- `ACK`：确认并提交失败的源消息，不再发布新消息。
- `RETRY`：发布重试信封消息，然后提交失败的源消息。
- `DLQ`：发布死信信封消息，然后提交失败的源消息。

默认策略是始终返回 `ACK` 的 `NoopFailureStrategy`。如需重试并在达到上限后投递死信队列，可使用 `MaxRetryStrategy`：

```python
from eventflow.dispatcher.failure import MaxRetryStrategy

failure_strategy = MaxRetryStrategy(max_retries=3)
```

该策略会在消息当前重试次数小于 `max_retries` 时投递重试 topic，并将次数加一；当前次数达到上限后，则投递 DLQ，并将次数加一。

策略通过 `actions` 声明其可能返回的动作。dispatcher 在启动前校验配置：可能返回 `RETRY` 的策略必须配置 `retry_topic`，可能返回 `DLQ` 的策略必须配置 `dlq_topic`。如有其他处理流程，可自行实现 `FailureHandlingStrategy`。

## 重试与死信消息格式

业务消息使用 JSON payload。`KafkaConsumer` 会通过 `json.loads` 解码每条消息体，因此非 JSON 消息会在进入批量处理器之前失败。

当策略选择 `RETRY` 或 `DLQ` 时，dispatcher 会将下列信封消息投递到对应 topic：

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

重试 topic 会自动加入消费者订阅。`_get_business_payload(message)` 会解开该信封，因此同一个处理器可以处理原始消息和重试消息。

只有在选定的失败处理动作完成后，原始消息的 offset 才会被存储并提交。

## 分区路由

默认情况下，重试和死信消息保持原消息的分区。可以重写 `get_partition_key`，通过稳定的业务键进行路由：

```python
class OrderDispatcher(EventDispatcher):
    # 此处省略 handler
    def get_partition_key(self, payload, message):
        return payload["customer_id"]
```

该键会通过 `mmh3` 哈希，再对 `partition_count` 取模。

## 生命周期钩子

Dispatcher 可以重写以下异步钩子：

- `before_start()`：Kafka consumer 启动前的初始化。
- `start_background_tasks()`：启动长时间运行的辅助任务；通过 `add_background_task(coro)` 注册的任务会在关闭时等待完成。
- `before_batch_handler(messages)` 和 `after_batch_handler(results)`：批量级别的埋点或准备工作。
- `on_assign_callback()`、`on_revoke_callback()`、`on_lost_callback()`：消费者组再均衡通知。

## 测试

测试套件不依赖 Kafka，使用模拟的 Broker 接口组件：

```bash
uv run python -m unittest discover -s tests -v
```

## 许可证

[MIT](LICENSE)
