# eventflow

[English](README.md)

`eventflow` 是一个面向 Python 与 Kafka 的可扩展异步事件分发框架。它将事件生产、Broker 消费、批量处理、重试投递和死信处理拆分开，使应用可以专注于业务处理逻辑。

> 当前框架仅支持**普通消费模式**。消息处理失败后，消息投递到重试主题或死信主题，随后会提交原始 offset。因此，消费失败后不保证严格顺序。

## 功能

- 基于 `confluent-kafka` 的异步 Kafka 生产者和消费者。
- 通过单个 dispatcher 扩展点批量处理消息。
- 重试消息保留来源 topic、分区、offset、时间戳、错误和重试次数。
- 达到重试上限后自动投递死信队列。
- 同一批次中成功和失败的消息分别处理。
- 支持消费者再均衡回调和优雅停机。
- 提供用于自定义流程的本地内存重试计数器。

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

继承 `EventDispatcher`，声明输入、重试和死信 topic，并实现 `_batch_handler_message`。处理器必须为每个输入消息返回一个 `ConsumeResult`。

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

创建运行所需组件并启动 dispatcher：

```python
from eventflow.broker.kafka.consumer import KafkaConsumer
from eventflow.broker.kafka.producer import KafkaProducer
from eventflow.dispatcher.retry_tracker import RetryTracker
from eventflow.emitter.emitter import EventEmitter

producer = KafkaProducer(producer_config)
emitter = EventEmitter(producer)
consumer = KafkaConsumer(consumer_config)

dispatcher = OrderDispatcher(
    partition_count=24,
    event_emitter=emitter,
    consumer=consumer,
    retry_tracker=RetryTracker(),
)
await dispatcher.run()
```

`partition_count` 必须与 dispatcher 使用的 Kafka topic 分区数一致。

## 消息格式与重试

业务消息使用 JSON payload。`KafkaConsumer` 会通过 `json.loads` 解码每条消息体，因此非 JSON 消息会在进入批量处理器之前失败。

当处理器返回失败结果时，dispatcher 会将下列信封消息投递到重试 topic，直到重试次数达到 `max_retries`（默认值：`3`）：

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

达到重试上限时，同样的信封会被发送到 `dlq_topic`。每个处理结果完成后，原始消息的 offset 会被存储并提交。

## 分区路由

默认情况下，重试和死信消息保持原消息的分区。可以重写 `get_partition_key`，通过稳定的业务键进行路由：

```python
class OrderDispatcher(EventDispatcher):
    # 此处省略 topics、retry_topic、dlq_topic 和 handler
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
