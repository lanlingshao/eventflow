import asyncio
import inspect
import json
import logging
from collections.abc import Callable

from confluent_kafka import KafkaException, Message, TopicPartition
from confluent_kafka.aio import AIOConsumer

from eventflow.dispatcher.consumer import Consumer, ConsumerMessage

kafka_logger = logging.getLogger("kafka_consumer")
logger = logging.getLogger(__name__)

# 参考官方文档: https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/asyncio_example.py
class KafkaConsumer(Consumer):
    def __init__(
        self,
        config: dict,
        topics: list[str] = None,
        batch_size: int = 100,
        poll_timeout: float = 5,
    ):
        self.config = config
        self.topics = topics
        self.batch_size = batch_size
        self.poll_timeout = poll_timeout
        self.consumer: AIOConsumer | None = None

        # rebalance callbacks
        self._on_assign_callbacks = []
        self._on_revoke_callbacks = []
        self._on_lost_callbacks = []

        # other callbacks
        self._on_error_callback = None
        self._on_throttle_callback = None
        self._on_stats_callback = None

    def set_topics(self, topics: list[str]):
        self.topics = topics

    # 使用回调的原因： 解构，避免kafkaConsumer中引入RuleEngine，因为kafkaConsumer是底层模块，不能引入业务逻辑（RuleEngine）
    def register_on_assign(self, func: Callable):
        self._on_assign_callbacks.append(func)

    def register_on_revoke(self, func: Callable):
        self._on_revoke_callbacks.append(func)

    def register_on_lost(self, func: Callable):
        self._on_lost_callbacks.append(func)

    def register_on_error(self, func: Callable):
        # 异常广播频道
        # 触发场景：
        # - broker 断开
        # - 网络错误
        # - 超时
        # - group 协调失败
        # - 认证问题
        # - leader 不可用
        # - partition 错误
        # - rebalance 异常
        self._on_error_callback = func

    def register_on_throttle(self, func: Callable):
        # Broker 限流通知
        # 触发场景：
        # - broker 过载
        # - quota 限制
        # - 客户端发送太快
        # - 集群背压
        self._on_throttle_callback =  func

    def register_on_stats(self, func: Callable):
        # Kafka 客户端 telemetry
        # 送 JSON 指标：
        # - 吞吐量
        # - 队列长度
        # - 请求延迟
        # - 连接状态
        # - broker 状态
        # - 重试次数
        # - lag
        # - 内存使用
        # - TCP 状态
        #
        # 👉 用来做监控 / 自动恢复 / 自适应限流
        self._on_stats_callback = func

    async def kafka_available(self) -> bool:
        servers_str = self.config.get("bootstrap.servers")
        servers = servers_str.split(",")
        for server in servers:
            try:
                host, port = server.split(":")
                r, w = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    3,
                )
                w.close()
                await w.wait_closed()
            except Exception:
                return False
        return True

    async def on_assign(self, consumer, partitions):
        # 分配分区

        # Calling incremental_assign is necessary to pause the assigned partitions
        # otherwise it'll be done by the consumer after callback termination.
        await consumer.incremental_assign(partitions)
        await consumer.pause(partitions)  # Demonstrates async partition control
        logger.info(f'on_assign {partitions}')

        # 执行回调
        for cb in self._on_assign_callbacks:
            try:
                result = cb(partitions)
                if inspect.isawaitable(result):
                    await result
            except Exception as e:
                logger.error(f'on_assign callback error: {e}')

        # Resume the partitions as it's just a pause example
        await consumer.resume(partitions)

    async def on_revoke(self, consumer, partitions):
        # 撤销分区，让给其他worker
        # 当新worker加入消费时，老worker会执行on_revoke撤销分区，然后撤销的分区会分配给新的worker，
        # 也就是旧worker在on_revoke中打印的partitions会在新worker的on_assign中打印出来

        logger.info(f'before on_revoke {partitions}')

        # 执行回调
        for cb in self._on_revoke_callbacks:
            try:
                result = cb(partitions)
                if inspect.isawaitable(result):
                    await result
            except Exception as e:
                logger.error(f'on_revoke callback error: {e}')

        try:
            # AsyncIO Pattern: Non-blocking commit during rebalance
            await consumer.commit()  # Ensure offsets are committed before losing partitions
        except Exception as e:
            logger.info(f'Error during commit: {e}')
        logger.info(f'after on_revoke {partitions}')

    async def on_lost(self, consumer, partitions):
        """
        on_lost与on_revoke的区别：
        on_revoke:
        - 触发时机：当 Kafka 触发 rebalance，某些 partition 即将从当前 consumer 转移给其他 consumer 时。
        - 典型场景：
            - consumer group 新增消费者
            - consumer 退出
            - topic partition 变化
            - 协议需要重新分配
        - 分区被收回（正常再平衡），仍然拥有这些 partition，只是将会失去分区
        - 可以安全执行：commit offset、flush state、保存缓存数据
        on_lost：
        - 触发时机：当 consumer 已经不再拥有 partition，但 rebalance 仍然发生。
        - 常见原因：
            - consumer session timeout
            - 心跳丢失
            - rebalance 已经完成
            - cooperative rebalance 中超时
        - Kafka 已经把 partition 分给别人,不能再 commit offset
        """
        logger.debug(f'on_lost {partitions}')
        # 执行回调
        for cb in self._on_revoke_callbacks:
            try:
                result = cb(partitions)
                if inspect.isawaitable(result):
                    await result
            except Exception as e:
                logger.error(f'on_revoke callback error: {e}')

    async def listen(self) -> list[ConsumerMessage]:
        msg_list: list[ConsumerMessage] = []
        try:
            messages: list[Message] = await self.consumer.consume(num_messages=self.batch_size, timeout=self.poll_timeout)
        except KafkaException as e:
            logger.warning(f"consume error {e}")
            return msg_list

        for kafka_msg in messages:
            msg = ConsumerMessage(
                topic=kafka_msg.topic(),
                offset=kafka_msg.offset(),
                headers=dict(kafka_msg.headers() or []),
                timestamp_ms=kafka_msg.timestamp()[1],
                key=kafka_msg.key(),
                partition=kafka_msg.partition(),
                payload=kafka_msg.value(),
                decoded_payload=json.loads(kafka_msg.value()),
                raw_message=kafka_msg, # 直接持有 kafka 原始消息
            )
            msg_list.append(msg)
        return msg_list

    async def seek(self, msg: ConsumerMessage):
        tp = TopicPartition(msg.topic, msg.partition, msg.offset)
        await self.consumer.seek(tp)

    async def pause_partition(self, msg):
        tp = TopicPartition(msg.topic, msg.partition)
        await self.consumer.pause([tp])

    async def resume_partition(self, msg):
        tp = TopicPartition(msg.topic, msg.partition)
        await self.consumer.resume([tp])

    async def store_offsets(self, msgs: list[ConsumerMessage]):
        partition_offsets = {}
        # 提交的offset为当前partition的offset + 1，
        # 因为 Kafka 提交的 offset 含义不是"已经消费到哪里”，而是“下一次从哪里开始消费”
        # offset是以topic+partition作为维度的，所以需要根据topic和partition来更新offset
        for msg in msgs:
            tp = (msg.topic, msg.partition)
            # offset的初始值是0
            # 初始化使用-1，确保第一次出现tp时，max()获取的是msg.offset + 1，因为-1 < msg.offset + 1
            partition_offsets[tp] = max(
                partition_offsets.get(tp, -1),
                msg.offset + 1,
            )

        offsets = [
            TopicPartition(topic, partition, offset)
            for (topic, partition), offset
            in partition_offsets.items()
        ]
        await self.consumer.store_offsets(offsets=offsets)

    async def commit(self):
        await self.consumer.commit(asynchronous=False)

    async def start(self):
        if not self.topics:
            raise ValueError('No topics specified')
        # confluent_kafka 使用的是 C 库 librdkafka 的日志系统，不兼容loguru，所以使用python 官方的logging库
        self.config.update(
            {
                'logger': kafka_logger,
                'log_level': 4,  # WARNING
            }
        )
        if self._on_error_callback:
            self.config.update(error_cb=self._on_error_callback)
        if self._on_throttle_callback:
            self.config.update(throttle_cb=self._on_throttle_callback)
        if self._on_stats_callback:
            self.config.update(stats_cb=self._on_stats_callback)
        self.consumer = AIOConsumer(self.config)
        await self.consumer.subscribe(
            self.topics,
            on_assign=self.on_assign,
            on_revoke=self.on_revoke,
            # Remember to set a on_lost callback
            # if you're committing on revocation
            # as lost partitions cannot be committed
            on_lost=self.on_lost,
        )

    async def stop(self):
        # await self.commit() EventDispatcher中已经commit了，不需要再commit了，否则会报错
        await self.consumer.unsubscribe()
        await self.consumer.close()
