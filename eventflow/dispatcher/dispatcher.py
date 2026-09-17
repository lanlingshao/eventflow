import asyncio
import logging
import json
import signal
from collections import defaultdict
from collections.abc import Coroutine
from dataclasses import dataclass
from enum import StrEnum

from eventflow.dispatcher.consumer import Consumer, ConsumerMessage
from eventflow.dispatcher.retry_tracker import RedisRetryTracker
from eventflow.emitter.emitter import EventEmitter
from eventflow.emitter.producer import MessageConfig
from eventflow.util.partition import get_partition

logger = logging.getLogger(__name__)


class ConsumeMode(StrEnum):
    STRICT_ORDER = "strict_order"
    NORMAL = "normal"


@dataclass
class ConsumeResult:
    msg: ConsumerMessage
    success: bool
    error: Exception | None = None


# TODO
#  1、子类的_batch_handler_message方法增加幂等性的操作，
class EventDispatcher:
    """
    支持：
    - 支持批量消息处理，不支持单条消费（减小消费失败处理等复杂度）
    - graceful shutdown
    - background tasks
    - 严格模式、普通模式：严格模式保证批量消费的顺序，适用于CDC/Binlog同步、金融转账、撮合/行情增量流、库存系统等场景

    子类：
    - 需要实现 _batch_handler_message（真正批量）
    """

    topics: list[str] = []

    shutdown_timeout = 10  # 超时时间（秒）
    max_retries = 3 # 最大重试次数

    consume_mode = ConsumeMode.NORMAL

    retry_topic: str | None = None # 重试 topic
    dlq_topic: str | None = None # 死信队列 topic

    def __init__(
        self,
        partition_count: int,
        event_emitter: EventEmitter,
        consumer: Consumer,
        retry_tracker: RedisRetryTracker,
    ):
        self.partition_count = partition_count
        
        self.event_emitter = event_emitter
        self.consumer = consumer
        self.retry_tracker = retry_tracker

        # 任务集合，先这样搞，后期如果需要，可以考虑使用basana项目的basana/core/helpers.py里面的TaskPool
        self._tasks: set[asyncio.Task] = set()
        self._background_tasks: set[asyncio.Task] = set()

        self._stop_event = asyncio.Event()

        # 单条 handler 并发限制
        # self._semaphore = asyncio.Semaphore(self.concurrency)

        # EventDispatcher的子类必须实现_handler_message或_batch_handler_message方法
        self._validate_handlers()

    def __init_subclass__(cls, **kwargs):
        # 约束子类为NORMAL模式时，必须设置retry_topic、dlq_topic属性
        # 约束子类为STRICT_ORDER模式时，必须设置dlq_topic属性，严格顺序消费模式不放入重试队列
        super().__init_subclass__(**kwargs)
        if cls.consume_mode == ConsumeMode.STRICT_ORDER:
            if not cls.dlq_topic:
                raise TypeError(f"{cls.__name__} must define dlq_topic when using STRICT mode")
        elif cls.consume_mode == ConsumeMode.NORMAL:
            if not cls.retry_topic:
                raise TypeError(f"{cls.__name__} must define retry_topic when using NORMAL mode")
            if not cls.dlq_topic:
                raise TypeError(f"{cls.__name__} must define dlq_topic when using NORMAL mode")
        else:
            raise ValueError(f"consume_mode: {cls.consume_mode} is not supported")

    def _validate_handlers(self):
        # 检查子类是否实现了_batch_handler_message方法
        has_batch = self.__class__._batch_handler_message is not EventDispatcher._batch_handler_message
        if not has_batch:
            raise TypeError(f"{self.__class__.__name__} must implement _batch_handler_message")

    @property
    def stopped(self) -> bool:
        """
        判断是否已经停止
        为什么不使用变量self._stopped来控制停止？原因如下：
        1. 协程无法“等待”一个 bool：
            这本质上是：轮询、人工 sleep、响应慢 or CPU 浪费
        2. 多个协程无法被同时唤醒：
            如果你有：
            - poll loop
            - handler worker
            - shutdown watcher
            它们都只能“自己猜”什么时候停
        3. 状态语义弱：并不能表达：
            - 是谁停的？
            - 是否已经进入 shutdown？
            - 是否允许再启动？

        """
        return self._stop_event.is_set()

    def configure_consumer(self):
        """
        子类配置consumer:
        - topics
        - callbacks
        - assign hooks
        """
        if self.topics:
            topics = self.topics.copy()
            if self.retry_topic:
                topics.append(self.retry_topic)
            if self.dlq_topic:
                topics.append(self.dlq_topic)
            self.consumer.set_topics(topics)
        if self.on_assign_callback():
            self.consumer.register_on_assign(self.on_assign_callback())
        if self.on_revoke_callback():
            self.consumer.register_on_revoke(self.on_revoke_callback())
        if self.on_lost_callback():
            self.consumer.register_on_lost(self.on_lost_callback())

    async def before_start(self):
        """
        consumer 启动前执行。
        子类可覆盖。
        """
        pass

    async def start_background_tasks(self):
        """
        子类可覆盖
        """
        pass

    async def before_batch_handler(self, msgs: list[ConsumerMessage]):
        """
        batch hook
        """
        pass

    async def after_batch_handler(self, results: list[ConsumeResult]):
        """
        batch hook
        """
        pass


    async def run(self):
        """
        主消费循环
        使用了设计模式中的「模版模式」
        """

        # 配置consumer
        self.configure_consumer()
        logger.info(f"{self.__class__.__name__} configure_consumer done")

        # 启动前初始化
        await self.before_start()
        logger.info(f"{self.__class__.__name__} before_start done")

        # 先检查kafka可用性（带超时）
        is_available = await self.consumer.kafka_available()
        if not is_available:
            logger.warning("Kafka is not available")
            return False
        loop = asyncio.get_running_loop()
        self._register_signal_handlers(loop)
        await self.consumer.start()
        logger.info(f"{self.__class__.__name__} consumer start done")

        # 启动worker的后台任务
        await self.start_background_tasks()
        logger.info(f"{self.__class__.__name__} start_background_tasks done")

        try:
            while not self.stopped:
                # 开始很多次取不到消息，因为Kafka 消费者在真正开始拉消息前，要先完成「入组 + Rebalance + Offset 决定」，这一步本身就会耗时
                msgs = await self.consumer.listen()
                # 无消息
                if not msgs:
                    await asyncio.sleep(1)
                    continue
                await self._dispatch_messages(msgs)
        except asyncio.CancelledError:
            if not self.stopped:
                raise
            logger.info(f"{self.__class__.__name__} run cancelled")
        finally:
            await self.stop()
            logger.info(f"{self.__class__.__name__} run finally")

    async def _dispatch_messages(self, msgs: list[ConsumerMessage]):
        logger.info(f"{self.__class__.__name__} _dispatch_messages, msgs count: {len(msgs)}")
        try:
            await self.before_batch_handler(msgs)
            results = await self._batch_handler_message(msgs)
            # TODO 目前股票预警系统的worker允许过程中，无法将消息无法发送retry topic和dlq topic中，所以暂时注释掉，后续fix了再开启
            await self.after_batch_handler(results)
            '''
            if self.consume_mode == ConsumeMode.STRICT_ORDER:
                await self._process_strict_results(results)
            else:
                await self._process_normal_results(results)
            '''
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("dispatch failed")

    async def _process_strict_results(self, results: list[ConsumeResult]):
        """
        trict mode:
        - partition级别严格顺序
        - partition独立seek
        - partition独立commit
        - retry count存在redis里面，因为strict mode下，消费失败的消息不会存到retry topic中，只能等待原topic的重复消费
        """
        partition_results: dict[tuple[str, int], list[ConsumeResult]] = defaultdict(list)

        # 按 topic+partition 分组
        for result in results:
            key = (result.msg.topic, result.msg.partition)
            partition_results[key].append(result)

        commit_msgs = []
        seek_partitions = []

        for _, partition_batch in partition_results.items():
            # partition内按offset排序
            partition_batch.sort(key=lambda r: r.msg.offset)
            for result in partition_batch:
                msg = result.msg
                if result.success:
                    await self.retry_tracker.clear(msg.topic, msg.partition, msg.offset)
                    commit_msgs.append(msg)
                    continue

                retry_count = await self.retry_tracker.incr(msg.topic, msg.partition, msg.offset)

                logger.error(
                    f"strict consume failed "
                    f"retry={retry_count} "
                    f"topic={msg.topic} "
                    f"partition={msg.partition} "
                    f"offset={msg.offset}"
                    f"msg={msg}"
                )

                # 超过重试次数
                if retry_count >= self.max_retries:
                    await self._send_to_dlq(msg, result.error, retry_count)
                    await self.retry_tracker.clear(msg.topic, msg.partition, msg.offset)
                    # DLQ后允许推进offset
                    commit_msgs.append(msg)
                    continue

                # 未达到最大重试次数
                seek_partitions.append(msg)

                # 当前partition停止继续处理
                break

        # commit成功消息
        if commit_msgs:
            await self.consumer.store_offsets(commit_msgs)
            await self.consumer.commit()
            logger.debug(f"{self.__class__.__name__} _process_strict_results, commit msgs count: {len(commit_msgs)}")

        # seek失败partition
        await asyncio.gather(*[self._retry_partition(msg) for msg in seek_partitions])
        if len(seek_partitions) > 0:
            logger.debug(f"{self.__class__.__name__} _process_strict_results, seek partitions count: {len(seek_partitions)}")

    async def _retry_partition(self, msg: ConsumerMessage):
        await self.consumer.pause_partition(msg)
        retry_count = await self.retry_tracker.get(msg.topic, msg.partition, msg.offset)
        await asyncio.sleep(min(2 ** retry_count, 30))
        await self.consumer.seek(msg)
        await self.consumer.resume_partition(msg)

    async def _process_normal_results(self, results: list[ConsumeResult]):
        """
        normal mode:
        - fail直接retry topic
        - 原topic offset直接commit
        - retry count存在消息里面的meta里面
        """
        commit_msgs = []
        for result in results:
            msg = result.msg
            if result.success:
                commit_msgs.append(msg)
                continue

            retry_count = self._get_normal_retry_count(msg) + 1
            if retry_count >= self.max_retries:
                await self._send_to_dlq(msg, result.error, retry_count)
            else:
                await self._send_to_retry(msg, result.error, retry_count)
            commit_msgs.append(msg)

        if commit_msgs:
            await self.consumer.store_offsets(commit_msgs)
            await self.consumer.commit()

    def get_partition_key(self, payload: dict, msg: ConsumerMessage) -> str | int | None:
        """
        子类可覆盖：
        用于 retry/dlq 分区路由

        返回:
        - str/int -> hash partition
        - None -> kafka 默认分区
        """
        return None

    def build_message_config(self, payload: dict, msg: ConsumerMessage) -> MessageConfig:
        partition_key = self.get_partition_key(payload, msg)
        partition = None
        if partition_key is not None:
            partition = get_partition(partition_key, self.partition_count)
        return MessageConfig(partition=partition)

    async def _send_to_retry(self, msg: ConsumerMessage, exc: Exception, retry_count: int):
        if not self.retry_topic:
            logger.error("retry topic not configured")
            return

        origin_payload = self.get_business_payload(msg)
        payload = {
            "meta": {
                "origin_topic": msg.topic,
                "origin_partition": msg.partition,
                "origin_offset": msg.offset,
                "retry_count": retry_count,
                "timestamp": msg.timestamp_ms,
                "error": repr(exc),
            },
            "payload": origin_payload,
        }

        message_config = self.build_message_config(
            payload=origin_payload,
            msg=msg,
        )
        payload_bytes = json.dumps(payload).encode()
        await self.event_emitter.emit(topic=self.retry_topic, payload=payload_bytes, message_config=message_config)

        logger.warning(f"message sent to retry topic={self.retry_topic} retry_count={retry_count}")

    async def _send_to_dlq(self, msg: ConsumerMessage, exc: Exception, retry_count: int):
        logger.debug(f"send to dlq, msg={msg} exc={exc} retry_count={retry_count}")
        if not self.dlq_topic:
            logger.error("dlq topic not configured")
            return
        origin_payload = self.get_business_payload(msg)
        payload = {
            "meta": {
                "origin_topic": msg.topic,
                "origin_partition": msg.partition,
                "origin_offset": msg.offset,
                "retry_count": retry_count,
                "timestamp": msg.timestamp_ms,
                "error": repr(exc),
            },
            "payload": origin_payload,
        }
        message_config = self.build_message_config(
            payload=origin_payload,
            msg=msg,
        )
        payload_bytes = json.dumps(payload).encode()
        await self.event_emitter.emit(topic=self.dlq_topic, payload=payload_bytes, message_config=message_config)

        logger.error(f"message sent to dlq topic={self.dlq_topic} payload={payload}")

    def get_business_payload(self, msg: ConsumerMessage) -> dict:
        payload = msg.decoded_payload or {}
        # retry/dlq envelope
        if "payload" in payload:
            return payload["payload"]
        # normal message
        return payload

    def _get_normal_retry_count(self, msg: ConsumerMessage) -> int:
        payload = msg.decoded_payload or {}
        meta = payload.get("meta", {})
        return int(meta.get("retry_count", 0))

    async def _batch_handler_message(self, msgs: list[ConsumerMessage]) -> list[ConsumeResult]:
        raise NotImplementedError

    async def stop(self):
        if self.stopped:
            return
        logger.info("stopping dispatcher")
        self._stop_event.set()
        await self._graceful_shutdown()

    async def _graceful_shutdown(self):
        logger.info(f"{self.__class__.__name__} graceful shutdown begin")
        tasks = list(self._tasks) + list(self._background_tasks)
        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=self.shutdown_timeout,
                )
            except TimeoutError:
                logger.warning(
                    f"Shutdown timeout ({self.shutdown_timeout}s), cancelling tasks"
                )
                for task in self._tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        await self.consumer.stop()
        logger.info(f"{self.__class__.__name__} graceful shutdown finished")

    def _register_signal_handlers(self, loop: asyncio.AbstractEventLoop):
        """
        注册退出信号
        """
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._on_stop_signal, sig)

    def _on_stop_signal(self, sig: signal.Signals):
        """
        信号处理
        """
        logger.info(f"{self.__class__.__name__} Received signal: {sig.name}")
        self._stop_event.set()

    def add_background_task(self, coro: Coroutine):
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def on_assign_callback(self):
        return None

    def on_revoke_callback(self):
        return None

    def on_lost_callback(self):
        return None
