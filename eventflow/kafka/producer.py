import logging

from confluent_kafka.aio import AIOProducer

from eventflow.emitter.producer import Producer, ProducerMessage

logger = logging.getLogger(__name__)
kafka_logger = logging.getLogger("kafka_producer")

# 参考 https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/asyncio_example.py

# AsyncIO Pattern: Event loop safe callbacks
# These callbacks are automatically scheduled onto the event loop by AIOProducer/AIOConsumer
# ensuring they don't block the loop and can safely interact with other async operations
async def error_cb(err):
    logger.warning(f'Kafka error: {err}')


async def throttle_cb(event):
    logger.warning(f'Kafka throttle event: {event}')


async def stats_cb(stats_json_str):
    # logger.info(f'Kafka stats: {stats_json_str}')
    pass


def configure_common(conf):
    # 先简单写，不像消费者写的那么详细，后续有需要再优化
    conf.update({
        'logger': kafka_logger,   # 👈 标准 logging
        'log_level': 4,           # WARNING

        'error_cb': error_cb,
        'throttle_cb': throttle_cb,
        'stats_cb': stats_cb,

        'statistics.interval.ms': 5000,

        # 🌟 强烈建议加
        'reconnect.backoff.ms': 1000,
        'reconnect.backoff.max.ms': 30000,
    })

    return conf


class KafkaProducer(Producer):
    def __init__(self, config):
        self.config = configure_common(dict(config))
        self._producer = AIOProducer(self.config)

    async def flush(self):
        await self._producer.flush()

    async def close(self):
        await self._producer.close()

    async def send(self, message: ProducerMessage):
        logger.debug(f"send message: {message}")
        if message.config is None:
            await self._producer.produce(topic=message.topic, value=message.payload)
        else:
            # 注意：AIOProducer的produce方法不支持headers参数
            await self._producer.produce(
                topic=message.topic,
                value=message.payload,
                key=message.config.key,
                partition=message.config.partition,
            )
