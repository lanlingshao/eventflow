import asyncio
import logging

from eventflow.broker.kafka.consumer import KafkaConsumer
from eventflow.broker.kafka.producer import KafkaProducer
from eventflow.dispatcher.dispatcher import EventDispatcher, ConsumeResult
from eventflow.dispatcher.failure import  MaxRetryStrategy
from eventflow.emitter.emitter import EventEmitter
from examples.normal_mode.conf import KafkaProducerConf, KafkaConsumerConf
from examples.normal_mode.constant import TOPIC, DLQ_TOPIC, RETRY_TOPIC, PARTITION_COUNT

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("dispatcher")


class NormalDispatcher(EventDispatcher):

    async def _batch_handler_message(self, msgs):
        results = []
        for msg in msgs:
            try:
                await self._handler_message(msg)
                results.append(ConsumeResult(msg=msg, success=True))
            except Exception as e:
                logger.error(f"{self.__class__} handle message failed, msg:{msg} err:{e}")
                results.append(ConsumeResult(msg=msg, success=False, error=e))
        return results

    async def _handler_message(self, msg):
        payload = self._get_business_payload(msg)
        logger.debug(f"consume message: {msg} payload:{payload}")
        '''
        if payload['event_id'] == 3:
            raise Exception("test error")
        '''


async def main():
    producer = KafkaProducer(KafkaProducerConf)
    emitter = EventEmitter(producer)
    consumer = KafkaConsumer(KafkaConsumerConf)

    # 注意: 创建的TOPIC、DLQ_TOPIC、RETRY_TOPIC的分区数必须与PARTITION_COUNT一致
    # Note: the partition count of TOPIC,DLQ_TOPIC,RETRY_TOPIC must be the same as PARTITION_COUNT
    # you can define your own failure strategy
    worker = NormalDispatcher(
        event_emitter=emitter,
        consumer=consumer,
        partition_count=PARTITION_COUNT,
        topics=[TOPIC],
        dlq_topic=DLQ_TOPIC,
        retry_topic=RETRY_TOPIC,
        failure_strategy=MaxRetryStrategy(max_retries=2),
    )
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())