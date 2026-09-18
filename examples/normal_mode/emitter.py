import asyncio
import logging
import json
import signal
from dataclasses import dataclass

from eventflow.emitter.emitter import EventEmitter
from eventflow.emitter.producer import MessageConfig
from eventflow.broker.kafka.producer import KafkaProducer
from eventflow.util.partition import get_partition
from examples.normal_mode.conf import KafkaProducerConf
from examples.normal_mode.constant import TOPIC, PARTITION_COUNT

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("emitter")


@dataclass
class Event:
    event_id: int

    def to_bytes(self) -> bytes:
        data = {
            "event_id": self.event_id,
        }
        return json.dumps(data).encode()


async def run_emitter():
    loop = asyncio.get_running_loop()

    producer = KafkaProducer(KafkaProducerConf)
    emitter = EventEmitter(producer)

    stop_event = asyncio.Event()

    def _on_stop_signal(sig: signal.Signals):
        logger.debug(f"Received signal: {sig.name}")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            _on_stop_signal,
            sig
        )


    index = 1
    try:
        while not stop_event.is_set():
            event = Event(
                event_id=index,
            )
            partition = get_partition(event.event_id, PARTITION_COUNT)
            message_config = MessageConfig(
                partition=partition
            )
            await emitter.emit(TOPIC, event.to_bytes(), message_config)
            await asyncio.sleep(5)
            index += 1
    except asyncio.CancelledError:
        await emitter.stop()
    finally:
        await emitter.stop()


if __name__ == '__main__':
    asyncio.run(run_emitter())