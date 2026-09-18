KafkaProducerConf = {
    "bootstrap.servers": "127.0.0.1:9092",

    "acks": "all",
    "enable.idempotence": True,
    "retries": 10,
    "delivery.timeout.ms": 120000,

    "max.in.flight.requests.per.connection": 5,

    "linger.ms": 20,
    "batch.size": 64 * 1024,         # 64KB
    "compression.type": "lz4",

    "queue.buffering.max.messages": 200_000,
    "queue.buffering.max.kbytes": 512 * 1024,  # 512MB

    "request.timeout.ms": 30000,

    "client.id": "quant-aio-producer",

    'reconnect.backoff.ms': 1000,
    'reconnect.backoff.max.ms': 10000,
}

KafkaConsumerConf = {
    "bootstrap.servers": "127.0.0.1:9092",
    "group.id": "test-consumer",

    "enable.auto.commit": False,
    "enable.auto.offset.store": False,
    "auto.offset.reset": "earliest",

    'partition.assignment.strategy': 'cooperative-sticky',

    "fetch.min.bytes": 1 * 1024 * 1024, # 1MB

    "session.timeout.ms": 10000,
    "heartbeat.interval.ms": 3000,
    "max.poll.interval.ms": 300000,

    "socket.keepalive.enable": True,
    "reconnect.backoff.ms": 100,
    "reconnect.backoff.max.ms": 5000,

    "isolation.level": "read_committed",
}