import json
import os
import signal
import time

import redis
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Histogram, start_http_server

BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "hotel-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
)
TOPIC = os.getenv("KAFKA_TOPIC", "hotel-price-updates")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "hotel-view-sink")
REDIS_HOST = os.getenv("REDIS_HOST", "redis.lab.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
PROCESSING_DELAY = float(os.getenv("PROCESSING_DELAY", "0.02"))

processed = Counter(
    "hotel_sink_processed_total",
    "Successfully processed Kafka records",
)
errors = Counter(
    "hotel_sink_errors_total",
    "Failed processing attempts",
)
latency = Histogram(
    "hotel_sink_processing_seconds",
    "Sink processing latency",
)

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

start_http_server(8000)

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)

consumer.subscribe([TOPIC])

print(
    f"consumer started group={GROUP_ID} topic={TOPIC}",
    flush=True,
)

try:
    while running:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"kafka error: {msg.error()}", flush=True)
                errors.inc()
            continue

        try:
            started = time.time()
            event = json.loads(msg.value().decode())

            time.sleep(PROCESSING_DELAY)

            redis_key = f"hotel:{event['hotel_id']}"

            r.hset(
                redis_key,
                mapping={
                    "event_id": event["event_id"],
                    "price": event["price"],
                    "currency": event["currency"],
                    "available": str(event["available"]).lower(),
                    "observed_at": event["observed_at"],
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )

            # Commit only AFTER the materialized view was updated.
            consumer.commit(message=msg, asynchronous=False)

            processed.inc()
            latency.observe(time.time() - started)

            print(
                f"processed hotel={event['hotel_id']} "
                f"partition={msg.partition()} offset={msg.offset()}",
                flush=True,
            )

        except Exception as exc:
            # Do not commit the offset if downstream processing failed.
            # A restart/rebalance can therefore replay uncommitted work.
            errors.inc()
            print(f"processing failed: {exc}", flush=True)
            time.sleep(1)

finally:
    consumer.close()
