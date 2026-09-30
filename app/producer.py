import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "hotel-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
)
TOPIC = os.getenv("KAFKA_TOPIC", "hotel-price-updates")
MESSAGE_COUNT = int(os.getenv("MESSAGE_COUNT", "100"))
RATE_PER_SECOND = float(os.getenv("RATE_PER_SECOND", "100"))
HOTEL_COUNT = int(os.getenv("HOTEL_COUNT", "25"))

producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
    }
)

failures = 0


def delivery_report(err, msg):
    global failures
    if err is not None:
        failures += 1
        print(f"delivery failed: {err}", flush=True)


for i in range(MESSAGE_COUNT):
    hotel_id = f"H{(i % HOTEL_COUNT) + 1:04d}"

    event = {
        "event_id": str(uuid.uuid4()),
        "hotel_id": hotel_id,
        "price": round(random.uniform(70, 450), 2),
        "currency": "EUR",
        "available": random.random() > 0.05,
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }

    producer.produce(
        TOPIC,
        key=hotel_id.encode(),
        value=json.dumps(event).encode(),
        callback=delivery_report,
    )
    producer.poll(0)

    if RATE_PER_SECOND > 0:
        time.sleep(1 / RATE_PER_SECOND)

producer.flush()

print(
    f"produced={MESSAGE_COUNT} failures={failures} topic={TOPIC}",
    flush=True,
)
