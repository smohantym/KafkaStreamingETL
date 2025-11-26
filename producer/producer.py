import json
import os
import random
import string
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "events")
MSGS_PER_SEC = int(os.getenv("MSGS_PER_SEC", "5"))


def random_id(prefix: str, length: int = 8) -> str:
    return prefix + "".join(random.choices(string.ascii_letters + string.digits, k=length))


def create_event(i: int) -> dict:
    # introduce occasional missing values & duplicates
    user_id = random.randint(1, 5)
    product_id = random.randint(1, 8)

    event = {
        "event_id": random_id("evt_"),
        "user_id": user_id,
        "product_id": product_id,
        "amount": round(random.uniform(5, 200), 2),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "country": random.choice(["IN", "US", "US", "US", "DE", None]),
        "device": random.choice(["MOBILE", "mobile ", "DESKTOP", None]),
    }

    # 1/10 messages have null amount to test fillna
    if random.random() < 0.1:
        event["amount"] = None

    # occasionally emit duplicate events for dedup
    if random.random() < 0.05:
        dup = event.copy()
        dup["event_id"] = event["event_id"]  # same id = dup
        return dup

    return event


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"[PRODUCER] Sending messages to {TOPIC} on {BOOTSTRAP_SERVERS}")

    interval = 1.0 / max(MSGS_PER_SEC, 1)

    try:
        i = 0
        while True:
            event = create_event(i)
            producer.send(TOPIC, value=event)
            i += 1
            if i % 50 == 0:
                print(f"[PRODUCER] Sent {i} messages...")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("[PRODUCER] Stopping...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
