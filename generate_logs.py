import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

import faker
from confluent_kafka import Producer

TOPIC = "logs"
BOOTSTRAP_SERVERS = "localhost:9092"
GENERATOR_ID = os.environ.get("GENERATOR_ID", "gen-1")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE") or 0)
BATCH_PAUSE_SECONDS = 20

WEBSITES = ["shop.example.com", "news.example.com", "docs.example.com"]
WEBSITE_WEIGHTS = [5, 3, 2]

PATHS = ["/", "/products", "/products/item-1", "/login", "/search", "/help"]
PATH_WEIGHTS = [2, 5, 3, 2, 4, 1]

METHODS = ["GET", "POST", "PUT", "DELETE"]
METHOD_WEIGHTS = [75, 18, 5, 2]

STATUS_CODES = [200, 201, 301, 304, 400, 403, 404, 500]
STATUS_WEIGHTS = [70, 5, 4, 5, 3, 2, 8, 3]


def make_users(
        fake: faker.Faker, generator_id: str, count: int = 100
) -> list[dict[str, str | int]]:
    users = [
        {"user_id": f"{generator_id}_user_{number:04d}", "ip": fake.ipv4()}
        for number in range(1, count + 1)
    ]
    # A few users are intentionally more active, making rankings meaningful.
    for number, user in enumerate(users):
        user["weight"] = 8 if number < 5 else 1
    return users


def on_delivery(error, message) -> None:
    if error:
        print(f"Delivery failed: {error}")


def make_log_record(
        users: list[dict[str, str | int]],
        event_time: datetime,
        fake: faker.Faker,
        generator_id: str,
) -> dict:
    user = random.choices(users, weights=[int(item["weight"]) for item in users], k=1)[0]
    website = random.choices(WEBSITES, weights=WEBSITE_WEIGHTS, k=1)[0]
    path = random.choices(PATHS, weights=PATH_WEIGHTS, k=1)[0]
    method = random.choices(METHODS, weights=METHOD_WEIGHTS, k=1)[0]
    status = random.choices(STATUS_CODES, weights=STATUS_WEIGHTS, k=1)[0]
    referrer = "-" if random.random() < 0.35 else f"https://{website}{random.choice(PATHS)}"

    return {
        "event_time": event_time.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "generator_id": generator_id,
        "user_id": user["user_id"],
        "ip": user["ip"],
        "website": website,
        "method": method,
        "path": path,
        "status": status,
        "response_time_ms": max(1, int(random.gauss(500, 120))),
        "bytes_sent": random.randint(100, 20_000),
        "referrer": referrer,
        "user_agent": fake.user_agent(),
    }


def generate_and_send(
        producer: Producer,
        users: list[dict[str, str | int]],
        fake: faker.Faker,
        generator_id: str,
        first_event_time: datetime,
        log_size: int = random.randint(10, 1000),
) -> datetime:
    event_time = first_event_time

    for _ in range(log_size):
        record = make_log_record(users, event_time, fake, generator_id)
        producer.produce(
            TOPIC,
            key=record["user_id"],
            value=json.dumps(record, separators=(",", ":")),
            callback=on_delivery,
        )
        producer.poll(0)
        event_time += timedelta(milliseconds=random.randint(100, 5_000))

    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"{remaining} messages were not delivered within 30 seconds")

    print(f"Sent {log_size:,} records to Kafka topic '{TOPIC}'.")
    return event_time


if __name__ == "__main__":
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    fake = faker.Faker()
    users = make_users(fake, GENERATOR_ID)
    next_event_time = datetime.now(timezone.utc)

    try:
        while True:
            if BATCH_SIZE != 0:
                next_event_time = generate_and_send(
                    producer, users, fake, GENERATOR_ID, next_event_time, BATCH_SIZE
                )
            else:
                next_event_time = generate_and_send(
                    producer, users, fake, GENERATOR_ID, next_event_time
                )
            time.sleep(BATCH_PAUSE_SECONDS)
    except KeyboardInterrupt:
        print("Stopping log generation.")
    finally:
        producer.flush(30)
