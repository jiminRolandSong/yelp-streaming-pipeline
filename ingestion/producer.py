import argparse
import json
import os
import time

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_REVIEWS = os.getenv("KAFKA_TOPIC_REVIEWS", "yelp-reviews")
SAMPLE_SIZE = int(os.getenv("SAMPLE_SIZE", "100000"))

INPUT_FILE = os.path.join("data", "sampled_reviews.json")


def parse_args():
    parser = argparse.ArgumentParser(description="Produce Yelp reviews to Kafka")
    parser.add_argument(
        "--rate",
        type=float,
        default=100.0,
        help="Messages per second (default: 100)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of messages to send (default: all)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    rate = args.rate
    limit = args.limit or SAMPLE_SIZE
    delay = 1.0 / rate if rate > 0 else 0

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        compression_type="gzip",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    print(f"Sending to topic '{KAFKA_TOPIC_REVIEWS}' at {rate} msg/sec (limit: {limit:,})")

    count = 0
    start = time.time()

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            record = json.loads(line)
            key = record.get("business_id")

            producer.send(KAFKA_TOPIC_REVIEWS, key=key, value=record)
            count += 1

            if count % 1000 == 0:
                elapsed = time.time() - start
                actual_rate = count / elapsed if elapsed > 0 else 0
                print(f"[producer] {count:,} sent | elapsed: {elapsed:.1f}s | rate: {actual_rate:.1f} msg/s")

            if count >= limit:
                break

            if delay > 0:
                time.sleep(delay)

    producer.flush()
    producer.close()

    elapsed = time.time() - start
    print(f"Done. {count:,} messages sent in {elapsed:.2f}s ({count / elapsed:.1f} msg/s avg)")


if __name__ == "__main__":
    main()
