import argparse
import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_REVIEWS = os.getenv("KAFKA_TOPIC_REVIEWS", "yelp-reviews")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW")
S3_PREFIX = os.getenv("S3_PREFIX", "raw/reviews/")

CONSUMER_GROUP_ID = "yelp-pipeline-consumer"


def parse_args():
    parser = argparse.ArgumentParser(description="Consume Yelp reviews from Kafka and upload to S3")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of messages per S3 upload batch (default: 1000)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Seconds to wait for new messages before exiting (default: 30)",
    )
    return parser.parse_args()


def upload_batch(s3_client, bucket: str, prefix: str, batch: list) -> str:
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    timestamp_str = now.strftime("%Y%m%dT%H%M%S%f")
    s3_key = f"{prefix}{date_str}/{timestamp_str}.json"

    body = "\n".join(json.dumps(record) for record in batch)
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    return s3_key


def main():
    args = parse_args()
    batch_size = args.batch_size
    timeout_sec = args.timeout

    if not S3_BUCKET_RAW:
        raise ValueError("S3_BUCKET_RAW is not set in .env.")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    consumer = KafkaConsumer(
        KAFKA_TOPIC_REVIEWS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=timeout_sec * 1000,
    )

    print(f"Consuming from topic '{KAFKA_TOPIC_REVIEWS}' (group: {CONSUMER_GROUP_ID})")
    print(f"Batch size: {batch_size} | Idle timeout: {timeout_sec}s | Target: s3://{S3_BUCKET_RAW}/{S3_PREFIX}")

    batch = []
    total_consumed = 0
    total_uploaded = 0
    start = time.time()

    try:
        for message in consumer:
            batch.append(message.value)
            total_consumed += 1

            if len(batch) >= batch_size:
                s3_key = upload_batch(s3_client, S3_BUCKET_RAW, S3_PREFIX, batch)
                total_uploaded += len(batch)
                print(f"[upload] s3://{S3_BUCKET_RAW}/{s3_key} ({len(batch)} records | total uploaded: {total_uploaded:,})")
                batch = []

    except Exception as e:
        print(f"Consumer stopped: {e}")

    finally:
        # Upload remaining messages
        if batch:
            s3_key = upload_batch(s3_client, S3_BUCKET_RAW, S3_PREFIX, batch)
            total_uploaded += len(batch)
            print(f"[upload] s3://{S3_BUCKET_RAW}/{s3_key} ({len(batch)} records | total uploaded: {total_uploaded:,})")

        consumer.close()

    elapsed = time.time() - start
    print(f"Done. Consumed: {total_consumed:,} | Uploaded: {total_uploaded:,} | Elapsed: {elapsed:.2f}s")


if __name__ == "__main__":
    main()
