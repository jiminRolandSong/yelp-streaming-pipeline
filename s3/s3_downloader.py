import os

import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


def list_files(prefix: str, bucket: str = None) -> list[str]:
    bucket = bucket or S3_BUCKET_RAW
    if not bucket:
        raise ValueError("S3_BUCKET_RAW is not set in .env.")

    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    print(f"[list] Found {len(keys)} file(s) under s3://{bucket}/{prefix}")
    return keys


def download_file(s3_key: str, local_path: str, bucket: str = None) -> None:
    bucket = bucket or S3_BUCKET_RAW
    if not bucket:
        raise ValueError("S3_BUCKET_RAW is not set in .env.")

    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    print(f"[download] s3://{bucket}/{s3_key} -> {local_path}")
    s3_client.download_file(bucket, s3_key, local_path)
    print(f"[download] Done: {local_path}")


def download_prefix(s3_prefix: str, local_dir: str, bucket: str = None) -> int:
    bucket = bucket or S3_BUCKET_RAW
    if not bucket:
        raise ValueError("S3_BUCKET_RAW is not set in .env.")

    keys = list_files(s3_prefix, bucket)
    if not keys:
        print(f"[download] No files found under prefix '{s3_prefix}'")
        return 0

    count = 0
    for s3_key in keys:
        relative_path = s3_key[len(s3_prefix):].lstrip("/")
        local_path = os.path.join(local_dir, relative_path.replace("/", os.sep))
        download_file(s3_key, local_path, bucket)
        count += 1

    print(f"[download] Prefix download complete: {count} file(s) -> '{local_dir}'")
    return count


if __name__ == "__main__":
    print("Listing files under raw/reviews/...")
    files = list_files("raw/reviews/")
    for f in files:
        print(f"  {f}")
