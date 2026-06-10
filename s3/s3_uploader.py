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


def upload_file(local_path: str, s3_key: str, bucket: str = None) -> None:
    bucket = bucket or S3_BUCKET_RAW
    if not bucket:
        raise ValueError("S3_BUCKET_RAW is not set in .env.")

    file_size = os.path.getsize(local_path)
    print(f"[upload] {local_path} -> s3://{bucket}/{s3_key} ({file_size:,} bytes)")
    s3_client.upload_file(local_path, bucket, s3_key)
    print(f"[upload] Done: s3://{bucket}/{s3_key}")


def upload_directory(local_dir: str, s3_prefix: str, bucket: str = None) -> int:
    bucket = bucket or S3_BUCKET_RAW
    if not bucket:
        raise ValueError("S3_BUCKET_RAW is not set in .env.")

    if not os.path.isdir(local_dir):
        raise FileNotFoundError(f"Directory not found: {local_dir}")

    s3_prefix = s3_prefix.rstrip("/") + "/"
    count = 0

    for root, _, files in os.walk(local_dir):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, local_dir).replace("\\", "/")
            s3_key = s3_prefix + relative_path

            upload_file(local_path, s3_key, bucket)
            count += 1

    print(f"[upload] Directory upload complete: {count} file(s) from '{local_dir}' -> s3://{bucket}/{s3_prefix}")
    return count


if __name__ == "__main__":
    print("Uploading data/ directory to S3...")
    upload_directory("data", "raw/data")
