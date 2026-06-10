# Databricks notebook source

import io
import json

import boto3
from pyspark.sql import functions as F
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# AWS credentials from Databricks Secrets
AWS_ACCESS_KEY_ID = dbutils.secrets.get(scope="yelp-pipeline", key="aws-access-key")
AWS_SECRET_ACCESS_KEY = dbutils.secrets.get(scope="yelp-pipeline", key="aws-secret-key")
AWS_REGION = "us-east-1"
S3_BUCKET_RAW = "yelp-pipeline-raw"
S3_PREFIX = "raw/reviews/"

DELTA_OUTPUT_PATH = "dbfs:/Volumes/workspace/yelp/reviews_delta"
PAGE_SIZE = 100  # files per pagination page

# COMMAND ----------
# Step 1: Read all JSON Lines files from S3 via boto3 with pagination
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

print(f"Reading from s3://{S3_BUCKET_RAW}/{S3_PREFIX}")

paginator = s3_client.get_paginator("list_objects_v2")
records = []
file_count = 0

for page in paginator.paginate(Bucket=S3_BUCKET_RAW, Prefix=S3_PREFIX, PaginationConfig={"PageSize": PAGE_SIZE}):
    for obj in page.get("Contents", []):
        s3_key = obj["Key"]
        if not s3_key.endswith(".json"):
            continue

        response = s3_client.get_object(Bucket=S3_BUCKET_RAW, Key=s3_key)
        content = response["Body"].read().decode("utf-8")

        for line in content.splitlines():
            line = line.strip()
            if line:
                records.append(json.loads(line))

        file_count += 1
        print(f"[S3] Read file {file_count}: {s3_key} (cumulative records: {len(records):,})")

print(f"[Step 1] Total files read: {file_count} | Total records: {len(records):,}")

# COMMAND ----------
# Step 1b: Create DataFrame from records
REVIEW_SCHEMA = StructType([
    StructField("review_id",   StringType(),  True),
    StructField("business_id", StringType(),  True),
    StructField("user_id",     StringType(),  True),
    StructField("stars",       FloatType(),   True),
    StructField("text",        StringType(),  True),
    StructField("date",        StringType(),  True),
    StructField("useful",      IntegerType(), True),
    StructField("funny",       IntegerType(), True),
    StructField("cool",        IntegerType(), True),
])

df_raw = spark.createDataFrame(records, schema=REVIEW_SCHEMA)
print(f"[Step 1] DataFrame created: {df_raw.count():,} records")

# COMMAND ----------
# Step 2: Filter nulls on critical columns
df_filtered = df_raw.filter(
    F.col("review_id").isNotNull()
    & F.col("business_id").isNotNull()
    & F.col("stars").isNotNull()
    & F.col("text").isNotNull()
    & F.col("date").isNotNull()
)
print(f"[Step 2] After null filter: {df_filtered.count():,}")

# COMMAND ----------
# Step 3: Add derived columns
df_transformed = (
    df_filtered
    .withColumn("review_length", F.length(F.col("text")))
    .withColumn(
        "sentiment_bucket",
        F.when(F.col("stars") >= 4, "positive")
         .when(F.col("stars") == 3, "neutral")
         .otherwise("negative"),
    )
    .withColumn("year_month", F.date_format(F.to_timestamp(F.col("date"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM"))
    .withColumn("ingest_timestamp", F.current_timestamp())
)
print(f"[Step 3] After transformation: {df_transformed.count():,}")

# COMMAND ----------
# Step 4: Deduplicate on review_id (keep first occurrence)
df_deduped = df_transformed.dropDuplicates(["review_id"])
print(f"[Step 4] After deduplication: {df_deduped.count():,}")

# COMMAND ----------
# Step 5: Write to Delta
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp")
(
    df_deduped
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("year_month")
    .saveAsTable("workspace.yelp.reviews")
)
print(f"[Step 5] Delta write complete -> {DELTA_OUTPUT_PATH}")

# COMMAND ----------
# Verify
df_verify = spark.table("workspace.yelp.reviews")
print(f"[Verify] Delta records: {df_verify.count():,}")
df_verify.printSchema()
df_verify.show(5, truncate=80)
