# Databricks notebook source

import json
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account

# COMMAND ----------
# Config - GCP credentials from Databricks Secrets
GCP_SERVICE_ACCOUNT_INFO = json.loads(
    dbutils.secrets.get(scope="yelp-pipeline", key="gcp-service-account-json")
)
GCP_PROJECT_ID = "yelp-pipeline-498923"
BQ_DATASET = "yelp_analytics"
BQ_TABLE = "fact_reviews"
BQ_TABLE_FULL = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
DELTA_TABLE = "workspace.yelp.reviews"

credentials = service_account.Credentials.from_service_account_info(
    GCP_SERVICE_ACCOUNT_INFO,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)


# COMMAND ----------
# Step 1: Read from Delta table
print(f"Reading from Delta table: {DELTA_TABLE}")
df_spark = spark.table(DELTA_TABLE)
row_count_before = df_spark.count()
print(f"[Step 1] Rows to load: {row_count_before:,}")

# COMMAND ----------
# Step 2: Convert to pandas and fix types
df_pandas = df_spark.toPandas()

# string -> date
df_pandas["date"] = pd.to_datetime(df_pandas["date"], format="%Y-%m-%d %H:%M:%S", errors="coerce").dt.date

# ingest_timestamp: ensure tz-naive (BigQuery TIMESTAMP expects UTC-naive or tz-aware)
if df_pandas["ingest_timestamp"].dt.tz is not None:
    df_pandas["ingest_timestamp"] = df_pandas["ingest_timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)

print(f"[Step 2] Pandas DataFrame ready: {len(df_pandas):,} rows | dtypes: date={df_pandas['date'].dtype}, ingest_timestamp={df_pandas['ingest_timestamp'].dtype}")

# COMMAND ----------
# Step 3: Load to BigQuery via pandas_gbq
print(f"Loading to BigQuery: {BQ_TABLE_FULL} (WRITE_TRUNCATE)")
pandas_gbq.to_gbq(
    df_pandas,
    destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
    project_id=GCP_PROJECT_ID,
    if_exists="replace",
    credentials=credentials,
    progress_bar=True,
)
print(f"[Step 3] Load complete -> {BQ_TABLE_FULL}")
# COMMAND ----------
# Step 4: Verify via SELECT COUNT(*)
bq_client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)

query = f"SELECT COUNT(*) AS row_count FROM `{BQ_TABLE_FULL}`"
result = bq_client.query(query).result()
row_count_bq = next(result).row_count

print(f"[Step 4] Verification: rows loaded to BigQuery = {row_count_bq:,}")

if row_count_bq == row_count_before:
    print(f"Row count matched: {row_count_bq:,}")
else:
    print(f"WARNING: Row count mismatch — Delta: {row_count_before:,}, BigQuery: {row_count_bq:,}")
