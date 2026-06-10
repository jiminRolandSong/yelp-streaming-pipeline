import os

import pandas as pd
import pandas_gbq
from dotenv import load_dotenv
from google.oauth2 import service_account

load_dotenv()

GCP_SERVICE_ACCOUNT_JSON = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "yelp-pipeline-498923-8b2caffededb.json")
GCP_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "yelp-pipeline-498923")
BQ_DATASET = "yelp_analytics"
BQ_TABLE = "dim_businesses"
BQ_TABLE_FULL = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

INPUT_FILE = os.path.join("data", "businesses.json")

COLUMNS = ["business_id", "name", "city", "state", "categories", "stars", "review_count"]


def main():
    print(f"Reading: {INPUT_FILE}")
    df = pd.read_json(INPUT_FILE, lines=True, dtype=str)

    df = df[[c for c in COLUMNS if c in df.columns]]

    df = df.rename(columns={"stars": "avg_stars"})
    df["avg_stars"] = pd.to_numeric(df["avg_stars"], errors="coerce")
    df["review_count"] = pd.to_numeric(df["review_count"], errors="coerce").astype("Int64")

    row_count_before = len(df)
    print(f"[Step 1] Rows to load: {row_count_before:,}")

    credentials = service_account.Credentials.from_service_account_file(
        GCP_SERVICE_ACCOUNT_JSON,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    print(f"Loading to BigQuery: {BQ_TABLE_FULL} (WRITE_TRUNCATE)")
    pandas_gbq.to_gbq(
        df,
        destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
        project_id=GCP_PROJECT_ID,
        if_exists="replace",
        credentials=credentials,
        progress_bar=True,
    )
    print(f"[Step 2] Load complete -> {BQ_TABLE_FULL}")

    from google.cloud import bigquery
    bq_client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
    result = bq_client.query(f"SELECT COUNT(*) AS row_count FROM `{BQ_TABLE_FULL}`").result()
    row_count_after = next(result).row_count
    print(f"[Step 3] BigQuery row count: {row_count_after:,}")

    if row_count_after == row_count_before:
        print(f"Row count matched: {row_count_after:,}")
    else:
        print(f"WARNING: Row count mismatch — local: {row_count_before:,}, BigQuery: {row_count_after:,}")


if __name__ == "__main__":
    main()
