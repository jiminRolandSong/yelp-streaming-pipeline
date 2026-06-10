import os
import sys

import boto3
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from components.sidebar import render_sidebar

GCP_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "yelp-pipeline-498923")
GCP_SERVICE_ACCOUNT_JSON = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "yelp-pipeline-498923-8b2caffededb.json")
BQ_DATASET = "yelp_analytics"

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "yelp-pipeline-raw")
S3_PREFIX = os.getenv("S3_PREFIX", "raw/reviews/")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "yelp-insights")

st.set_page_config(page_title="Pipeline Status — Yelp Analytics", layout="wide")

render_sidebar()

st.title("Pipeline Status")
st.markdown("Real-time status of each stage in the Yelp streaming pipeline.")


def _get_bq_client() -> bigquery.Client:
    import json
    sa_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "")
    
    if sa_json.startswith("{"):
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(sa_json),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    elif os.path.exists(sa_json):
        credentials = service_account.Credentials.from_service_account_file(
            sa_json,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    else:
        raise ValueError("GCP_SERVICE_ACCOUNT_JSON not set properly")
    
    return bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)

def _get_bq_row_count(table: str) -> int:
    client = _get_bq_client()
    result = client.query(f"SELECT COUNT(*) AS n FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{table}`").result()
    return next(result).n


def _get_s3_file_count() -> int:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=S3_BUCKET_RAW, Prefix=S3_PREFIX):
        count += len(page.get("Contents", []))
    return count


def _get_dynamodb_insight_count() -> int:
    dynamodb = boto3.resource(
        "dynamodb",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    table = dynamodb.Table(DYNAMODB_TABLE)
    return table.scan(Select="COUNT")["Count"]


if st.button("Refresh Status", type="secondary"):
    st.rerun()

st.markdown("---")

stages = [
    {
        "label": "Kafka Producer",
        "description": "Reads `data/sampled_reviews.json` and produces to Kafka topic `yelp-reviews`",
        "metric_fn": None,
        "metric_label": None,
    },
    {
        "label": "Kafka Consumer → S3",
        "description": f"Consumes from Kafka and uploads JSON Lines batches to `s3://{S3_BUCKET_RAW}/{S3_PREFIX}`",
        "metric_fn": _get_s3_file_count,
        "metric_label": "S3 files",
    },
    {
        "label": "Databricks PySpark Transform",
        "description": "Reads S3, applies transforms, writes Delta table `workspace.yelp.reviews`",
        "metric_fn": None,
        "metric_label": None,
    },
    {
        "label": "BigQuery Load — fact_reviews",
        "description": "Loads Delta table into `yelp_analytics.fact_reviews`",
        "metric_fn": lambda: _get_bq_row_count("fact_reviews"),
        "metric_label": "rows in fact_reviews",
    },
    {
        "label": "BigQuery Load — dim_businesses",
        "description": "Loads `data/businesses.json` into `yelp_analytics.dim_businesses`",
        "metric_fn": lambda: _get_bq_row_count("dim_businesses"),
        "metric_label": "rows in dim_businesses",
    },
    {
        "label": "Cerebras AI Insights",
        "description": "Generates business insights from BigQuery aggregations and caches to DynamoDB",
        "metric_fn": _get_dynamodb_insight_count,
        "metric_label": "insights in DynamoDB",
    },
]

for stage in stages:
    with st.container(border=True):
        col_check, col_info, col_metric = st.columns([1, 6, 3])

        metric_value = None
        error_msg = None

        if stage["metric_fn"]:
            try:
                metric_value = stage["metric_fn"]()
            except Exception as e:
                error_msg = str(e)

        with col_check:
            if stage["metric_fn"] is None:
                st.markdown("⚙️")
            elif error_msg:
                st.markdown("❌")
            elif metric_value is not None and metric_value > 0:
                st.markdown("✅")
            else:
                st.markdown("⏳")

        with col_info:
            st.markdown(f"**{stage['label']}**")
            st.caption(stage["description"])
            if error_msg:
                st.error(f"Error: {error_msg}", icon="🚨")

        with col_metric:
            if metric_value is not None:
                st.metric(label=stage["metric_label"], value=f"{metric_value:,}")
            elif stage["metric_fn"] is None:
                st.caption("No metric available")
