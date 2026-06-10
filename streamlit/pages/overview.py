import os
import sys

import altair as alt
import pandas as pd
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

st.set_page_config(page_title="Overview — Yelp Analytics", layout="wide")

render_sidebar()

st.title("Overview")


def _get_bq_client() -> bigquery.Client:
    # Streamlit Cloud: JSON 내용이 환경변수에 있음
    sa_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "")
    
    if sa_json.startswith("{"):  # JSON 내용인 경우
        import json
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(sa_json),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    elif os.path.exists(sa_json):  # 로컬: 파일 경로인 경우
        credentials = service_account.Credentials.from_service_account_file(
            sa_json,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    else:
        raise ValueError("GCP_SERVICE_ACCOUNT_JSON not set properly")
    
    return bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)

@st.cache_data(ttl=600, show_spinner="Loading summary metrics...")
def load_summary_metrics() -> dict:
    client = _get_bq_client()
    sql = f"""
        SELECT
            COUNT(*) AS review_count,
            MIN(date) AS min_date,
            MAX(date) AS max_date
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.fact_reviews`
    """
    row = list(client.query(sql).result())[0]
    biz_sql = f"SELECT COUNT(*) AS biz_count FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_businesses`"
    biz_row = list(client.query(biz_sql).result())[0]
    return {
        "review_count": row.review_count,
        "biz_count": biz_row.biz_count,
        "min_date": str(row.min_date),
        "max_date": str(row.max_date),
    }


@st.cache_data(ttl=600, show_spinner="Loading data from BigQuery...")
def load_city_avg_stars() -> pd.DataFrame:
    client = _get_bq_client()
    sql = f"""
        SELECT b.city, ROUND(AVG(r.stars), 2) AS avg_stars, COUNT(*) AS review_count
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.fact_reviews` r
        JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_businesses` b USING (business_id)
        WHERE b.city IS NOT NULL
        GROUP BY b.city
        HAVING COUNT(*) >= 100
        ORDER BY avg_stars DESC
        LIMIT 10
    """
    return client.query(sql).to_dataframe()


@st.cache_data(ttl=600, show_spinner="Loading sentiment data...")
def load_sentiment_distribution() -> pd.DataFrame:
    client = _get_bq_client()
    sql = f"""
        SELECT sentiment_bucket, COUNT(*) AS review_count
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.fact_reviews`
        WHERE sentiment_bucket IS NOT NULL
        GROUP BY sentiment_bucket
        ORDER BY review_count DESC
    """
    return client.query(sql).to_dataframe()


@st.cache_data(ttl=600, show_spinner="Loading monthly trend...")
def load_monthly_trend() -> pd.DataFrame:
    client = _get_bq_client()
    sql = f"""
        SELECT year_month, ROUND(AVG(stars), 3) AS avg_stars, COUNT(*) AS review_count
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.fact_reviews`
        WHERE year_month IS NOT NULL AND year_month >= '2008-01'
        GROUP BY year_month
        ORDER BY year_month
    """
    return client.query(sql).to_dataframe()


@st.cache_data(ttl=600, show_spinner="Loading star distribution...")
def load_star_distribution() -> pd.DataFrame:
    client = _get_bq_client()
    sql = f"""
        SELECT CAST(stars AS INT64) AS stars, COUNT(*) AS review_count
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.fact_reviews`
        WHERE stars IS NOT NULL
        GROUP BY stars
        ORDER BY stars
    """
    return client.query(sql).to_dataframe()


try:
    st.caption("Based on Yelp Open Dataset — 100,000 reviews analyzed")

    metrics = load_summary_metrics()
    m1, m2, m3 = st.columns(3)
    m1.metric("Total Reviews", f"{metrics['review_count']:,}")
    m2.metric("Total Businesses", f"{metrics['biz_count']:,}")
    m3.metric("Data Period", f"{metrics['min_date']} ~ {metrics['max_date']}")

    st.markdown("---")
    col1, col2 = st.columns(2)

    # Chart 1: Top 10 cities by avg stars
    with col1:
        st.subheader("Top 10 Cities by Average Star Rating")
        st.caption("Cities with at least 100 reviews only")
        df_city = load_city_avg_stars()
        chart_city = (
            alt.Chart(df_city)
            .mark_bar()
            .encode(
                x=alt.X("avg_stars:Q", scale=alt.Scale(domain=[0, 5]), title="Average Stars"),
                y=alt.Y("city:N", sort="-x", title="City"),
                color=alt.Color("avg_stars:Q", scale=alt.Scale(scheme="blues"), legend=None),
                tooltip=["city", "avg_stars", "review_count"],
            )
            .properties(height=350)
        )
        st.altair_chart(chart_city, use_container_width=True)

    # Chart 2: Sentiment distribution (donut)
    with col2:
        st.subheader("Sentiment Distribution")
        st.caption("4–5 stars: positive, 3 stars: neutral, 1–2 stars: negative")
        df_sentiment = load_sentiment_distribution()
        chart_sentiment = (
            alt.Chart(df_sentiment)
            .mark_arc(innerRadius=60)
            .encode(
                theta=alt.Theta("review_count:Q"),
                color=alt.Color(
                    "sentiment_bucket:N",
                    scale=alt.Scale(
                        domain=["positive", "neutral", "negative"],
                        range=["#2ecc71", "#f39c12", "#e74c3c"],
                    ),
                    legend=alt.Legend(title="Sentiment"),
                ),
                tooltip=["sentiment_bucket", "review_count"],
            )
            .properties(height=350)
        )
        st.altair_chart(chart_sentiment, use_container_width=True)

    col3, col4 = st.columns(2)

    # Chart 3: Monthly avg stars trend
    with col3:
        st.subheader("Monthly Average Star Rating Trend")
        st.caption("Average star rating change from 2008 to present")
        df_trend = load_monthly_trend()
        chart_trend = (
            alt.Chart(df_trend)
            .mark_line(point=True)
            .encode(
                x=alt.X("year_month:N", title="Month", axis=alt.Axis(labelAngle=-45)),
                y=alt.Y("avg_stars:Q", scale=alt.Scale(domain=[1, 5]), title="Average Stars"),
                tooltip=["year_month", "avg_stars", "review_count"],
            )
            .properties(height=350)
        )
        st.altair_chart(chart_trend, use_container_width=True)

    # Chart 4: Star distribution histogram
    with col4:
        st.subheader("Star Rating Distribution")
        st.caption("Proportion of all reviews by star rating")
        df_stars = load_star_distribution()
        chart_stars = (
            alt.Chart(df_stars)
            .mark_bar()
            .encode(
                x=alt.X("stars:O", title="Stars"),
                y=alt.Y("review_count:Q", title="Number of Reviews"),
                color=alt.Color(
                    "stars:O",
                    scale=alt.Scale(scheme="yellowgreenblue"),
                    legend=None,
                ),
                tooltip=["stars", "review_count"],
            )
            .properties(height=350)
        )
        st.altair_chart(chart_stars, use_container_width=True)

except Exception as e:
    st.error(f"Failed to load data from BigQuery: {e}")
