import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from cerebras.cloud.sdk import Cerebras
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dynamodb.cache_manager import save_insight

load_dotenv()

CEREBRAS_API_KEY = os.getenv("CEREBRAS_API_KEY")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "yelp-pipeline-498923")
BQ_DATASET = "yelp_analytics"
BQ_TABLE = "fact_reviews"
GCP_SERVICE_ACCOUNT_JSON = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "yelp-pipeline-498923-8b2caffededb.json")

CEREBRAS_MODEL = "gpt-oss-120b"

_GENERAL_QUERIES = {
    "top10_city_avg_stars": f"""
        SELECT b.city, ROUND(AVG(r.stars), 2) AS avg_stars, COUNT(*) AS review_count
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` r
        JOIN `{BQ_PROJECT_ID}.{BQ_DATASET}.dim_businesses` b USING (business_id)
        WHERE b.city IS NOT NULL
        GROUP BY b.city
        ORDER BY avg_stars DESC
        LIMIT 10
    """,
    "sentiment_distribution": f"""
        SELECT sentiment_bucket, COUNT(*) AS review_count,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE sentiment_bucket IS NOT NULL
        GROUP BY sentiment_bucket
        ORDER BY review_count DESC
    """,
    "monthly_avg_stars_trend": f"""
        SELECT year_month, ROUND(AVG(stars), 3) AS avg_stars, COUNT(*) AS review_count
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE year_month IS NOT NULL
        GROUP BY year_month
        ORDER BY year_month
    """,
}


def _get_bq_client() -> bigquery.Client:
    if os.path.exists(GCP_SERVICE_ACCOUNT_JSON):
        credentials = service_account.Credentials.from_service_account_file(
            GCP_SERVICE_ACCOUNT_JSON,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(project=BQ_PROJECT_ID, credentials=credentials)
    return bigquery.Client(project=BQ_PROJECT_ID)


def _query(client: bigquery.Client, sql: str) -> list[dict]:
    rows = list(client.query(sql.strip()).result())
    return [dict(r) for r in rows]


def _call_cerebras(prompt: str) -> str:
    client = Cerebras(api_key=CEREBRAS_API_KEY)
    response = client.chat.completions.create(
        model=CEREBRAS_MODEL,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.choices[0].message.content.strip()


# ── General ──────────────────────────────────────────────────────────────────

def generate_insights() -> str:
    bq = _get_bq_client()
    top10 = _query(bq, _GENERAL_QUERIES["top10_city_avg_stars"])
    sentiment = _query(bq, _GENERAL_QUERIES["sentiment_distribution"])
    trend = _query(bq, _GENERAL_QUERIES["monthly_avg_stars_trend"])

    top10_text = "\n".join(f"  {r['city']}: avg {r['avg_stars']} stars ({r['review_count']} reviews)" for r in top10)
    sentiment_text = "\n".join(f"  {r['sentiment_bucket']}: {r['review_count']} reviews ({r['pct']}%)" for r in sentiment)
    trend_text = "\n".join(f"  {r['year_month']}: avg {r['avg_stars']} stars ({r['review_count']} reviews)" for r in trend)

    prompt = f"""You are a business analytics expert. Below is aggregated Yelp review data.

[Top 10 Cities by Average Star Rating]
{top10_text}

[Review Sentiment Distribution]
{sentiment_text}

[Monthly Average Star Rating Trend]
{trend_text}

Based on this data, provide exactly 3 key insights from a business owner's perspective.
Each insight should be concise, actionable, and written in one sentence.
Format: numbered list (1. 2. 3.)"""

    insights = _call_cerebras(prompt)
    save_insight(insights, category="general")
    return insights


# ── Category Performance ──────────────────────────────────────────────────────

def generate_category_performance() -> str:
    bq = _get_bq_client()
    sql = f"""
        SELECT b.categories, ROUND(AVG(r.stars), 2) AS avg_stars, COUNT(*) AS review_count
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` r
        JOIN `{BQ_PROJECT_ID}.{BQ_DATASET}.dim_businesses` b USING (business_id)
        WHERE b.categories IS NOT NULL
        GROUP BY b.categories
        HAVING COUNT(*) >= 50
        ORDER BY avg_stars DESC
        LIMIT 15
    """
    rows = _query(bq, sql)
    data_text = "\n".join(f"  {r['categories']}: avg {r['avg_stars']} stars ({r['review_count']} reviews)" for r in rows)

    prompt = f"""You are a business analytics expert analyzing Yelp review data by business category.

[Top Categories by Average Star Rating (min 50 reviews)]
{data_text}

Based on this data, provide exactly 3 key insights from a business owner's perspective about category performance.
Focus on which categories consistently perform well and what that means for business strategy.
Each insight should be concise, actionable, and written in one sentence.
Format: numbered list (1. 2. 3.)"""

    insights = _call_cerebras(prompt)
    save_insight(insights, category="category_performance")
    return insights


# ── Review Engagement ─────────────────────────────────────────────────────────

def generate_review_engagement() -> str:
    bq = _get_bq_client()
    sql = f"""
        SELECT
            CAST(stars AS INT64) AS stars,
            ROUND(AVG(CAST(review_length AS FLOAT64)), 0) AS avg_review_length,
            ROUND(AVG(CAST(useful AS FLOAT64)), 2) AS avg_useful,
            ROUND(AVG(CAST(funny AS FLOAT64)), 2) AS avg_funny,
            ROUND(AVG(CAST(cool AS FLOAT64)), 2) AS avg_cool,
            COUNT(*) AS review_count
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE stars IS NOT NULL
          AND review_length IS NOT NULL
          AND useful IS NOT NULL
        GROUP BY stars
        ORDER BY stars
    """
    rows = _query(bq, sql)
    data_text = "\n".join(
        f"  {r['stars']} stars: avg length={r['avg_review_length']} chars, useful={r['avg_useful']}, funny={r['avg_funny']}, cool={r['avg_cool']} ({r['review_count']} reviews)"
        for r in rows
    )

    prompt = f"""You are a business analytics expert analyzing Yelp review engagement metrics.

[Review Engagement by Star Rating]
{data_text}

Based on this data, provide exactly 3 key insights from a business owner's perspective about customer engagement.
Focus on how review length and social signals (useful/funny/cool) differ across star ratings.
Each insight should be concise, actionable, and written in one sentence.
Format: numbered list (1. 2. 3.)"""

    insights = _call_cerebras(prompt)
    save_insight(insights, category="review_engagement")
    return insights


# ── Regional Competition ──────────────────────────────────────────────────────

def generate_regional_competition() -> str:
    bq = _get_bq_client()
    sql = f"""
        SELECT
            b.city,
            COUNT(DISTINCT b.business_id) AS business_count,
            ROUND(AVG(r.stars), 2) AS avg_stars,
            COUNT(*) AS review_count
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` r
        JOIN `{BQ_PROJECT_ID}.{BQ_DATASET}.dim_businesses` b USING (business_id)
        WHERE b.city IS NOT NULL
        GROUP BY b.city
        HAVING COUNT(*) >= 200
        ORDER BY business_count DESC
        LIMIT 15
    """
    rows = _query(bq, sql)
    data_text = "\n".join(
        f"  {r['city']}: {r['business_count']} businesses, avg {r['avg_stars']} stars ({r['review_count']} reviews)"
        for r in rows
    )

    prompt = f"""You are a business analytics expert analyzing regional market competition using Yelp data.

[Cities by Business Count vs Average Star Rating (min 200 reviews)]
{data_text}

Based on this data, provide exactly 3 key insights from a business owner's perspective about regional competition.
Focus on the relationship between market density and customer satisfaction.
Each insight should be concise, actionable, and written in one sentence.
Format: numbered list (1. 2. 3.)"""

    insights = _call_cerebras(prompt)
    save_insight(insights, category="regional_competition")
    return insights


# ── Sentiment Depth ───────────────────────────────────────────────────────────

def generate_sentiment_depth() -> str:
    bq = _get_bq_client()
    sql = f"""
        SELECT
            CAST(stars AS INT64) AS stars,
            sentiment_bucket,
            ROUND(AVG(review_length), 0) AS avg_review_length,
            COUNT(*) AS review_count,
            ROUND(AVG(useful), 2) AS avg_useful
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE stars IS NOT NULL AND sentiment_bucket IS NOT NULL
        GROUP BY stars, sentiment_bucket
        ORDER BY stars
    """
    rows = _query(bq, sql)
    data_text = "\n".join(
        f"  {r['stars']} stars ({r['sentiment_bucket']}): avg length={r['avg_review_length']} chars, useful={r['avg_useful']}, count={r['review_count']}"
        for r in rows
    )

    prompt = f"""You are a business analytics expert analyzing sentiment patterns in Yelp reviews.

[Review Length and Usefulness by Star Rating and Sentiment]
{data_text}

Based on this data, provide exactly 3 key insights from a business owner's perspective about negative review patterns and sentiment depth.
Focus on what makes negative reviews different from positive ones, and what businesses can learn from that.
Each insight should be concise, actionable, and written in one sentence.
Format: numbered list (1. 2. 3.)"""

    insights = _call_cerebras(prompt)
    save_insight(insights, category="sentiment_depth")
    return insights


# ── Run All ───────────────────────────────────────────────────────────────────

def generate_all_insights() -> dict[str, str]:
    tasks = {
        "category_performance": generate_category_performance,
        "review_engagement":    generate_review_engagement,
        "regional_competition": generate_regional_competition,
        "sentiment_depth":      generate_sentiment_depth,
    }
    results = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fn): category for category, fn in tasks.items()}
        for future in as_completed(futures):
            category = futures[future]
            try:
                results[category] = future.result()
                print(f"[Cerebras] Done: {category}")
            except Exception as e:
                print(f"[Cerebras] Failed: {category} — {e}")
                results[category] = ""
    return results


if __name__ == "__main__":
    print("=== General Insights ===")
    print(generate_insights())
    print("\n=== All Category Insights ===")
    all_results = generate_all_insights()
    for cat, text in all_results.items():
        print(f"\n--- {cat} ---\n{text}")
