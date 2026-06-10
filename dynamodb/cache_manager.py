import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "yelp-insights")

CATEGORY_LABELS = {
    "general":              "General",
    "category_performance": "Category Performance",
    "review_engagement":    "Review Engagement",
    "regional_competition": "Regional Competition",
    "sentiment_depth":      "Sentiment Depth",
}

_dynamodb = boto3.resource(
    "dynamodb",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


def create_table_if_not_exists() -> object:
    try:
        table = _dynamodb.Table(DYNAMODB_TABLE)
        table.load()
        print(f"[DynamoDB] Table '{DYNAMODB_TABLE}' already exists.")
        return table
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    print(f"[DynamoDB] Creating table '{DYNAMODB_TABLE}'...")
    table = _dynamodb.create_table(
        TableName=DYNAMODB_TABLE,
        KeySchema=[
            {"AttributeName": "insight_id", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "insight_id", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()
    print(f"[DynamoDB] Table '{DYNAMODB_TABLE}' created.")
    return table


def save_insight(insight_text: str, category: str = "general") -> str:
    table = create_table_if_not_exists()
    now = datetime.now(timezone.utc)
    insight_id = f"insight_{int(now.timestamp() * 1000)}"

    item = {
        "insight_id": insight_id,
        "insight_text": insight_text,
        "created_at": now.isoformat(),
        "date": now.strftime("%Y-%m-%d"),
        "source": "cerebras/gpt-oss-120b",
        "category": category,
        "category_label": CATEGORY_LABELS.get(category, category),
    }
    table.put_item(Item=item)
    print(f"[DynamoDB] Saved: {insight_id} (category: {category})")
    return insight_id


def get_latest_insight() -> dict | None:
    table = create_table_if_not_exists()
    response = table.scan()
    items = response.get("Items", [])
    if not items:
        print("[DynamoDB] No insights found.")
        return None
    return max(items, key=lambda x: x["created_at"])


def get_insights_by_category(category: str) -> list[dict]:
    table = create_table_if_not_exists()
    response = table.scan(
        FilterExpression=Attr("category").eq(category),
    )
    items = response.get("Items", [])
    items.sort(key=lambda x: x["created_at"], reverse=True)
    return items


def get_insights_grouped_by_date() -> dict[str, list[dict]]:
    table = create_table_if_not_exists()
    response = table.scan()
    items = response.get("Items", [])
    items.sort(key=lambda x: x["created_at"], reverse=True)

    grouped: dict[str, list[dict]] = {}
    for item in items:
        date_key = item.get("date", item.get("created_at", "")[:10])
        grouped.setdefault(date_key, []).append(item)
    return grouped


def list_insights(limit: int = 10) -> list[dict]:
    table = create_table_if_not_exists()
    response = table.scan()
    items = response.get("Items", [])
    items.sort(key=lambda x: x["created_at"], reverse=True)
    return items[:limit]


if __name__ == "__main__":
    test_text = (
        "1. Cities with higher average ratings show stronger customer retention.\n"
        "2. Negative sentiment reviews spike in winter months.\n"
        "3. Businesses with over 500 reviews maintain a 0.3-star advantage."
    )

    print("=== Saving test insight ===")
    saved_id = save_insight(test_text, category="general")
    print(f"Saved insight_id: {saved_id}")

    print("\n=== Insights by category: general ===")
    items = get_insights_by_category("general")
    for item in items[:3]:
        print(f"  [{item['created_at']}] {item['insight_id']}")

    print("\n=== Grouped by date ===")
    grouped = get_insights_grouped_by_date()
    for date, entries in grouped.items():
        print(f"  {date}: {len(entries)} insight(s)")
