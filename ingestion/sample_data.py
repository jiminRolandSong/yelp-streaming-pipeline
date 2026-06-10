import json
import os

from dotenv import load_dotenv

load_dotenv()

YELP_DATASET_PATH = os.getenv("YELP_DATASET_PATH", "")  # reviews JSON Lines file
YELP_BUSINESS_PATH = os.getenv("YELP_BUSINESS_PATH", "")  # business JSON Lines file
SAMPLE_SIZE = int(os.getenv("SAMPLE_SIZE", "100000"))

OUTPUT_DIR = "data"
REVIEWS_OUTPUT = os.path.join(OUTPUT_DIR, "sampled_reviews.json")
BUSINESS_OUTPUT = os.path.join(OUTPUT_DIR, "businesses.json")


def sample_reviews(src: str, dst: str, sample_size: int) -> int:
    count = 0
    with open(src, "r", encoding="utf-8") as fin, open(dst, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            fout.write(line + "\n")
            count += 1
            if count % 10000 == 0:
                print(f"[reviews] {count:,} records processed...")
            if count >= sample_size:
                break
    return count


def copy_businesses(src: str, dst: str) -> int:
    count = 0
    with open(src, "r", encoding="utf-8") as fin, open(dst, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            fout.write(line + "\n")
            count += 1
            if count % 10000 == 0:
                print(f"[business] {count:,} records processed...")
    return count


def main():
    if not YELP_DATASET_PATH:
        raise ValueError("YELP_DATASET_PATH is not set in .env.")
    if not YELP_BUSINESS_PATH:
        raise ValueError("YELP_BUSINESS_PATH is not set in .env.")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Starting review sampling: up to {SAMPLE_SIZE:,} records")
    review_count = sample_reviews(YELP_DATASET_PATH, REVIEWS_OUTPUT, SAMPLE_SIZE)
    print(f"Reviews done: {review_count:,} records -> {REVIEWS_OUTPUT}")

    print("Starting business data copy")
    business_count = copy_businesses(YELP_BUSINESS_PATH, BUSINESS_OUTPUT)
    print(f"Business done: {business_count:,} records -> {BUSINESS_OUTPUT}")


if __name__ == "__main__":
    main()
