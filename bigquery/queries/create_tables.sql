-- ============================================================
-- Project : yelp-pipeline-498923
-- Dataset : yelp_analytics
-- ============================================================

-- Step 1: Create dataset
CREATE SCHEMA IF NOT EXISTS `yelp-pipeline-498923.yelp_analytics`
OPTIONS (
  description = 'Yelp streaming pipeline analytics dataset',
  location     = 'US'
);

-- ============================================================
-- Step 2: fact_reviews
--   Partitioned by DATE(ingest_timestamp)
--   Clustered by sentiment_bucket
-- ============================================================
CREATE OR REPLACE TABLE `yelp-pipeline-498923.yelp_analytics.fact_reviews`
(
  review_id        STRING    NOT NULL,
  business_id      STRING    NOT NULL,
  user_id          STRING,
  stars            FLOAT64   NOT NULL,
  date             DATE      NOT NULL,
  useful           INT64,
  funny            INT64,
  cool             INT64,
  review_length    INT64,
  sentiment_bucket STRING,
  year_month       STRING,
  ingest_timestamp TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingest_timestamp)
CLUSTER BY sentiment_bucket
OPTIONS (
  description              = 'Yelp review fact table, partitioned by ingest date, clustered by sentiment',
  require_partition_filter = FALSE
);

-- ============================================================
-- Step 3: dim_businesses
-- ============================================================
CREATE OR REPLACE TABLE `yelp-pipeline-498923.yelp_analytics.dim_businesses`
(
  business_id  STRING  NOT NULL,
  name         STRING,
  city         STRING,
  state        STRING,
  categories   STRING,
  avg_stars    FLOAT64,
  review_count INT64
)
OPTIONS (
  description = 'Yelp business dimension table'
);

-- ============================================================
-- Step 4: dim_dates
-- ============================================================
CREATE OR REPLACE TABLE `yelp-pipeline-498923.yelp_analytics.dim_dates`
(
  date_id    INT64  NOT NULL,
  full_date  DATE   NOT NULL,
  year       INT64  NOT NULL,
  month      INT64  NOT NULL,
  day        INT64  NOT NULL,
  year_month STRING NOT NULL
)
OPTIONS (
  description = 'Date dimension table'
);
