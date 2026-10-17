-- Glue database: telecom_fraud_radar (created by transform.athena.setup)
-- Silver paths and physical column names/types match the Parquet files written
-- by quality_gate. Each event-date partition contains deterministic data.parquet
-- or quarantine.parquet files. `timestamp` is quoted because Hive reserves it.

CREATE EXTERNAL TABLE IF NOT EXISTS activation (
  event_id string,
  subscriber_id string,
  msisdn string,
  imsi string,
  event_type string,
  `timestamp` string,
  channel string
)
PARTITIONED BY (year string, month string, day string)
STORED AS PARQUET
LOCATION 's3://telco-fraud-bronze/silver/activation/';

CREATE EXTERNAL TABLE IF NOT EXISTS purchases (
  purchase_id string,
  subscriber_id string,
  bundle_id string,
  channel string,
  price string,
  `timestamp` string
)
PARTITIONED BY (year string, month string, day string)
STORED AS PARQUET
LOCATION 's3://telco-fraud-bronze/silver/bundles/purchases/';

CREATE EXTERNAL TABLE IF NOT EXISTS voice (
  call_id string,
  subscriber_id string,
  destination_number string,
  duration_sec bigint,
  cost decimal(38,4),
  `timestamp` timestamp,
  country_code string
)
PARTITIONED BY (year string, month string, day string)
STORED AS PARQUET
LOCATION 's3://telco-fraud-bronze/silver/usage/voice/';

CREATE EXTERNAL TABLE IF NOT EXISTS data_sessions (
  session_id string,
  subscriber_id string,
  url_domain string,
  bytes_used bigint,
  cost decimal(38,4),
  `timestamp` timestamp,
  country_code string
)
PARTITIONED BY (year string, month string, day string)
STORED AS PARQUET
LOCATION 's3://telco-fraud-bronze/silver/usage/data/';

CREATE EXTERNAL TABLE IF NOT EXISTS revenue (
  transaction_id string,
  subscriber_id string,
  payment_type string,
  amount decimal(38,2),
  currency string,
  `timestamp` timestamp,
  status string
)
PARTITIONED BY (year string, month string, day string)
STORED AS PARQUET
LOCATION 's3://telco-fraud-bronze/silver/revenue/prepaid/';

CREATE EXTERNAL TABLE IF NOT EXISTS account_daily_features (
  subscriber_id string,
  activation_event_count bigint,
  sim_swap_count bigint,
  bundle_purchase_count bigint,
  bundle_spend double,
  distinct_bundles_purchased bigint,
  voice_call_count bigint,
  international_call_count bigint,
  voice_cost double,
  voice_duration_sec bigint,
  data_session_count bigint,
  data_bytes bigint,
  data_cost double,
  revenue_transaction_count bigint,
  net_revenue_amount double,
  failed_payment_count bigint
)
PARTITIONED BY (year string, month string, day string)
STORED AS PARQUET
LOCATION 's3://telco-fraud-bronze/gold/account_daily_features/';

CREATE EXTERNAL TABLE IF NOT EXISTS subscriber_profiles (
  subscriber_id string,
  msisdn string,
  imsi string,
  segment string,
  kyc_status string,
  account_age_days decimal(38,0),
  lines_per_identity decimal(38,0),
  risk_score_baseline string,
  last_updated string
)
PARTITIONED BY (snapshot_date string)
STORED AS PARQUET
LOCATION 's3://telco-fraud-bronze/gold/reference/subscriber_profile/';

CREATE EXTERNAL TABLE IF NOT EXISTS bundle_catalog (
  bundle_id string,
  name string,
  price string,
  allowance_type string,
  allowance_amount decimal(38,0),
  validity_days decimal(38,0),
  segment string,
  last_updated string
)
PARTITIONED BY (snapshot_date string)
STORED AS PARQUET
LOCATION 's3://telco-fraud-bronze/gold/reference/bundle_catalog/';
