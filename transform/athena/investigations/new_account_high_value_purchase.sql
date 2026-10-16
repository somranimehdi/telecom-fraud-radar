-- Young / non-verified accounts with a high-value purchase and no completed
-- revenue transaction on the same date. Uses the latest exported profile snapshot.
WITH latest_snapshot AS (
  SELECT max(snapshot_date) AS snapshot_date FROM subscriber_profiles
), current_profiles AS (
  SELECT p.* FROM subscriber_profiles p
  JOIN latest_snapshot s ON p.snapshot_date = s.snapshot_date
), unique_purchases AS (
  SELECT purchase_id, subscriber_id, "timestamp" AS event_timestamp, bundle_id, price
  FROM (
    SELECT p.*, row_number() OVER (PARTITION BY purchase_id ORDER BY year DESC, month DESC, day DESC) AS rn
    FROM purchases p
  ) WHERE rn = 1
), unique_revenue AS (
  SELECT transaction_id, subscriber_id, "timestamp" AS event_timestamp, status
  FROM (
    SELECT r.*, row_number() OVER (PARTITION BY transaction_id ORDER BY year DESC, month DESC, day DESC) AS rn
    FROM revenue r
  ) WHERE rn = 1
)
SELECT p.subscriber_id, p.event_timestamp AS purchase_timestamp,
       p.bundle_id, p.price, c.kyc_status, c.account_age_days
FROM unique_purchases p
JOIN current_profiles c ON c.subscriber_id = p.subscriber_id
LEFT JOIN unique_revenue r
  ON r.subscriber_id = p.subscriber_id
 AND substr(cast(r.event_timestamp AS string), 1, 10) = substr(p.event_timestamp, 1, 10)
 AND r.status = 'completed'
WHERE cast(p.price AS double) >= 75
  AND (c.account_age_days <= 30 OR c.kyc_status <> 'verified')
  AND r.transaction_id IS NULL;
