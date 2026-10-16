-- Repeat purchase proxy for bundle abuse. Source 6 (bundle consumption) is
-- deferred, so this does not claim to detect allowance over-consumption.
WITH unique_purchases AS (
  SELECT purchase_id, subscriber_id, bundle_id, "timestamp" AS event_timestamp, price
  FROM (
    SELECT p.*, row_number() OVER (PARTITION BY purchase_id ORDER BY year DESC, month DESC, day DESC) AS rn
    FROM purchases p
  ) WHERE rn = 1
)
SELECT subscriber_id, bundle_id,
       substr(event_timestamp, 1, 10) AS event_date,
       count(*) AS repeat_purchases, sum(cast(price AS double)) AS spend
FROM unique_purchases
GROUP BY subscriber_id, bundle_id, substr(event_timestamp, 1, 10)
HAVING count(*) >= 3;
