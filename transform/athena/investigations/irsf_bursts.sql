-- Daily IRSF signal: concentrated calls to the simulated high-risk destinations.
-- This query is an investigation aid; the Python rule uses a rolling 24-hour window.
WITH unique_calls AS (
  SELECT call_id, subscriber_id, "timestamp" AS event_timestamp, cost, country_code
  FROM (
    SELECT v.*, row_number() OVER (PARTITION BY call_id ORDER BY year DESC, month DESC, day DESC) AS rn
    FROM voice v
  ) WHERE rn = 1
)
SELECT subscriber_id, date(event_timestamp) AS event_date,
       count(*) AS high_risk_calls, sum(cost) AS high_risk_cost
FROM unique_calls
WHERE country_code IN ('SO', 'PK', 'LV')
GROUP BY subscriber_id, date(event_timestamp)
HAVING count(*) >= 10;
