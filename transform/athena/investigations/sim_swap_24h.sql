-- SIM swap followed by one or more international calls within 24 hours.
WITH activations AS (
  SELECT event_id, subscriber_id, event_type, "timestamp" AS event_timestamp
  FROM (
    SELECT a.*, row_number() OVER (PARTITION BY event_id ORDER BY year DESC, month DESC, day DESC) AS rn
    FROM activation a
  ) WHERE rn = 1
), calls AS (
  SELECT call_id, subscriber_id, cost, "timestamp" AS event_timestamp, country_code
  FROM (
    SELECT v.*, row_number() OVER (PARTITION BY call_id ORDER BY year DESC, month DESC, day DESC) AS rn
    FROM voice v
  ) WHERE rn = 1
)
SELECT
  a.subscriber_id,
  a.event_timestamp AS sim_swap_timestamp,
  count(*) AS international_calls_24h,
  sum(v.cost) AS international_cost_24h
FROM activations a
JOIN calls v
  ON v.subscriber_id = a.subscriber_id
 AND with_timezone(v.event_timestamp, 'UTC')
       >= from_iso8601_timestamp(a.event_timestamp)
 AND with_timezone(v.event_timestamp, 'UTC')
       < date_add('hour', 24, from_iso8601_timestamp(a.event_timestamp))
WHERE a.event_type = 'SWAPPED'
  AND v.country_code <> 'TN'
GROUP BY a.subscriber_id, a.event_timestamp;
