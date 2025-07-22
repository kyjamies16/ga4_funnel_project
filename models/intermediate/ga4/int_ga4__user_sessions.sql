SELECT
  session_id,
  user_pseudo_id,
  MIN(event_timestamp) AS session_start,
  MAX(event_timestamp) AS session_end,
  TIMESTAMP_DIFF(MAX(event_timestamp), MIN(event_timestamp), SECOND) AS session_duration_seconds,

  COUNT(*) AS event_count,
  COUNTIF(event_name = 'page_view') AS pageviews,
  SUM(IFNULL(event_value, 0)) AS session_value,

  -- Entry page and attribution
  ARRAY_AGG(page_location ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS landing_page,
  ARRAY_AGG(traffic_source IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS traffic_source,
  ARRAY_AGG(traffic_medium IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS traffic_medium,

  ARRAY_AGG(device_category IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS device_category,
  ARRAY_AGG(platform IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS platform

FROM {{ ref('int_ga4__sessionized_events') }}
GROUP BY user_pseudo_id, session_id
