SELECT
  session_id, 
  user_pseudo_id,
  MIN(event_timestamp) AS session_start,
  MAX(event_timestamp) AS session_end,
  COUNT(*) AS event_count,
  COUNTIF(event_name = 'page_view') AS pageviews,
  SUM(IFNULL(event_value, 0)) AS session_value
FROM {{ ref('int_ga4__sessionized_events') }}
GROUP BY user_pseudo_id, session_id
