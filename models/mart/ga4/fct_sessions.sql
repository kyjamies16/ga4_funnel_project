SELECT
  session_id,
  user_pseudo_id,
  session_start,
  session_end,
  TIMESTAMP_DIFF(session_end, session_start, SECOND) AS session_duration_seconds,
  event_count,
  pageviews,
  session_value,
  traffic_source,
  traffic_medium,
  device_category,
  platform
FROM {{ ref('int_ga4__user_sessions') }}
