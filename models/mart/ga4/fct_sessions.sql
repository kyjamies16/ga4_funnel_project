SELECT
  session_id,
  user_pseudo_id,
  session_start,
  session_end,
  event_count,
  pageviews,
  session_value
FROM {{ ref('int_ga4__user_sessions') }}
