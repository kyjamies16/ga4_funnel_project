SELECT
  user_pseudo_id,
  session_id,
  event_name,
  item_name,
  quantity,
  event_value,
  event_timestamp
FROM {{ ref('int_ga4__ecommerce_events') }}
