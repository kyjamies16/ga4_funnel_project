SELECT
  session_id, 
  user_pseudo_id,
  event_name,
  item_name,
  quantity,
  event_value,
  event_timestamp
FROM {{ ref('int_ga4__sessionized_events') }}
WHERE event_name IN ('view_item', 'add_to_cart', 'purchase')
