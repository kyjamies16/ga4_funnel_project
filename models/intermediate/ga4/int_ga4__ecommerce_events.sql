SELECT
  session_id,
  user_pseudo_id,
  event_name,
  item_name,
  quantity,
  event_value,
  event_timestamp,
  event_date,
  transaction_id,
  purchase_revenue_in_usd,
  total_item_quantity,
  unique_items,
  tax_value_in_usd,
  shipping_value_in_usd,
  page_location,
  page_title,
  device_category,
  traffic_source,
  traffic_medium,
  platform
FROM {{ ref('int_ga4__sessionized_events') }}
WHERE event_name IN ('view_item', 'add_to_cart', 'purchase')
