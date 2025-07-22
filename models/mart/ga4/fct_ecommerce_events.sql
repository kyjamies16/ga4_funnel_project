SELECT
  user_pseudo_id,
  session_id,
  event_date,
  event_name,
  event_timestamp,
  item_name,
  quantity,
  event_value,

  -- Page context
  page_location,
  page_title,

  -- Attribution and device
  traffic_source,
  traffic_medium,
  device_category,
  platform,

  -- Purchase details (for 'purchase' events)
  transaction_id,
  purchase_revenue_in_usd,
  total_item_quantity,
  unique_items,
  tax_value_in_usd,
  shipping_value_in_usd

FROM {{ ref('int_ga4__ecommerce_events') }}
