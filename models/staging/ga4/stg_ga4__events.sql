WITH source AS (
  SELECT *
  FROM {{ source('ga4', 'events') }}
  WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'

),

flattened AS (
  SELECT
    -- Identifiers & core
    user_pseudo_id,
    event_name,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    event_date,
    event_bundle_sequence_id,
    event_value_in_usd,
    platform,
    stream_id,

    -- Attribution
    traffic_source.source AS traffic_source,
    traffic_source.medium AS traffic_medium,
    traffic_source.name AS campaign_name,

    -- Device
    device.category AS device_category,
    device.mobile_brand_name AS mobile_brand_name,
    device.operating_system AS operating_system,

    -- Geo
    geo.country AS country,

    -- LTV & first-touch
    TIMESTAMP_MICROS(user_first_touch_timestamp) AS user_first_touch_timestamp,
    user_ltv.revenue AS user_ltv_revenue,

    -- Flattened event_params
    {{ get_event_param_value('page_location', 'string') }} AS page_location,
    {{ get_event_param_value('page_title', 'string') }} AS page_title,
    {{ get_event_param_value('item_name', 'string') }} AS item_name,
    {{ get_event_param_value('quantity', 'int') }} AS quantity,
    {{ get_event_param_value('value', 'double') }} AS event_value,

    -- Ecommerce fields
    ecommerce.transaction_id,
    ecommerce.purchase_revenue_in_usd,
    ecommerce.total_item_quantity,
    ecommerce.unique_items,
    ecommerce.tax_value_in_usd,
    ecommerce.shipping_value_in_usd

  FROM source
)

SELECT * FROM flattened
