with source as (

  select *
  from {{ source('ga4', 'events') }}
  where _TABLE_SUFFIX between '20201101' and '20210131'

),

flattened as (

  select
    -- Identifiers & core
    user_pseudo_id as user_id,
    event_name,
    timestamp_micros(event_timestamp) as event_timestamp,
    parse_date('%Y%m%d', event_date) as event_date,
    event_bundle_sequence_id,
    event_value_in_usd,
    platform,
    cast(stream_id as string) as stream_id,

    -- Attribution
    traffic_source.source as traffic_source,
    traffic_source.medium as traffic_medium,
    traffic_source.name as campaign_name,

    -- Device
    device.category as device_category,
    device.mobile_brand_name,
    device.operating_system,

    -- Geo
    geo.country as country,
    geo.region as state,
    geo.city as city,

    -- LTV & first-touch
    timestamp_micros(user_first_touch_timestamp) as user_first_touch_timestamp,
    user_ltv.revenue as user_ltv_revenue,

    -- Flattened event parameters using macros
    {{ get_event_param_value('page_location', 'string') }} as page_location,
    {{ get_event_param_value('page_title', 'string') }} as page_title,
    {{ get_event_param_value('item_name', 'string') }} as item_name,
    {{ get_event_param_value('quantity', 'int') }} as quantity,
    {{ get_event_param_value('value', 'double') }} as event_value,

    -- Ecommerce object (only populated for 'purchase' events)
    ecommerce.transaction_id,
    ecommerce.purchase_revenue_in_usd as purchase_revenue,
    ecommerce.total_item_quantity,
    ecommerce.unique_items,
    ecommerce.tax_value_in_usd as tax_value,
    ecommerce.shipping_value_in_usd as shipping_value, 

  from source

)

select * from flattened
