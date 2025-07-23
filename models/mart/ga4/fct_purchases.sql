with base as (

    select *
    from {{ ref('int_ga4__ecommerce_item_events') }}
    where event_name = 'purchase'
      and transaction_id is not null

),

aggregated as (

    select
        transaction_id,
        min(event_timestamp) as transaction_timestamp,
        max(user_id) as user_id,  -- If 1:1 with transaction, otherwise consider array_agg distinct
        sum(purchase_revenue) as total_revenue,
        sum(tax_value) as total_tax,
        sum(shipping_value) as total_shipping,
        sum(total_item_quantity) as item_quantity,
        sum(unique_items) as unique_items_count,
        max(platform) as platform,
        max(device_category) as device_category,
        max(traffic_source) as traffic_source,
        max(traffic_medium) as traffic_medium,
        max(country) as country

    from base
    group by transaction_id

)

select * from aggregated
