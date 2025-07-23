with base as (

    select *
    from {{ ref('stg_ga4__events') }}
    where event_name in ('add_to_cart', 'view_item', 'purchase')

),

item_events as (

    select
        user_id,
        event_name,
        event_timestamp,
        event_date,
        item_name,
        quantity,
        event_value,
        transaction_id,
        purchase_revenue,
        total_item_quantity,
        unique_items,
        tax_value,
        shipping_value,
        platform,
        device_category,
        traffic_source,
        traffic_medium,
        country

    from base
    where item_name is not null or quantity is not null or purchase_revenue is not null

)

select * from item_events
