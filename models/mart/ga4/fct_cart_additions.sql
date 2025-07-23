with base as (

    select *
    from {{ ref('int_ga4__ecommerce_item_events') }}
    where event_name = 'add_to_cart'

),

cart_adds as (

    select
        user_id,
        event_timestamp,
        event_date,
        item_name,
        quantity,
        event_value,
        platform,
        device_category,
        traffic_source,
        traffic_medium,
        country

    from base
    where item_name is not null
      and quantity is not null

)

select * from cart_adds
