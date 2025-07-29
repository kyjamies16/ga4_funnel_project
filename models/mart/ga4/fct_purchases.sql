with base as (

    select *
    from {{ ref('int_ga4__ecommerce_item_events') }}
    where event_name = 'purchase'
      and transaction_id is not null

)

select
    transaction_id,
    user_id,
    item_name,
    platform,
    device_category,
    traffic_source,
    traffic_medium,
    country,
    min(event_timestamp) as transaction_timestamp,
    sum(purchase_revenue) as item_revenue,
    sum(tax_value) as item_tax,
    sum(shipping_value) as item_shipping,
    sum(total_item_quantity) as item_quantity
from base
group by
    transaction_id,
    user_id,
    item_name,
    platform,
    device_category,
    traffic_source,
    traffic_medium,
    country
