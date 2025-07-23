with base as (

    select *
    from {{ ref('stg_ga4__events') }}

),

user_first_touch as (

    select
        user_id,

        -- First seen timestamp
        min(user_first_touch_timestamp) as first_touch_ts,
        min(event_date) as first_seen_date,

        -- LTV
        max(user_ltv_revenue) as lifetime_value,

        
        any_value(device_category) as default_device_category,
        any_value(traffic_source) as default_traffic_source,
        any_value(traffic_medium) as default_traffic_medium,
        any_value(country) as default_country,
        any_value(state) as default_state,
        any_value(city) as default_city,
        any_value(platform) as default_platform
        
    from base
    group by user_id

)

select * from user_first_touch
