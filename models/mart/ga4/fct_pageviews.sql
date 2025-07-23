with base as (

    select *
    from {{ ref('stg_ga4__events') }}
    where event_name = 'page_view'

),

pageviews as (

    select
        user_id,
        event_timestamp,
        event_date,
        page_title,
        page_location,
        platform,
        device_category,
        traffic_source,
        traffic_medium,
        country

    from base
    where page_title is not null

)

select * from pageviews
