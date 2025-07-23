with distinct_events as (

    select distinct event_name
    from {{ ref('stg_ga4__events') }}

),

categorized as (

    select
        event_name,
        case
            when event_name in ('purchase') then 'transactional'
            when event_name in ('add_to_cart', 'view_item') then 'engagement'
            when event_name in ('page_view', 'session_start', 'scroll') then 'navigational'
            when event_name in ('first_visit', 'user_engagement') then 'lifecycle'
            else 'other'
        end as event_category

    from distinct_events

)

select * from categorized
