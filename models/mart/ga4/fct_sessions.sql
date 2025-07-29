with events as (

    select
        user_id,
        event_name,
        event_timestamp,
        event_date,
        page_location,
        page_title,
        item_name,
        quantity,
        event_value,
        purchase_revenue,
        total_item_quantity,
        unique_items,
        tax_value,
        shipping_value,
        device_category,
        traffic_source,
        traffic_medium,
        transaction_id,
        platform
    from {{ ref('stg_ga4__events') }}

),

ranked_events as (

    select
        *,
        lag(event_timestamp) over (
            partition by user_id order by event_timestamp
        ) as prev_event_timestamp
    from events

),

session_flags as (

    select
        *,
        if(
            timestamp_diff(event_timestamp, prev_event_timestamp, MINUTE) > 30
            or prev_event_timestamp is null,
            1,
            0
        ) as is_new_session
    from ranked_events

),

sessioned as (

    select
        *,
        sum(is_new_session) over (
            partition by user_id order by event_timestamp
        ) as session_num
    from session_flags

),

final as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key(['user_id', 'session_num']) }} as session_id,
        min(event_timestamp) over (
            partition by user_id, session_num
        ) as session_start_timestamp
    from sessioned

),

aggregated as (

    select
        session_id,
        user_id,
        session_start_timestamp,
        min(event_date) as session_date,
        max(event_timestamp) as session_end_timestamp,
        timestamp_diff(max(event_timestamp), session_start_timestamp, SECOND) as session_duration_sec,

        -- Behavior metrics
        countif(event_name = 'page_view') as pageviews,
        countif(event_name = 'purchase') as purchases,

        -- Revenue & quantity
        sum(purchase_revenue) as total_revenue,
        sum(total_item_quantity) as total_items,
        sum(unique_items) as unique_item_count,

        -- Attribution & device metadata
        any_value(platform) as platform,
        any_value(device_category) as device_category,
        any_value(traffic_source) as traffic_source,
        any_value(traffic_medium) as traffic_medium

    from final
    group by session_id, user_id, session_start_timestamp

)

select * from aggregated
