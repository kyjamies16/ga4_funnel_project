{{ config(materialized='ephemeral') }}

with events as (
    select * from {{ ref('stg_ga4__events') }}
),

sessionized as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['user_pseudo_id', 'event_timestamp']) }} as session_id
    from events
)

select * from sessionized
