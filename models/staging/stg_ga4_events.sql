WITH source AS (
  SELECT *
  FROM {{ source('ga4', 'events') }}
),

flattened AS (
  SELECT
    user_pseudo_id,
    event_name,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    {{ dbt_utils.generate_surrogate_key(['user_pseudo_id','event_timestamp','event_name']) }} AS event_id

    -- Nested fields
    device.category AS device_category,
    geo.country AS country,
    traffic_source.source AS traffic_source,
    traffic_source.medium AS traffic_medium,

    -- Flatten event params
    {{ get_event_param_value('page_location', 'string') }} AS page_location,
    {{ get_event_param_value('page_title', 'string') }} AS page_title,
    {{ get_event_param_value('item_name', 'string') }} AS item_name,
    {{ get_event_param_value('quantity', 'int') }} AS quantity,
    {{ get_event_param_value('value', 'double') }} AS event_value

  FROM source
)

SELECT * FROM flattened
