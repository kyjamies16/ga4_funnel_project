WITH events AS (
  SELECT
    user_pseudo_id,
    event_name,
    event_timestamp,
    event_date,
    page_location,
    page_title,
    item_name,
    quantity,
    event_value,
    purchase_revenue_in_usd,
    total_item_quantity,
    unique_items,
    tax_value_in_usd,
    shipping_value_in_usd,
    device_category,
    traffic_source,
    traffic_medium,
    transaction_id,
    platform,
  FROM {{ ref('stg_ga4__events') }}
),

-- Add previous event timestamp for each user to help identify session boundaries
ranked_events AS (
  SELECT
    *,
    LAG(event_timestamp) OVER (
      PARTITION BY user_pseudo_id ORDER BY event_timestamp
    ) AS prev_event_timestamp
  FROM events
),

-- Flag the start of a new session if the time difference is greater than 30 minutes or if it's the user's first event
session_flags AS (
  SELECT
    *,
    IF(
      TIMESTAMP_DIFF(event_timestamp, prev_event_timestamp, MINUTE) > 30
      OR prev_event_timestamp IS NULL,
      1,
      0
    ) AS is_new_session
  FROM ranked_events
),

-- Assign a session number to each event by cumulatively summing the session flags per user
sessioned AS (
  SELECT
    *,
    SUM(is_new_session) OVER (
      PARTITION BY user_pseudo_id ORDER BY event_timestamp
    ) AS session_num
  FROM session_flags
),

-- Generate a surrogate session_id and calculate the session start timestamp
final AS (
  SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['user_pseudo_id', 'session_num']) }} AS session_id,
    MIN(event_timestamp) OVER (
      PARTITION BY user_pseudo_id, session_num
    ) AS session_start_timestamp
  FROM sessioned
)

-- Output the final sessionized events
SELECT * FROM final
