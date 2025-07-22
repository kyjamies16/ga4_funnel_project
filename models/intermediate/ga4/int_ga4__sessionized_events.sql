WITH events AS (
  SELECT
    user_pseudo_id,
    event_name,
    event_timestamp,
    event_timestamp AS event_ts,
    page_location,
    item_name,
    quantity,
    event_value 
  FROM {{ ref('stg_ga4__events') }}
),

-- Add previous event timestamp per user for sessionization logic
ranked_events AS (
  SELECT
    *,
    LAG(event_ts) OVER (PARTITION BY user_pseudo_id ORDER BY event_ts) AS prev_event_ts
  FROM events
),

-- Flag new sessions if time difference > 30 minutes or first event for user
session_flags AS (
  SELECT
    *,
    IF(
      TIMESTAMP_DIFF(event_ts, prev_event_ts, MINUTE) > 30 OR prev_event_ts IS NULL,
      1,
      0
    ) AS is_new_session
  FROM ranked_events
),

-- Assign session numbers by cumulatively summing new session flags per user
sessioned AS (
  SELECT
    *,
    SUM(is_new_session) OVER (PARTITION BY user_pseudo_id ORDER BY event_ts) AS session_num
  FROM session_flags
),

-- Generate a unique session_id using user_pseudo_id and session_num
final AS (
  SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['user_pseudo_id', 'session_num']) }} AS session_id
  FROM sessioned
)

-- Output the final sessionized events
SELECT * FROM final
