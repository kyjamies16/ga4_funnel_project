WITH user_events AS (
  SELECT
    user_pseudo_id,
    device_category,
    country,
    traffic_source,
    traffic_medium,
    event_timestamp AS event_time
  FROM {{ ref('stg_ga4__events') }}
),

first_touch AS (
  SELECT
    user_pseudo_id,
    country,
    source,
    medium,
    event_time,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_time) AS rn
  FROM user_events
),

most_common_country AS (
  SELECT
    user_pseudo_id,
    country,
    COUNT(*) AS country_event_count,
    RANK() OVER (PARTITION BY user_pseudo_id ORDER BY COUNT(*) DESC) AS geo_rank
  FROM user_events
  WHERE country IS NOT NULL
  GROUP BY user_pseudo_id, country
)

SELECT
  u.user_pseudo_id AS user_id,
  MIN(u.event_time) AS first_seen_at,
  MAX(u.event_time) AS last_seen_at,
  mc.country AS most_common_country,
  ft.source AS first_touch_source,
  ft.medium AS first_touch_medium,
  ft.country AS first_touch_country
FROM user_events u
LEFT JOIN most_common_country mc
  ON u.user_pseudo_id = mc.user_pseudo_id AND mc.geo_rank = 1
LEFT JOIN first_touch ft
  ON u.user_pseudo_id = ft.user_pseudo_id AND ft.rn = 1
GROUP BY
  u.user_pseudo_id,
  mc.country,
  ft.source,
  ft.medium,
  ft.country
