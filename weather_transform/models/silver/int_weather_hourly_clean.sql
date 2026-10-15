{{ config(materialized='table') }}

WITH staging_data AS (
    SELECT * FROM {{ ref('stg_weather_hourly') }}
),

quarantine_data AS (
    SELECT city_id, observed_at_utc 
    FROM {{ ref('quarantine_weather_hourly') }}
)

SELECT s.*
FROM staging_data s
WHERE NOT EXISTS (
    SELECT 1 FROM quarantine_data q
    WHERE s.city_id = q.city_id AND s.observed_at_utc = q.observed_at_utc
)