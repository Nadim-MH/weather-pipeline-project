-- models/gold/mart_pipeline_health.sql
{{ config(materialized='view') }}

WITH bronze_daily AS (
    SELECT
        DATE(pulled_at) AS run_date,
        COUNT(*) AS records_ingested,
        MIN(pulled_at) AS run_started_at,
        MAX(pulled_at) AS run_ended_at
    FROM {{ source('bronze', 'raw_weather_responses') }}
    GROUP BY run_date
),

quarantine_daily AS (
    SELECT
        DATE(quarantined_at) AS run_date,
        COUNT(*) AS records_quarantined
    FROM {{ ref('quarantine_weather_hourly') }}
    GROUP BY run_date
)

SELECT
    b.run_date,
    b.records_ingested,
    COALESCE(q.records_quarantined, 0) AS records_quarantined,
    b.run_started_at,
    b.run_ended_at,
    TIMESTAMP_DIFF(b.run_ended_at, b.run_started_at, SECOND) AS run_duration_seconds
FROM bronze_daily b
LEFT JOIN quarantine_daily q ON b.run_date = q.run_date
ORDER BY b.run_date DESC