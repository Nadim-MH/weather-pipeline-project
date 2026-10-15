{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date_day', 'data_type': 'date'},
    cluster_by=['city_id']
) }}

WITH hourly_local AS (
    SELECT
        city_id,
        DATE(observed_at_utc, 'Asia/Riyadh') AS date_day,
        temperature_2m_c,
        relative_humidity_2m_percent,
        precipitation_mm,
        weather_code
    FROM {{ ref('fct_weather_hourly') }}
    
    {% if is_incremental() %}
    -- استخدام توقيت الرياض لضمان دقة نافذة التحديث
    WHERE DATE(observed_at_utc, 'Asia/Riyadh') >= DATE_SUB(CURRENT_DATE('Asia/Riyadh'), INTERVAL 14 DAY)
    {% endif %}
),

daily_agg AS (
    SELECT
        city_id,
        date_day,
        MIN(temperature_2m_c) AS temp_min_c,
        MAX(temperature_2m_c) AS temp_max_c,
        AVG(temperature_2m_c) AS temp_avg_c,
        AVG(relative_humidity_2m_percent) AS humidity_avg_pct,
        SUM(precipitation_mm) AS precipitation_sum_mm,
        SUM(precipitation_mm) > 0.1 AS is_rainy_day,
        COUNT(*) AS hours_observed
    FROM hourly_local
    GROUP BY city_id, date_day
),

dominant_code AS (
    -- أشد كود طقس بنفس اليوم (أعلى رقم = أشد شدة تقريباً حسب ترتيب WMO)
    SELECT
        city_id,
        date_day,
        weather_code AS dominant_weather_code
    FROM (
        SELECT
            city_id,
            date_day,
            weather_code,
            ROW_NUMBER() OVER (
                PARTITION BY city_id, date_day
                ORDER BY weather_code DESC
            ) AS rn
        FROM hourly_local
    )
    WHERE rn = 1
)

SELECT
    a.*,
    d.dominant_weather_code
FROM daily_agg a
JOIN dominant_code d
    ON a.city_id = d.city_id AND a.date_day = d.date_day