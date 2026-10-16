{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date_day', 'data_type': 'date'},
    cluster_by=['city_id']
) }}

SELECT
    c.city_id,
    DATE(c.observed_at_utc) AS date_day,
    c.observed_at_utc,
    c.weather_code,
    c.temperature_2m_c,
    c.relative_humidity_2m_percent,
    c.precipitation_mm,
    c.wind_speed_10m_kmh
FROM {{ ref('int_weather_hourly_clean') }} c

{% if is_incremental() %}
WHERE DATE(c.observed_at_utc) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
{% endif %}
