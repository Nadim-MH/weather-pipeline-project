-- models/gold/vw_dashboard_daily.sql
{{ config(materialized='view') }}

SELECT
    f.city_id,
    c.city_name,
    c.region,
    f.date_day,
    d.year,
    d.month,
    d.day_of_week,
    d.season,
    d.season_year,
    d.is_complete_season,
    f.temp_min_c,
    f.temp_max_c,
    f.temp_avg_c,
    (f.temp_max_c - f.temp_min_c) AS temp_range_c,
    f.humidity_avg_pct,
    f.precipitation_sum_mm,
    f.is_rainy_day,
    f.hours_observed,
    c.latitude,
    c.longitude,
    wc.description AS dominant_weather_description,
    wc.weather_group AS dominant_weather_group

FROM {{ ref('fct_daily_weather') }} f
JOIN {{ ref('dim_city') }} c ON f.city_id = c.city_id
JOIN {{ ref('dim_date') }} d ON f.date_day = d.date_day
LEFT JOIN {{ ref('dim_weather_code') }} wc ON f.dominant_weather_code = wc.weather_code