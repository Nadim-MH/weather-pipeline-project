{{ config(
    materialized='incremental',
    unique_key=['city_id', 'observed_at_utc'],
    incremental_strategy='merge'
) }}

WITH validated_data AS (
    SELECT 
        *,
        -- تقييم القواعد الفيزيائية وتحديد سبب العزل
        CASE 
            WHEN relative_humidity_2m_percent < 0 OR relative_humidity_2m_percent > 100 THEN 'Invalid Humidity Range'
            WHEN precipitation_mm < 0 THEN 'Negative Precipitation'
            WHEN wind_speed_10m_kmh < 0 THEN 'Negative Wind Speed'
            WHEN temperature_2m_c < -15 OR temperature_2m_c > 60 THEN 'Unrealistic Temperature for SA'
            ELSE NULL
        END AS quarantine_reason

    FROM {{ ref('stg_weather_hourly') }}
    
    {% if is_incremental() %}
    WHERE pulled_at > (SELECT MAX(pulled_at) FROM {{ this }})
    {% endif %}
)

-- فلترة السجلات المرفوضة فقط وإضافة طابع زمني لوقت العزل
SELECT 
    city_id,
    request_date,
    pulled_at,
    observed_at_utc,
    temperature_2m_c,
    relative_humidity_2m_percent,
    precipitation_mm,
    wind_speed_10m_kmh,
    weather_code,
    quarantine_reason,
    CURRENT_TIMESTAMP() AS quarantined_at
FROM validated_data
WHERE quarantine_reason IS NOT NULL