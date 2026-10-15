{{ config(
    materialized='incremental',
    unique_key=['city_id', 'observed_at_utc'],
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT 
        city_id,
        request_date,
        pulled_at,
        payload
    FROM {{ source('bronze', 'raw_weather_responses') }}
    
    
    {% if is_incremental() %}
    WHERE pulled_at > (SELECT MAX(pulled_at) FROM {{ this }})
    {% endif %}

    QUALIFY ROW_NUMBER() OVER (
    PARTITION BY city_id, request_date 
    ORDER BY pulled_at DESC
    ) = 1
),

-- خطوة التحسين الجديدة: استخراج المصفوفات مرة واحدة قبل الفك
extracted_arrays AS (
    SELECT
        city_id,
        request_date,
        pulled_at,
        JSON_QUERY_ARRAY(payload, '$.hourly.time') AS time_array,
        JSON_QUERY_ARRAY(payload, '$.hourly.temperature_2m') AS temp_array,
        JSON_QUERY_ARRAY(payload, '$.hourly.relative_humidity_2m') AS humidity_array,
        JSON_QUERY_ARRAY(payload, '$.hourly.precipitation') AS precip_array,
        JSON_QUERY_ARRAY(payload, '$.hourly.wind_speed_10m') AS wind_array,
        JSON_QUERY_ARRAY(payload, '$.hourly.weather_code') AS weather_code_array
    FROM source_data
),

unnested_data AS (
    SELECT
        city_id,
        request_date,
        pulled_at,
        
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M', JSON_VALUE(time_val), 'UTC') AS observed_at_utc,
        
        -- سحب القيم من المصفوفات المستخرجة مسبقاً وتحديث أسماء الأعمدة
        CAST(JSON_VALUE(temp_array[OFFSET(pos)]) AS FLOAT64) AS temperature_2m_c,
        CAST(JSON_VALUE(humidity_array[OFFSET(pos)]) AS FLOAT64) AS relative_humidity_2m_percent,
        CAST(JSON_VALUE(precip_array[OFFSET(pos)]) AS FLOAT64) AS precipitation_mm,
        CAST(JSON_VALUE(wind_array[OFFSET(pos)]) AS FLOAT64) AS wind_speed_10m_kmh,
        CAST(JSON_VALUE(weather_code_array[OFFSET(pos)]) AS INT64) AS weather_code

    FROM extracted_arrays,
    UNNEST(time_array) AS time_val WITH OFFSET AS pos
)

SELECT * FROM unnested_data