{{ config(materialized='table') }}

WITH spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2021-01-01' as date)",
        end_date="cast(current_date() as date)"
    ) }}
),

enriched AS (
    SELECT
        date_day,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(MONTH FROM date_day) AS month,
        FORMAT_DATE('%A', date_day) AS day_of_week,
        CASE
            WHEN EXTRACT(MONTH FROM date_day) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM date_day) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM date_day) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Autumn'
        END AS season,
        CASE
            WHEN EXTRACT(MONTH FROM date_day) = 12 THEN EXTRACT(YEAR FROM date_day) + 1
            ELSE EXTRACT(YEAR FROM date_day)
        END AS season_year,
        -- الفصل مكتمل لو انتهى فعليًا قبل اليوم الحالي بفترة كافية
        date_day < DATE_TRUNC(CURRENT_DATE(), QUARTER) AS is_complete_season
    FROM spine
)

SELECT * FROM enriched