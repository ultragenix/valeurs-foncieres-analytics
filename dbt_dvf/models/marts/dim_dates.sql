{{ config(
    materialized='table',
    schema='dvf_analytics'
) }}

WITH date_spine AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2014-01-01' as date)",
        end_date="cast('2026-01-01' as date)"
    ) }}

),

final AS (

    SELECT
        CAST(date_day AS DATE) AS date_key,
        CAST(date_day AS DATE) AS full_date,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        EXTRACT(MONTH FROM date_day) AS month,
        FORMAT_DATE('%B', date_day) AS month_name,
        EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
        EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) AS is_weekend
    FROM date_spine

)

SELECT *
FROM final
