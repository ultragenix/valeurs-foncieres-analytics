/*
    dim_dates — Date dimension table.

    Source : generated using dbt_utils.date_spine (no raw data source)

    Purpose:
      Provide a calendar lookup table covering the full DVF+ data range
      (2014-01-01 to 2025-12-31). Allows the dashboard to filter and
      group transactions by year, quarter, month, day of week, and
      weekend/weekday without repeating date logic in every query.

    The date_spine macro generates one row per day between the start
    and end dates. The final CTE extracts calendar attributes.

    Materialization : TABLE in dvf_analytics dataset

    Grain : one row per calendar day
    Primary key : date_key

    Output columns:
      date_key    -- DATE, PK (same value as full_date)
      full_date   -- DATE, the calendar date
      year        -- INT64, e.g. 2024
      quarter     -- INT64, 1-4
      month       -- INT64, 1-12
      month_name  -- STRING, full English month name (e.g. 'January')
      day_of_week -- INT64, BigQuery convention: 1=Sunday, 7=Saturday
      is_weekend  -- BOOL, TRUE for Saturday (7) and Sunday (1)
*/

{{ config(
    materialized='table',
    schema='dvf_analytics'
) }}

WITH date_spine AS (

    -- Generate one row per day from 2014-01-01 through 2025-12-31
    -- (end_date is exclusive, so '2026-01-01' gives up to '2025-12-31')
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
        -- BigQuery DAYOFWEEK: 1 = Sunday, 2 = Monday, ..., 7 = Saturday
        EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
        EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) AS is_weekend
    FROM date_spine

)

SELECT *
FROM final
