/*
    fct_transactions — Fact table for real estate transactions.

    Source : int_transactions__enriched (pre-aggregated mutation grain)

    Purpose:
      Final analytics-ready fact table. Selects the columns needed by the
      Looker Studio dashboard and computes the derived price_per_sqm metric.

    Materialization:
      - TABLE in the dvf_analytics dataset
      - Partitioned by transaction_year (integer range 2014-2025, 1-year buckets)
        for efficient year-based filtering
      - Clustered by department_code and property_type_code to accelerate
        the most common dashboard filter patterns

    Grain : one row per transaction (mutation)
    Primary key : transaction_id (business key), mutation_id (technical key)
    Foreign keys:
      department_code    -> dim_geography.geo_code
      commune_code       -> dim_communes.commune_code
      property_type_code -> dim_property_types.property_type_code

    Output columns:
      transaction_id         -- STRING, open-data business key
      mutation_id            -- INT64, technical key
      transaction_date       -- DATE
      transaction_year       -- INT64 (partition key)
      transaction_month      -- INT64 (1-12)
      mutation_nature_id     -- INT64
      mutation_nature_label  -- STRING (e.g. 'Vente')
      is_vefa                -- BOOL, sale-before-completion flag
      department_code        -- STRING (cluster key 1)
      commune_code           -- STRING, INSEE code
      property_type_code     -- STRING (cluster key 2)
      property_type_label    -- STRING
      transaction_price_eur  -- FLOAT64, always > 0
      built_area_sqm         -- FLOAT64
      land_area_sqm          -- FLOAT64
      price_per_sqm          -- FLOAT64, computed: price / built area (NULL if no area)
      premises_count         -- INT64
      house_count            -- INT64
      apartment_count        -- INT64
      commercial_count       -- INT64
      outbuilding_count      -- INT64
      room_count             -- INT64
      latitude               -- FLOAT64
      longitude              -- FLOAT64
*/

{{ config(
    materialized='table',
    schema='dvf_analytics',
    partition_by={
        'field': 'transaction_year',
        'data_type': 'int64',
        'range': {
            'start': 2014,
            'end': 2026,
            'interval': 1
        }
    },
    cluster_by=['department_code', 'property_type_code']
) }}

WITH enriched AS (

    SELECT *
    FROM {{ ref('int_transactions__enriched') }}

),

final AS (

    SELECT
        mutation_opendata_id AS transaction_id,
        mutation_id,
        transaction_date,
        transaction_year,
        transaction_month,
        mutation_nature_id,
        mutation_nature_label,
        is_vefa,
        department_code,
        commune_code,
        property_type_code,
        property_type_label,
        transaction_price_eur,
        total_built_area_sqm AS built_area_sqm,
        land_area_sqm,
        -- Derived metric: price per square meter of built area.
        -- SAFE_DIVIDE returns NULL when the denominator is 0 or NULL,
        -- and NULLIF guards against division by zero for edge cases.
        SAFE_DIVIDE(
            transaction_price_eur,
            NULLIF(total_built_area_sqm, 0)
        ) AS price_per_sqm,
        premises_count,
        house_count,
        apartment_count,
        commercial_count,
        outbuilding_count,
        room_count,
        latitude,
        longitude
    FROM enriched

)

SELECT *
FROM final
