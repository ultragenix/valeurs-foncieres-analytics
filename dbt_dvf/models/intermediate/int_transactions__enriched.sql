/*
    int_transactions__enriched — Intermediate model: enriched transactions.

    Sources:
      - stg_dvf__mutations        (base grain: one row per mutation)
      - stg_dvf__dispositions     (aggregated per mutation)
      - stg_dvf__locals           (aggregated per mutation)
      - stg_dvf__parcelles        (aggregated per mutation)

    Purpose:
      Flatten the DVF+ star schema (mutation -> disposition -> local / parcelle)
      into a single wide row per mutation. The 1:N child tables are pre-aggregated
      into summary metrics (totals, maxima, counts) and then LEFT JOINed back
      to the mutation grain.

      Commune code resolution: prefer the commune_code from parcelle data
      (more reliable because it comes from the cadastre), with a fallback to
      the mutation's own insee_code.

    Grain : one row per mutation
    Primary key : mutation_id

    Output columns:
      -- From mutations
      mutation_id, mutation_opendata_id, transaction_date, transaction_year,
      transaction_month, mutation_nature_id, mutation_nature_label, is_vefa,
      department_code, commune_code, insee_code, transaction_price_eur,
      property_type_code, property_type_label, total_built_area_sqm,
      house_built_area_sqm, apartment_built_area_sqm, commercial_built_area_sqm,
      land_area_sqm, premises_count, house_count, apartment_count,
      commercial_count, outbuilding_count, latitude, longitude

      -- From disposition aggregation
      total_disposition_price_eur

      -- From local aggregation
      room_count (max across locals), total_room_count

      -- From parcelle aggregation
      total_built_land_area_sqm, total_agricultural_land_area_sqm,
      total_natural_land_area_sqm
*/

WITH mutations AS (

    SELECT *
    FROM {{ ref('stg_dvf__mutations') }}

),

-- Aggregate disposition-level data to the mutation grain:
-- total disposition price and count of dispositions per mutation
disposition_agg AS (

    SELECT
        mutation_id,
        COUNT(*) AS disposition_count_actual,
        SUM(disposition_price_eur) AS total_disposition_price_eur
    FROM {{ ref('stg_dvf__dispositions') }}
    GROUP BY mutation_id

),

-- Aggregate local-level data to the mutation grain:
-- room counts (max and total) and largest local built area
local_agg AS (

    SELECT
        mutation_id,
        MAX(main_room_count) AS max_room_count,
        SUM(main_room_count) AS total_room_count,
        MAX(built_area_sqm) AS max_local_built_area_sqm
    FROM {{ ref('stg_dvf__locals') }}
    GROUP BY mutation_id

),

-- Aggregate parcelle-level data to the mutation grain:
-- commune code (first by ID order), and land-use surface totals
parcelle_agg AS (

    SELECT
        mutation_id,
        -- Take the first commune code (most mutations involve one commune).
        -- Ordered by disposition_parcel_id for deterministic results.
        ARRAY_AGG(commune_code ORDER BY disposition_parcel_id LIMIT 1)[OFFSET(0)] AS commune_code,
        COUNT(DISTINCT commune_code) AS commune_count_parcelle,
        SUM(built_land_area_sqm) AS total_built_land_area_sqm,
        SUM(agricultural_land_area_sqm) AS total_agricultural_land_area_sqm,
        SUM(natural_land_area_sqm) AS total_natural_land_area_sqm
    FROM {{ ref('stg_dvf__parcelles') }}
    GROUP BY mutation_id

),

-- Join all aggregations back to the mutation grain
enriched AS (

    SELECT
        m.mutation_id,
        m.mutation_opendata_id,
        m.transaction_date,
        m.transaction_year,
        m.transaction_month,
        m.mutation_nature_id,
        m.mutation_nature_label,
        m.is_vefa,
        m.department_code,
        -- Prefer parcelle commune_code (cadastral source); fall back to mutation INSEE code
        COALESCE(p.commune_code, m.insee_code) AS commune_code,
        m.insee_code,
        m.transaction_price_eur,
        m.property_type_code,
        m.property_type_label,
        m.total_built_area_sqm,
        m.house_built_area_sqm,
        m.apartment_built_area_sqm,
        m.commercial_built_area_sqm,
        m.land_area_sqm,
        m.premises_count,
        m.house_count,
        m.apartment_count,
        m.commercial_count,
        m.outbuilding_count,
        -- Use max room count as the representative value; default to 0 when no locals exist
        COALESCE(la.max_room_count, 0) AS room_count,
        la.total_room_count,
        m.latitude,
        m.longitude,
        da.total_disposition_price_eur,
        p.total_built_land_area_sqm,
        p.total_agricultural_land_area_sqm,
        p.total_natural_land_area_sqm
    FROM mutations AS m
    LEFT JOIN disposition_agg AS da
        ON m.mutation_id = da.mutation_id
    LEFT JOIN local_agg AS la
        ON m.mutation_id = la.mutation_id
    LEFT JOIN parcelle_agg AS p
        ON m.mutation_id = p.mutation_id

)

SELECT *
FROM enriched
