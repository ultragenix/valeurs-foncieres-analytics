-- Enriched transactions at one row per mutation.
-- Aggregates 1:N relationships from dispositions, locals, and parcelles.

WITH mutations AS (

    SELECT *
    FROM {{ ref('stg_dvf__mutations') }}

),

disposition_agg AS (

    SELECT
        mutation_id,
        COUNT(*) AS disposition_count_actual,
        SUM(disposition_price_eur) AS total_disposition_price_eur
    FROM {{ ref('stg_dvf__dispositions') }}
    GROUP BY mutation_id

),

local_agg AS (

    SELECT
        mutation_id,
        MAX(main_room_count) AS max_room_count,
        SUM(main_room_count) AS total_room_count,
        MAX(built_area_sqm) AS max_local_built_area_sqm
    FROM {{ ref('stg_dvf__locals') }}
    GROUP BY mutation_id

),

parcelle_agg AS (

    SELECT
        mutation_id,
        -- Take the first commune code (most mutations involve one commune)
        ARRAY_AGG(commune_code ORDER BY disposition_parcel_id LIMIT 1)[OFFSET(0)] AS commune_code,
        COUNT(DISTINCT commune_code) AS commune_count_parcelle,
        SUM(built_land_area_sqm) AS total_built_land_area_sqm,
        SUM(agricultural_land_area_sqm) AS total_agricultural_land_area_sqm,
        SUM(natural_land_area_sqm) AS total_natural_land_area_sqm
    FROM {{ ref('stg_dvf__parcelles') }}
    GROUP BY mutation_id

),

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
