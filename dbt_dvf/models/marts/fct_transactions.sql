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
