{{ config(
    materialized='table',
    schema='dvf_analytics'
) }}

WITH property_types AS (

    -- Extract distinct property type codes and labels from mutations
    SELECT DISTINCT
        property_type_code,
        property_type_label
    FROM {{ ref('stg_dvf__mutations') }}
    WHERE property_type_code IS NOT NULL

),

final AS (

    SELECT
        property_type_code,
        property_type_label,
        -- Level 1: first character (1=built property, 2=unbuilt land)
        SUBSTR(property_type_code, 1, 1) AS property_type_level1,
        CASE SUBSTR(property_type_code, 1, 1)
            WHEN '1' THEN 'Built property'
            WHEN '2' THEN 'Unbuilt land'
            ELSE 'Other'
        END AS property_type_level1_label
    FROM property_types

)

SELECT *
FROM final
