/*
    dim_property_types — Dimension table for property types.

    Source : stg_dvf__mutations (distinct codtypbien + libtypbien pairs)

    Purpose:
      Build a reference table of property type codes used in the GnDVF
      classification system. The codes are hierarchical strings (e.g. '111'
      for apartments, '121' for houses). This model also derives a
      coarser level-1 grouping from the first character of the code:
        '1' = Built property (bati)
        '2' = Unbuilt land (non bati)

    Materialization : TABLE in dvf_analytics dataset

    Grain : one row per unique property type code
    Primary key : property_type_code

    Output columns:
      property_type_code         -- STRING, PK, GnDVF hierarchical code
      property_type_label        -- STRING, human-readable label
      property_type_level1       -- STRING, first character of code ('1' or '2')
      property_type_level1_label -- STRING, 'Built property' or 'Unbuilt land'
*/

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
        -- Level 1 grouping: first character of the hierarchical code
        -- '1' = built property (bati), '2' = unbuilt land (non bati)
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
