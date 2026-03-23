/*
    dim_communes — Dimension table for communes (municipalities).

    Sources:
      - stg_dvf__parcelles    (commune codes observed in transaction data)
      - stg_geo__communes     (official commune names from Etalab GeoJSON)

    Purpose:
      Build a deduplicated list of communes that appear in the transaction
      data, enriched with their official names from the geographic reference.

      Department assignment: a commune may appear with multiple department
      codes in edge cases (boundary changes, data quality). This model
      resolves the ambiguity by keeping the most frequently observed
      department code for each commune.

      Name resolution: the commune name comes from the GeoJSON reference;
      when no match is found, the commune code itself is used as a fallback.

    Materialization : TABLE in dvf_analytics dataset

    Grain : one row per commune
    Primary key : commune_code

    Output columns:
      commune_code    -- STRING, PK, INSEE code (e.g. '75056')
      commune_name    -- STRING, official name or code fallback
      department_code -- STRING, most frequent department for this commune
*/

{{ config(
    materialized='table',
    schema='dvf_analytics'
) }}

WITH all_commune_codes AS (

    -- Collect commune codes from both parcelle data AND mutation fallback
    -- to ensure full coverage (some mutations have no parcelle records)
    SELECT
        commune_code,
        department_code
    FROM {{ ref('stg_dvf__parcelles') }}
    WHERE commune_code IS NOT NULL

    UNION ALL

    SELECT DISTINCT
        insee_code AS commune_code,
        department_code
    FROM {{ ref('stg_dvf__mutations') }}
    WHERE insee_code IS NOT NULL

),

parcelle_communes AS (

    -- Deduplicate: for each commune_code, find the department_code that
    -- appears most often (handles edge cases where a commune is tagged
    -- with different departments across sources)
    SELECT
        commune_code,
        ARRAY_AGG(department_code ORDER BY dept_count DESC LIMIT 1)[OFFSET(0)] AS department_code
    FROM (
        SELECT
            commune_code,
            department_code,
            COUNT(*) AS dept_count
        FROM all_commune_codes
        GROUP BY commune_code, department_code
    )
    GROUP BY commune_code

),

geo_communes AS (

    SELECT
        commune_code,
        commune_name
    FROM {{ ref('stg_geo__communes') }}

),

final AS (

    SELECT
        pc.commune_code,
        -- Fall back to code when the GeoJSON reference has no matching commune
        COALESCE(gc.commune_name, pc.commune_code) AS commune_name,
        pc.department_code
    FROM parcelle_communes AS pc
    LEFT JOIN geo_communes AS gc
        ON pc.commune_code = gc.commune_code

)

SELECT *
FROM final
