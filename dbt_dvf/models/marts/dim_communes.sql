{{ config(
    materialized='table',
    schema='dvf_analytics'
) }}

WITH parcelle_communes AS (

    -- Deduplicate by commune_code, keeping the most frequent department
    SELECT
        commune_code,
        ARRAY_AGG(department_code ORDER BY dept_count DESC LIMIT 1)[OFFSET(0)] AS department_code
    FROM (
        SELECT
            commune_code,
            department_code,
            COUNT(*) AS dept_count
        FROM {{ ref('stg_dvf__parcelles') }}
        WHERE commune_code IS NOT NULL
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
        COALESCE(gc.commune_name, pc.commune_code) AS commune_name,
        pc.department_code
    FROM parcelle_communes AS pc
    LEFT JOIN geo_communes AS gc
        ON pc.commune_code = gc.commune_code

)

SELECT *
FROM final
