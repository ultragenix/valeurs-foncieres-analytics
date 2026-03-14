{{ config(
    materialized='table',
    schema='dvf_analytics'
) }}

WITH parcelle_communes AS (

    -- Extract distinct commune codes from parcelle data
    SELECT DISTINCT
        commune_code,
        department_code
    FROM {{ ref('stg_dvf__parcelles') }}
    WHERE commune_code IS NOT NULL

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
