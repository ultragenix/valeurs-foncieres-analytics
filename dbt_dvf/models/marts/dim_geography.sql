{{ config(
    materialized='table',
    schema='dvf_analytics'
) }}

WITH departments AS (

    SELECT
        department_code AS geo_code,
        department_name AS geo_name,
        'department' AS geo_level,
        department_code,
        department_name,
        geometry,
        ST_Y(ST_CENTROID(geometry)) AS centroid_lat,
        ST_X(ST_CENTROID(geometry)) AS centroid_lon
    FROM {{ ref('stg_geo__departments') }}

),

communes AS (

    SELECT
        gc.commune_code AS geo_code,
        gc.commune_name AS geo_name,
        'commune' AS geo_level,
        -- Extract department code from the first 2 or 3 chars of commune code
        -- DOM communes have 3-char department codes (e.g., 97105 -> 971)
        CASE
            WHEN gc.commune_code LIKE '97%' THEN SUBSTR(gc.commune_code, 1, 3)
            ELSE SUBSTR(gc.commune_code, 1, 2)
        END AS department_code,
        CAST(NULL AS STRING) AS department_name,
        gc.geometry,
        ST_Y(ST_CENTROID(gc.geometry)) AS centroid_lat,
        ST_X(ST_CENTROID(gc.geometry)) AS centroid_lon
    FROM {{ ref('stg_geo__communes') }} AS gc

),

unioned AS (

    SELECT * FROM departments
    UNION ALL
    SELECT * FROM communes

)

SELECT *
FROM unioned
