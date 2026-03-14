WITH source AS (

    SELECT *
    FROM {{ source('dvf_raw', 'geo_departments') }}

),

cleaned AS (

    SELECT
        CAST(code AS STRING) AS department_code,
        CAST(nom AS STRING) AS department_name,
        SAFE.ST_GEOGFROMGEOJSON(geometry) AS geometry
    FROM source

)

SELECT *
FROM cleaned
WHERE department_code IS NOT NULL
