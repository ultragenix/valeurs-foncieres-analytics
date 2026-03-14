WITH source AS (

    SELECT *
    FROM {{ source('dvf_raw', 'geo_communes') }}

),

cleaned AS (

    SELECT
        CAST(code AS STRING) AS commune_code,
        CAST(nom AS STRING) AS commune_name,
        SAFE.ST_GEOGFROMGEOJSON(geometry) AS geometry
    FROM source

)

SELECT *
FROM cleaned
WHERE commune_code IS NOT NULL
