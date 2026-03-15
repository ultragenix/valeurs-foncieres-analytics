/*
    stg_geo__communes — Staging model for commune (municipality) boundaries.

    Source : dvf_raw.geo_communes (loaded from Etalab GeoJSON)

    Purpose:
      - Rename French column names (code, nom) to English aliases
      - Convert the raw GeoJSON geometry string to a native BigQuery
        GEOGRAPHY type using SAFE.ST_GEOGFROMGEOJSON
      - Filter out rows with NULL commune_code

    Grain : one row per French commune
    Primary key : commune_code

    Output columns:
      commune_code -- STRING, PK, 5-character INSEE code (e.g. '75056')
      commune_name -- STRING, official commune name
      geometry     -- GEOGRAPHY, polygon boundary for map visualizations
*/

WITH source AS (

    SELECT *
    FROM {{ source('dvf_raw', 'geo_communes') }}

),

cleaned AS (

    SELECT
        CAST(code AS STRING) AS commune_code,
        CAST(nom AS STRING) AS commune_name,
        -- Convert raw GeoJSON string to BigQuery GEOGRAPHY; SAFE prefix
        -- returns NULL for invalid geometries rather than raising an error
        SAFE.ST_GEOGFROMGEOJSON(geometry) AS geometry
    FROM source

)

SELECT *
FROM cleaned
WHERE commune_code IS NOT NULL
