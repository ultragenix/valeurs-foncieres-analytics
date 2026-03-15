/*
    stg_geo__departments — Staging model for department boundaries.

    Source : dvf_raw.geo_departments (loaded from Etalab GeoJSON)

    Purpose:
      - Rename French column names (code, nom) to English aliases
      - Convert the raw GeoJSON geometry string to a native BigQuery
        GEOGRAPHY type using SAFE.ST_GEOGFROMGEOJSON (tolerates malformed
        geometries by returning NULL instead of failing)
      - Filter out rows with NULL department_code

    Grain : one row per French department
    Primary key : department_code

    Output columns:
      department_code -- STRING, PK (e.g. '75', '13', '2A', '971')
      department_name -- STRING, official department name
      geometry        -- GEOGRAPHY, polygon boundary for map visualizations
*/

WITH source AS (

    SELECT *
    FROM {{ source('dvf_raw', 'geo_departments') }}

),

cleaned AS (

    SELECT
        CAST(code AS STRING) AS department_code,
        CAST(nom AS STRING) AS department_name,
        -- Convert raw GeoJSON string to BigQuery GEOGRAPHY; SAFE prefix
        -- returns NULL for invalid geometries rather than raising an error
        SAFE.ST_GEOGFROMGEOJSON(geometry) AS geometry
    FROM source

)

SELECT *
FROM cleaned
WHERE department_code IS NOT NULL
