/*
    dim_geography — Geography dimension combining departments and communes.

    Sources:
      - stg_geo__departments  (department-level boundaries from Etalab)
      - stg_geo__communes     (commune-level boundaries from Etalab)

    Purpose:
      Provide a single geography lookup table at two levels of granularity
      (department and commune). Used by the Looker Studio dashboard for
      map visualizations and geographic drill-downs.

      The two sources are unioned with a discriminator column (geo_level)
      so that dashboard filters can select the appropriate level.

      Centroid coordinates are computed from the geometry polygons to
      support point-based map markers when polygon rendering is not needed.

      For communes, the department code is derived from the INSEE code:
      - Metropolitan France: first 2 characters (e.g. '75056' -> '75')
      - Overseas departments (DOM): first 3 characters (e.g. '97105' -> '971')

    Materialization : TABLE in dvf_analytics dataset

    Grain : one row per geographic entity (department or commune)
    Primary key : geo_code

    Output columns:
      geo_code        -- STRING, PK (department code or commune INSEE code)
      geo_name        -- STRING, entity name
      geo_level       -- STRING, 'department' or 'commune'
      department_code -- STRING, parent department (= geo_code for departments)
      department_name -- STRING, department name (NULL for commune rows)
      geometry        -- GEOGRAPHY, polygon boundary
      centroid_lat    -- FLOAT64, latitude of polygon centroid
      centroid_lon    -- FLOAT64, longitude of polygon centroid
*/

{{ config(
    materialized='table',
    schema='dvf_analytics'
) }}

-- Department-level geography (one row per department)
WITH departments AS (

    SELECT
        department_code AS geo_code,
        department_name AS geo_name,
        'department' AS geo_level,
        department_code,
        department_name,
        geometry,
        -- Compute centroid for point-based map markers
        ST_Y(ST_CENTROID(geometry)) AS centroid_lat,
        ST_X(ST_CENTROID(geometry)) AS centroid_lon
    FROM {{ ref('stg_geo__departments') }}

),

-- Commune-level geography (one row per commune)
communes AS (

    SELECT
        gc.commune_code AS geo_code,
        gc.commune_name AS geo_name,
        'commune' AS geo_level,
        -- Derive department code from the INSEE commune code:
        -- DOM communes (starting with '97') use 3-char department codes,
        -- metropolitan communes use the first 2 characters
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

-- Stack both levels into a single table
unioned AS (

    SELECT * FROM departments
    UNION ALL
    SELECT * FROM communes

)

SELECT *
FROM unioned
