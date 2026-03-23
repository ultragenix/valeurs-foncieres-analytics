/*
    stg_dvf__parcelles — Staging model for parcel-disposition links.

    Source : dvf_raw.disposition_parcelle
             (chosen over dvf_raw.parcelle because it is richer: includes
              commune code, transaction dates, and land surface breakdowns)

    Purpose:
      - Rename raw French column names to descriptive English aliases
      - Cast every column to its correct BigQuery type
      - Parse transaction date and normalize the parcel-sold boolean flag

    Each row represents a parcel involved in one disposition. The land
    surface columns (dcntsol, dcntagri, dcntnat) come from the cadastre
    and describe the parcel's land-use breakdown.

    Grain : one row per parcel-disposition link
    Primary key : disposition_parcel_id
    Foreign keys:
      mutation_id    -> stg_dvf__mutations.mutation_id
      disposition_id -> stg_dvf__dispositions.disposition_id

    Output columns:
      disposition_parcel_id      -- INT64, PK (from iddispopar)
      disposition_id             -- INT64, FK to stg_dvf__dispositions
      parcel_id                  -- INT64, FK to the parcelle reference table
      mutation_id                -- INT64, FK to stg_dvf__mutations
      parcel_identifier          -- STRING, cadastral parcel ID (e.g. '750011234')
      department_code            -- STRING
      commune_code               -- STRING, INSEE code (critical for geo joins)
      section_prefix             -- STRING, cadastral section prefix
      section_number             -- STRING, cadastral section number
      plan_number                -- STRING, cadastral plan number
      transaction_date           -- DATE, parsed from string
      transaction_year           -- INT64
      is_parcel_sold             -- BOOL, whether this specific parcel was sold
      built_land_area_sqm        -- FLOAT64, built-up land surface (from cadastre)
      agricultural_land_area_sqm -- FLOAT64, agricultural land surface
      natural_land_area_sqm      -- FLOAT64, natural/uncultivated land surface
*/

WITH source AS (

    SELECT *
    FROM {{ source('dvf_raw', 'disposition_parcelle') }}

),

cleaned AS (

    SELECT
        CAST(iddispopar AS INT64) AS disposition_parcel_id,
        CAST(iddispo AS INT64) AS disposition_id,
        CAST(idparcelle AS INT64) AS parcel_id,
        CAST(idmutation AS INT64) AS mutation_id,
        CAST(idpar AS STRING) AS parcel_identifier,
        CAST(coddep AS STRING) AS department_code,
        -- codcomm is the 3-char commune suffix; concat with department code
        -- to reconstruct the full 5-char INSEE commune code
        CONCAT(CAST(coddep AS STRING), CAST(codcomm AS STRING)) AS commune_code,
        CAST(prefsect AS STRING) AS section_prefix,
        CAST(nosect AS STRING) AS section_number,
        CAST(noplan AS STRING) AS plan_number,
        SAFE.PARSE_DATE('%Y-%m-%d', CAST(datemut AS STRING)) AS transaction_date,
        CAST(anneemut AS INT64) AS transaction_year,
        -- Parcel-sold flag arrives in heterogeneous text form, same as VEFA
        CASE UPPER(CAST(parcvendue AS STRING))
            WHEN 'T' THEN TRUE
            WHEN 'TRUE' THEN TRUE
            WHEN '1' THEN TRUE
            ELSE FALSE
        END AS is_parcel_sold,
        -- Land-use breakdown from cadastral records (in square meters)
        CAST(dcntsol AS FLOAT64) AS built_land_area_sqm,
        CAST(dcntagri AS FLOAT64) AS agricultural_land_area_sqm,
        CAST(dcntnat AS FLOAT64) AS natural_land_area_sqm
    FROM source

)

SELECT *
FROM cleaned
