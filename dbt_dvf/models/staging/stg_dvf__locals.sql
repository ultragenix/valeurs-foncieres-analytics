/*
    stg_dvf__locals — Staging model for locals (premises / building units).

    Source : dvf_raw.local

    Purpose:
      - Rename raw French column names to descriptive English aliases
      - Cast every column to its correct BigQuery type
      - Parse transaction date from string to DATE

    Each local represents an individual building unit (house, apartment,
    commercial space, or outbuilding) involved in a disposition. A single
    mutation can include many locals.

    Grain : one row per local (building unit in a disposition)
    Primary key : local_id
    Foreign key : mutation_id -> stg_dvf__mutations.mutation_id

    Output columns:
      local_id              -- INT64, PK (from iddispoloc)
      disposition_parcel_id -- INT64, link to disposition_parcelle
      mutation_id           -- INT64, FK to stg_dvf__mutations
      local_identifier      -- STRING, stable cadastral local ID
      local_type_code       -- INT64, 1=house, 2=apartment, 3=outbuilding, 4=commercial
      local_type_label      -- STRING, human-readable type name
      main_room_count       -- INT64, number of main rooms (pieces principales)
      built_area_sqm        -- FLOAT64, floor area in square meters
      department_code       -- STRING
      transaction_date      -- DATE, parsed from string
      transaction_year      -- INT64
*/

WITH source AS (

    SELECT *
    FROM {{ source('dvf_raw', 'local') }}

),

cleaned AS (

    SELECT
        CAST(iddispoloc AS INT64) AS local_id,
        CAST(iddispopar AS INT64) AS disposition_parcel_id,
        CAST(idmutation AS INT64) AS mutation_id,
        CAST(idloc AS STRING) AS local_identifier,
        CAST(codtyploc AS INT64) AS local_type_code,
        CAST(libtyploc AS STRING) AS local_type_label,
        CAST(nbpprinc AS INT64) AS main_room_count,
        CAST(sbati AS FLOAT64) AS built_area_sqm,
        CAST(coddep AS STRING) AS department_code,
        SAFE.PARSE_DATE('%Y-%m-%d', CAST(datemut AS STRING)) AS transaction_date,
        CAST(anneemut AS INT64) AS transaction_year
    FROM source

)

SELECT *
FROM cleaned
