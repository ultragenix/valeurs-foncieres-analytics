/*
    stg_dvf__dispositions — Staging model for dispositions (sub-transactions).

    Source : dvf_raw.disposition

    Purpose:
      - Rename raw French column names to descriptive English aliases
      - Cast every column to its correct BigQuery type

    A mutation can contain multiple dispositions. Each disposition
    represents one line item in the notarial deed (e.g., a sale may
    include the building and an adjacent parcel as separate dispositions).

    Grain : one row per disposition
    Primary key : disposition_id
    Foreign key : mutation_id -> stg_dvf__mutations.mutation_id

    Output columns:
      disposition_id        -- INT64, PK (from iddispo)
      mutation_id           -- INT64, FK to stg_dvf__mutations
      disposition_number    -- INT64, ordinal position within the mutation
      disposition_price_eur -- FLOAT64, price attributed to this disposition
      lot_count             -- INT64, number of lots in this disposition
      department_code       -- STRING
*/

WITH source AS (

    SELECT *
    FROM {{ source('dvf_raw', 'disposition') }}

),

cleaned AS (

    SELECT
        CAST(iddispo AS INT64) AS disposition_id,
        CAST(idmutation AS INT64) AS mutation_id,
        CAST(nodispo AS INT64) AS disposition_number,
        CAST(valeurfonc AS FLOAT64) AS disposition_price_eur,
        CAST(nblot AS INT64) AS lot_count,
        CAST(coddep AS STRING) AS department_code
    FROM source

)

SELECT *
FROM cleaned
