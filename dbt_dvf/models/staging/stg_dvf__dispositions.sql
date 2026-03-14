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
