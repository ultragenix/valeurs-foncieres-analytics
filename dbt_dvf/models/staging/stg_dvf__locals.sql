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
