-- Sources from disposition_parcelle (richer than parcelle table)
-- Contains commune code, dates, and land surface breakdowns.

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
        CAST(codcomm AS STRING) AS commune_code,
        CAST(prefsect AS STRING) AS section_prefix,
        CAST(nosect AS STRING) AS section_number,
        CAST(noplan AS STRING) AS plan_number,
        SAFE.PARSE_DATE('%Y-%m-%d', CAST(datemut AS STRING)) AS transaction_date,
        CAST(anneemut AS INT64) AS transaction_year,
        CASE UPPER(CAST(parcvendue AS STRING))
            WHEN 'T' THEN TRUE
            WHEN 'TRUE' THEN TRUE
            WHEN '1' THEN TRUE
            ELSE FALSE
        END AS is_parcel_sold,
        CAST(dcntsol AS FLOAT64) AS built_land_area_sqm,
        CAST(dcntagri AS FLOAT64) AS agricultural_land_area_sqm,
        CAST(dcntnat AS FLOAT64) AS natural_land_area_sqm
    FROM source

)

SELECT *
FROM cleaned
