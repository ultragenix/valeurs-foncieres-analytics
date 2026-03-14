WITH source AS (

    SELECT *
    FROM {{ source('dvf_raw', 'mutation') }}

),

cleaned AS (

    SELECT
        CAST(idmutation AS INT64) AS mutation_id,
        CAST(idopendata AS STRING) AS mutation_opendata_id,
        SAFE.PARSE_DATE('%Y-%m-%d', CAST(datemut AS STRING)) AS transaction_date,
        CAST(anneemut AS INT64) AS transaction_year,
        CAST(moismut AS INT64) AS transaction_month,
        CAST(idnatmut AS INT64) AS mutation_nature_id,
        CAST(libnatmut AS STRING) AS mutation_nature_label,
        CAST(vefa AS BOOL) AS is_vefa,
        CAST(coddep AS STRING) AS department_code,
        CAST(codcomm AS STRING) AS commune_code_main,
        CAST(codinsee AS STRING) AS insee_code,
        CAST(valeurfonc AS FLOAT64) AS transaction_price_eur,
        CAST(nbdispo AS INT64) AS disposition_count,
        CAST(nblot AS INT64) AS lot_count,
        CAST(nbcomm AS INT64) AS commune_count,
        CAST(nbpar AS INT64) AS parcel_count,
        CAST(nbparmut AS INT64) AS sold_parcel_count,
        CAST(codtypbien AS STRING) AS property_type_code,
        CAST(libtypbien AS STRING) AS property_type_label,
        CAST(sbati AS FLOAT64) AS total_built_area_sqm,
        CAST(sbatmai AS FLOAT64) AS house_built_area_sqm,
        CAST(sbatapt AS FLOAT64) AS apartment_built_area_sqm,
        CAST(sbatact AS FLOAT64) AS commercial_built_area_sqm,
        CAST(sterr AS FLOAT64) AS land_area_sqm,
        CAST(nblocmut AS INT64) AS premises_count,
        CAST(nblocmai AS INT64) AS house_count,
        CAST(nblocapt AS INT64) AS apartment_count,
        CAST(nblocact AS INT64) AS commercial_count,
        CAST(nblocdep AS INT64) AS outbuilding_count,
        CAST(nbsuf AS INT64) AS fiscal_subdivision_count,
        CAST(latitude AS FLOAT64) AS latitude,
        CAST(longitude AS FLOAT64) AS longitude
    FROM source

)

SELECT *
FROM cleaned
WHERE transaction_price_eur > 0
