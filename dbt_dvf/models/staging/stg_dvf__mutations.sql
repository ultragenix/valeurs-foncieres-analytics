/*
    stg_dvf__mutations — Staging model for real estate mutations (sale deeds).

    Source : dvf_raw.mutation (loaded from Cerema DVF+ SQL dump)

    Purpose:
      - Rename raw French column names to descriptive English aliases
      - Cast every column to its correct BigQuery type (raw table is all STRING)
      - Parse the VEFA flag from its heterogeneous text representation to BOOLEAN
      - Filter out non-market transactions (price = 0 or NULL)

    Grain : one row per mutation (sale deed)
    Primary key : mutation_id

    Output columns:
      mutation_id             -- INT64, PK (from idmutation)
      mutation_opendata_id    -- STRING, open-data business key
      transaction_date        -- DATE, parsed from string datemut
      transaction_year        -- INT64
      transaction_month       -- INT64
      mutation_nature_id      -- INT64, FK to ann_nature_mutation
      mutation_nature_label   -- STRING
      is_vefa                 -- BOOL, sale-before-completion flag
      department_code         -- STRING (e.g. '75', '2A')
      insee_code              -- STRING, commune INSEE code
      transaction_price_eur   -- FLOAT64, always > 0 after filter
      disposition_count       -- INT64
      lot_count               -- INT64
      commune_count           -- INT64
      parcel_count            -- INT64
      sold_parcel_count       -- INT64
      property_type_code      -- STRING, GnDVF hierarchical code
      property_type_label     -- STRING
      total_built_area_sqm    -- FLOAT64
      house_built_area_sqm    -- FLOAT64
      apartment_built_area_sqm -- FLOAT64
      commercial_built_area_sqm -- FLOAT64
      land_area_sqm           -- FLOAT64
      premises_count          -- INT64
      house_count             -- INT64
      apartment_count         -- INT64
      commercial_count        -- INT64
      outbuilding_count       -- INT64
      fiscal_subdivision_count -- INT64
      latitude                -- FLOAT64, from PostGIS centroid
      longitude               -- FLOAT64, from PostGIS centroid
*/

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
        -- VEFA flag arrives as 'T', 'TRUE', '1', or other values depending on
        -- the source export. Normalize all truthy variants to TRUE.
        CASE UPPER(CAST(vefa AS STRING))
            WHEN 'T' THEN TRUE
            WHEN 'TRUE' THEN TRUE
            WHEN '1' THEN TRUE
            ELSE FALSE
        END AS is_vefa,
        CAST(coddep AS STRING) AS department_code,
        CAST(codinsee AS STRING) AS insee_code,
        CAST(valeurfonc AS FLOAT64) AS transaction_price_eur,
        CAST(nbdispo AS INT64) AS disposition_count,
        CAST(nblot AS INT64) AS lot_count,
        CAST(nbcomm AS INT64) AS commune_count,
        CAST(nbpar AS INT64) AS parcel_count,
        CAST(nbparmut AS INT64) AS sold_parcel_count,
        CAST(codtypbien AS STRING) AS property_type_code,
        CAST(libtypbien AS STRING) AS property_type_label,
        -- Built-area breakdowns by property type
        CAST(sbati AS FLOAT64) AS total_built_area_sqm,
        CAST(sbatmai AS FLOAT64) AS house_built_area_sqm,
        CAST(sbatapt AS FLOAT64) AS apartment_built_area_sqm,
        CAST(sbatact AS FLOAT64) AS commercial_built_area_sqm,
        CAST(sterr AS FLOAT64) AS land_area_sqm,
        -- Premises-count breakdowns by property type
        CAST(nblocmut AS INT64) AS premises_count,
        CAST(nblocmai AS INT64) AS house_count,
        CAST(nblocapt AS INT64) AS apartment_count,
        CAST(nblocact AS INT64) AS commercial_count,
        CAST(nblocdep AS INT64) AS outbuilding_count,
        CAST(nbsuf AS INT64) AS fiscal_subdivision_count,
        -- Coordinates extracted from PostGIS geometry during ingestion
        CAST(latitude AS FLOAT64) AS latitude,
        CAST(longitude AS FLOAT64) AS longitude
    FROM source

)

SELECT *
FROM cleaned
-- Exclude non-market transactions (donations, inheritances, etc.)
WHERE transaction_price_eur > 0
