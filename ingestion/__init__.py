"""DVF+ data ingestion package.

Provides a complete pipeline for downloading, restoring, exporting, and
loading French real-estate transaction data (DVF+ open-data from Cerema)
into Google BigQuery via an ephemeral PostgreSQL intermediate stage.

Modules:
    config           -- Shared constants, environment variables, and helpers.
    http_utils       -- Reusable HTTP streaming download utilities.
    download_dvf     -- Download and extract DVF+ SQL dump archives.
    restore_dump     -- Restore SQL dumps into ephemeral PostgreSQL.
    export_tables    -- Export PostgreSQL tables to CSV with geometry/array handling.
    upload_to_gcs    -- Upload CSV/GeoJSON files to Google Cloud Storage.
    load_to_bigquery -- Load GCS data into BigQuery raw tables.
    download_geojson -- Download administrative boundary GeoJSON from Etalab.
    chunked_ingest   -- Chunked full-France ingestion with crash-safe resume.
"""
