# Architecture

Technical architecture for the DVF+ France real estate analytics pipeline.

## System Overview

The pipeline ingests France's DVF+ real estate transaction dataset (PostgreSQL dump, 17 tables, ~20M transactions) into a cloud-native analytics stack on GCP. Data flows from Cerema's open data portal through an ephemeral PostgreSQL container, into GCS as a data lake, into BigQuery raw tables, through dbt transformations into a Kimball star schema, and finally into a Looker Studio dashboard.

```
+-------------------+     +-----------------------------+     +-------------------+
|  External Sources |     |  Local Machine (Docker)     |     |  GCP europe-west9 |
|                   |     |                             |     |                   |
|  Cerema Box       |---->|  PostgreSQL 16 + PostGIS    |---->|  GCS Bucket       |
|  (DVF+ .7z dump)  |     |  (ephemeral container)      |     |  (raw CSV/GeoJSON)|
|                   |     |                             |     |         |         |
|  Etalab           |---->|  data/export/*.csv          |---->|         v         |
|  (GeoJSON bounds) |     |  data/geojson/*.geojson     |     |  BigQuery         |
+-------------------+     +-----------------------------+     |  dvf_raw  (raw)   |
                                                              |      |            |
                                                              |  dbt v            |
                                                              |  dvf_staging      |
                                                              |  dvf_analytics    |
                                                              |      |            |
                                                              |      v            |
                                                              |  Looker Studio    |
                                                              +-------------------+
```

## Component Details

### 1. Infrastructure (Terraform)

All GCP resources are provisioned via Terraform (>= 1.5) using the `hashicorp/google` provider (~> 5.0).

**Resources created:**

| Resource | Type | Purpose |
|----------|------|---------|
| `google_storage_bucket.data_lake` | GCS bucket | Raw data landing zone (CSV + GeoJSON) |
| `google_bigquery_dataset.raw` | BigQuery dataset | `dvf_raw` -- raw tables loaded from GCS |
| `google_bigquery_dataset.staging` | BigQuery dataset | `dvf_staging` -- dbt staging/intermediate views |
| `google_bigquery_dataset.analytics` | BigQuery dataset | `dvf_analytics` -- dbt mart tables |
| `google_service_account.dvf_pipeline` | Service account | Identity for pipeline scripts |
| IAM bindings (3) | IAM | Storage Object Admin, BigQuery Data Editor, BigQuery Job User |

**Configuration (terraform/variables.tf):**

| Variable | Default | Description |
|----------|---------|-------------|
| `project_id` | *(required)* | GCP project ID |
| `region` | `europe-west9` | GCP region (Paris) |
| `gcs_bucket_name` | *(required)* | GCS bucket name |
| `bq_dataset_raw` | `dvf_raw` | Raw dataset name |
| `bq_dataset_staging` | `dvf_staging` | Staging dataset name |
| `bq_dataset_analytics` | `dvf_analytics` | Analytics dataset name |

### 2. Docker Services (docker-compose.yml)

Three services defined, two currently active:

| Service | Image | Purpose | Ports | Status |
|---------|-------|---------|-------|--------|
| `postgres` | Custom (PG 16 + PostGIS 3.4) | Ephemeral DVF+ restore and export | `127.0.0.1:5432` | Active during ingestion |
| `kestra-postgres` | `postgres:16` | Kestra metadata store | Internal only | Active with Kestra |
| `kestra` | `kestra/kestra:v0.21.1` | DAG-based pipeline orchestrator | `127.0.0.1:8080` | Active with Kestra |

All ports are bound to `127.0.0.1` (localhost only). All services have healthchecks configured.

The DVF PostgreSQL container is **ephemeral** -- it has no persistent volume. It is started for ingestion (restore SQL dump + export to CSV), then destroyed. It is not part of the runtime stack. It uses a custom `postgresql.conf` tuned for bulk-load operations (increased shared_buffers, work_mem, WAL settings) and `shm_size: 2g` to prevent shared memory exhaustion during large restores.

### 3. Ingestion Package (`ingestion/`)

The `ingestion` package contains 8 modules that form the data pipeline from source to BigQuery. All modules share configuration through `ingestion/config.py`.

#### 3.1 Shared Configuration (`config.py`)

Loads environment variables from `.env` and exposes:
- **Typed constants**: `GCP_PROJECT_ID`, `GCS_BUCKET_NAME`, `BQ_DATASET_RAW`, `DVF_MODE`, etc.
- **Path constants**: `DATA_DIR`, `DATA_EXPORT_DIR`, `DATA_GEOJSON_DIR`, `GCS_DVF_PREFIX`, `GCS_GEOJSON_PREFIX`
- **HTTP constants**: `HTTP_CONNECT_TIMEOUT` (30s), `DOWNLOAD_CHUNK_SIZE` (1 MB)
- **Connection helpers**: `get_pg_connection()` (psycopg2), `get_gcs_client()` (google-cloud-storage)

All external dependencies (psycopg2, google-cloud-storage) use lazy imports to avoid import-time issues during testing.

#### 3.2 Download DVF+ (`download_dvf.py`)

Downloads the DVF+ SQL dump from Cerema Box via HTTP. Supports two modes:
- **Automatic**: tries the data.gouv.fr redirect URL, then falls back to Cerema Box direct URL
- **Manual**: accepts a pre-downloaded `.7z` or `.sql` file via `--file` argument

The downloaded `.7z` archive is extracted with `py7zr`. Archive members are validated for path traversal before extraction (no `..` components, no absolute paths).

**Output**: `.sql` files in `data/`

#### 3.3 Restore Dump (`restore_dump.py`)

Restores SQL files into the ephemeral PostgreSQL container using `psql` subprocess calls. In demo mode (`DVF_MODE=demo`), filters data to keep only the configured departments (default: `DVF_DEMO_DEPARTMENTS=974`, La Reunion) by deleting rows outside those departments from all tables with a `coddep` column.

After restore, verifies that principal tables exist and logs row counts.

**Input**: `.sql` files in `data/`
**Output**: Populated PostgreSQL tables

#### 3.4 Export Tables (`export_tables.py`)

Exports 11 DVF+ tables from PostgreSQL to CSV using `COPY TO` with custom handling:
- **Geometry columns** (PostGIS points): extracted as `latitude` (FLOAT) and `longitude` (FLOAT) via `ST_Y()` and `ST_X()`
- **Polygon geometries**: dropped (not needed for BigQuery analytics)
- **Array columns**: reduced to first element (scalar value)
- **Simple tables** (no special columns): exported with plain `COPY TO`

Tables exported: `mutation`, `disposition`, `local`, `disposition_parcelle`, `parcelle`, `adresse`, `ann_nature_mutation`, `ann_type_local`, `ann_cgi`, `ann_nature_culture`, `ann_nature_culture_speciale`

Department codes used in WHERE clauses are validated against regex `^[0-9]{2,3}[A-B]?$` before SQL embedding.

**Input**: PostgreSQL tables
**Output**: CSV files in `data/export/`

#### 3.5 Download GeoJSON (`download_geojson.py`)

Downloads administrative boundary GeoJSON files from Etalab (2024 edition):
- `departements-1000m.geojson` (~340 KB, 101 departments)
- `communes-1000m.geojson` (~10 MB, ~35,000 communes)

Uses streaming download with progress bar. Validates downloaded files contain valid GeoJSON with features.

**Output**: GeoJSON files in `data/geojson/`

#### 3.6 Upload to GCS (`upload_to_gcs.py`)

Uploads exported files to the configured GCS bucket with organized prefixes:
- CSV files to `raw/dvf/` prefix (demo/standard mode)
- GeoJSON files to `raw/geojson/` prefix

Additionally provides `upload_chunk_to_gcs()` for chunked ingestion, which uploads each chunk's CSV files to per-table subdirectories (e.g., `raw/dvf/mutation/chunk_001.csv`). Header-only CSV files (< 1 KB) are automatically skipped.

Uses the shared `get_gcs_client()` from config. Progress displayed with tqdm.

**Input**: `data/export/*.csv` + `data/geojson/*.geojson` (or chunk directory for chunked mode)
**Output**: Files in `gs://<bucket>/raw/dvf/` and `gs://<bucket>/raw/geojson/`

#### 3.7 Load to BigQuery (`load_to_bigquery.py`)

Loads all data from GCS into BigQuery raw tables (`dvf_raw` dataset). Handles two data formats and two layout modes:

**CSV loading** (DVF+ tables):
- Discovers all `.csv` blobs under `raw/dvf/` prefix in GCS
- Groups blobs by table name, handling both flat layout (`raw/dvf/mutation.csv`) and chunked layout (`raw/dvf/mutation/chunk_001.csv`)
- Single-file tables: uses `autodetect=True` for schema inference
- Multi-file tables (chunked): uses wildcard URIs (`gs://bucket/raw/dvf/mutation/*`) with an explicit all-STRING schema (except `anneemut` as INT64) to avoid autodetect type conflicts across chunk files
- Uses `WRITE_TRUNCATE` for idempotent reloads
- The `mutation` table gets special treatment: integer range partitioning on `anneemut` (2014--2026, interval 1) and clustering on `coddep`, `codtypbien`

**GeoJSON loading** (administrative boundaries):
- Discovers all `.geojson` blobs under `raw/geojson/` prefix
- Downloads and parses each GeoJSON FeatureCollection in memory
- Each feature is converted to a flat dict row: properties as columns + geometry as JSON string
- Loaded via `load_table_from_json` with an explicit all-STRING schema to prevent autodetect from misinterpreting codes like `2A001` (Corse) as integers
- File-to-table mapping: `departements-1000m.geojson` / `departments.geojson` to `geo_departments`, `communes-1000m.geojson` / `communes.geojson` to `geo_communes`
- Geometry-to-GEOGRAPHY conversion happens in the dbt staging layer via `ST_GEOGFROMGEOJSON()`

**Input**: GCS bucket contents
**Output**: BigQuery tables in `dvf_raw` dataset

#### 3.8 Chunked Ingestion (`chunked_ingest.py`)

Orchestrates full-France ingestion by processing department SQL files in configurable batches. Designed for the national DVF+ dump where restoring all departments at once would exceed PostgreSQL shared memory and disk limits.

**Flow per chunk:**
1. Discover department SQL files (and the annexe reference file)
2. Read JSON progress file to determine which departments are already completed
3. Group remaining department files into chunks of `DVF_CHUNK_SIZE`
4. For each chunk: reset PG data schemas, restore department files, export to CSV, move CSVs to chunk subdirectory, upload to GCS per-table subdirectories, delete local files, save progress

**Progress tracking:**
- `data/chunked_progress.json` stores completed department filenames and per-table row counts
- Atomic writes (write-to-temp-then-rename) prevent corruption on crash
- On restart, already-completed departments are skipped automatically

**Command:** `make ingest-chunked` (or `uv run python -m ingestion.chunked_ingest`)

**Input**: Department SQL files in `data/` or `data/1_DONNEES_LIVRAISON/`
**Output**: Per-table chunk files in GCS (`raw/dvf/{table}/chunk_NNN.csv`)

### 4. BigQuery Raw Tables

After loading, the `dvf_raw` dataset contains:

| Table | Source | Partitioning | Clustering | Description |
|-------|--------|-------------|------------|-------------|
| `mutation` | CSV | `anneemut` (integer range, 2014--2026) | `coddep`, `codtypbien` | Transaction records |
| `disposition` | CSV | None | None | Sub-transactions |
| `local` | CSV | None | None | Building/premises details |
| `disposition_parcelle` | CSV | None | None | Parcels per disposition |
| `parcelle` | CSV | None | None | Cadastral parcel reference |
| `adresse` | CSV | None | None | Address reference |
| `ann_nature_mutation` | CSV | None | None | Mutation type labels |
| `ann_type_local` | CSV | None | None | Premises type labels |
| `ann_cgi` | CSV | None | None | Tax code references |
| `ann_nature_culture` | CSV | None | None | Land use type labels |
| `ann_nature_culture_speciale` | CSV | None | None | Special cultivation labels |
| `geo_departments` | GeoJSON | None | None | Department boundaries (geometry as STRING) |
| `geo_communes` | GeoJSON | None | None | Commune boundaries (geometry as STRING) |

For detailed partitioning and clustering rationale, see [PARTITIONING.md](PARTITIONING.md).

### 5. dbt Transformations (`dbt_dvf/`)

The dbt project transforms raw BigQuery tables into a Kimball star schema using dbt-core with the dbt-bigquery adapter. The project uses `dbt_utils` (>= 1.1.0) for the date spine macro.

**Project structure:**
- **Profile**: connects to BigQuery via service account key, targets `dvf_staging` as the default dataset
- **Schema routing**: a custom `generate_schema_name` macro routes models to their target datasets. Models without a custom schema go to `dvf_staging`; mart models with `schema='dvf_analytics'` go directly to `dvf_analytics`.

#### 5.1 Staging Layer (6 views in `dvf_staging`)

Staging views clean and rename raw table columns to English, cast types explicitly, and apply basic filters:

| Model | Source Table | Key Transformations |
|-------|-------------|-------------------|
| `stg_dvf__mutations` | `dvf_raw.mutation` | Rename columns to English, cast types, parse date from string, filter `transaction_price_eur > 0` (removes non-market transactions) |
| `stg_dvf__dispositions` | `dvf_raw.disposition` | Rename columns, cast types |
| `stg_dvf__locals` | `dvf_raw.local` | Rename columns, cast types, parse date |
| `stg_dvf__parcelles` | `dvf_raw.disposition_parcelle` | Rename columns, cast types, parse date. Sources from `disposition_parcelle` (richer than `parcelle`: includes commune code, dates, land surfaces) |
| `stg_geo__departments` | `dvf_raw.geo_departments` | Rename `code`/`nom` to English, convert geometry STRING to BigQuery GEOGRAPHY via `SAFE.ST_GEOGFROMGEOJSON()` |
| `stg_geo__communes` | `dvf_raw.geo_communes` | Rename `code`/`nom` to English, convert geometry to GEOGRAPHY |

All staging models are materialized as **views** (no storage cost, always fresh).

#### 5.2 Intermediate Layer (1 view in `dvf_staging`)

| Model | Description |
|-------|-------------|
| `int_transactions__enriched` | Joins mutation data with aggregated disposition, local, and parcelle information at one row per mutation. Commune code sourced from parcelle data with fallback to the mutation's own commune code. Aggregations: disposition count and total price, max room count from locals, first commune code from parcelles. |

Materialized as a **view**.

#### 5.3 Mart Layer (5 tables in `dvf_analytics`)

| Model | Grain | Materialization | Partitioning | Clustering |
|-------|-------|-----------------|-------------|------------|
| `fct_transactions` | One row per transaction (mutation) | Table | `transaction_year` (integer range, 2014--2026) | `department_code`, `property_type_code` |
| `dim_communes` | One row per commune (municipality) | Table | None | None |
| `dim_property_types` | One row per property type code | Table | None | None |
| `dim_dates` | One row per day (2014-01-01 to 2025-12-31) | Table | None | None |
| `dim_geography` | One row per geographic entity (dept or commune) | Table | None | None |

**`fct_transactions`**: Selects from `int_transactions__enriched`, computes `price_per_sqm` as `SAFE_DIVIDE(transaction_price_eur, NULLIF(total_built_area_sqm, 0))`. Includes transaction price, areas, premises counts, room count, location coordinates, VEFA flag, and mutation nature.

**`dim_communes`**: Distinct commune codes from parcelle data joined with GeoJSON commune names. Falls back to commune code if no name match.

**`dim_property_types`**: Distinct property type codes from mutations with level 1 classification (1=Built property, 2=Unbuilt land).

**`dim_dates`**: Generated via `dbt_utils.date_spine` with day granularity. Includes year, quarter, month, month name, day of week, and is_weekend flag.

**`dim_geography`**: Union of department and commune boundaries from the geo staging models. Includes `geo_level` indicator ('department' or 'commune'), BigQuery GEOGRAPHY type, and computed centroids.

#### 5.4 dbt Data Tests (62 tests)

| Test Type | Coverage |
|-----------|----------|
| `unique` | All primary keys across staging, intermediate, and mart models |
| `not_null` | All primary keys, foreign keys, and critical business columns |
| `relationships` | `stg_dvf__dispositions.mutation_id` to staging mutations, `stg_dvf__locals.mutation_id` to staging mutations, `stg_dvf__parcelles.mutation_id` to staging mutations, `fct_transactions.department_code` to `dim_geography`, `fct_transactions.commune_code` to `dim_communes`, `fct_transactions.property_type_code` to `dim_property_types` |
| `accepted_values` | `dim_property_types.property_type_level1` in ('1', '2'), `dim_geography.geo_level` in ('department', 'commune') |
| `expression_is_true` | `fct_transactions.transaction_price_eur > 0` |

### 6. Looker Studio Dashboard

The dashboard connects to BigQuery mart tables in the `dvf_analytics` dataset and provides interactive visualizations:

| Tile | Chart Type | Dimension | Metric |
|------|-----------|-----------|--------|
| Tile 1: Transaction Count by Property Type | Horizontal bar chart | `property_type_label` | Record Count |
| Tile 2: Transaction Volume by Year | Line chart | `transaction_year` | Record Count (+ optional avg price) |
| Tile 3: Price per m2 by Department | Bar chart or map | `department_code` | AVG(`price_per_sqm`) |
| Filter: Department Code | Drop-down list | `department_code` | N/A |

Setup requires manual creation in the Looker Studio web UI. Full instructions and validation queries are in [DASHBOARD.md](DASHBOARD.md).

### 7. Kestra Orchestration

The Kestra DAG (`kestra/flows/dvf_pipeline.yml`) wraps all pipeline steps (ingestion + dbt) into a single orchestrated flow.

**DAG structure (8 top-level tasks, 2 parallel children):**

| Task | Type | Dependencies | Description |
|------|------|-------------|-------------|
| `download` | Shell | None | Download DVF+ SQL dump from Cerema (retry x3) |
| `start_postgres` | Shell | `download` | Start ephemeral PostgreSQL container |
| `restore` | Shell | `start_postgres` | Restore SQL dump into PostgreSQL (retry x3) |
| `export_and_geojson` | Parallel | `restore` | Parallel wrapper for export + GeoJSON download |
| `export_tables` | Shell | (parallel child) | Export PostgreSQL tables to CSV |
| `download_geojson` | Shell | (parallel child) | Download GeoJSON admin boundaries |
| `stop_postgres` | Shell | `export_and_geojson` | Stop and remove ephemeral PostgreSQL container |
| `upload` | Shell | `stop_postgres` | Upload CSV + GeoJSON to GCS |
| `load_bigquery` | Shell | `upload` | Load CSV + GeoJSON from GCS into BigQuery raw tables |
| `dbt_transform` | Shell | `load_bigquery` | Run dbt deps + run + test |

**Features:**
- Parallel execution for `export_tables` and `download_geojson` (wrapped in `export_and_geojson` parallel task)
- Retry logic on `download` and `restore` tasks (transient network/database failures)
- Mode input (`demo`/`full`) passed as a flow parameter
- Accessible via Kestra web UI at http://localhost:8080

**Commands:**
- `make docker-up-kestra` -- start Kestra and its internal PostgreSQL
- `make kestra-deploy` -- deploy the flow YAML to Kestra via API
- `make pipeline` -- trigger the pipeline execution via Kestra API
- `make pipeline-local` -- run the full pipeline sequentially without Kestra (fallback)

## Networking

| Service | Port | Binding | Protocol |
|---------|------|---------|----------|
| PostgreSQL (ephemeral) | 5432 | `127.0.0.1` only | TCP |
| Kestra UI | 8080 | `127.0.0.1` only | HTTP |
| GCS | 443 | Outbound HTTPS | HTTPS |
| BigQuery | 443 | Outbound HTTPS | HTTPS |
| Cerema Box | 443 | Outbound HTTPS | HTTPS |
| Etalab | 443 | Outbound HTTPS | HTTPS |

All external connections use HTTPS. No inbound connections from the internet are required.

## Security

- All credentials stored in `.env` (gitignored) and `gcp-sa-key.json` (gitignored)
- Service account uses least-privilege IAM: Storage Object Admin (bucket-scoped), BigQuery Data Editor (project-scoped), BigQuery Job User (project-scoped)
- Docker ports bound to `127.0.0.1` -- not exposed to network
- PostgreSQL uses local-only credentials (`dvf_local_only`) -- ephemeral container with no persistent data
- Archive extraction validates members for path traversal attacks
- Department code inputs validated against regex before SQL embedding
- dbt connects to BigQuery via service account key (no password in connection string)
- No secrets in code, Terraform files, or documentation

## Data Volumes

| Stage | Demo mode (1 dept) | Full mode (all France) |
|-------|--------------------|-----------------------|
| SQL dump download | ~100 MB | ~4--5 GB |
| PostgreSQL restore | ~200 MB disk | ~15 GB disk |
| CSV export | ~50 MB | ~5--10 GB |
| GeoJSON download | ~10 MB | ~10 MB |
| GCS storage | ~60 MB | ~5--10 GB |
| BigQuery raw tables | ~60 MB | ~5--10 GB |
| BigQuery staging views | 0 (views) | 0 (views) |
| BigQuery mart tables | ~60 MB | ~5--10 GB |

## Pipeline Modes

The `DVF_MODE` environment variable controls the pipeline scope:

| Mode | Value | Departments | Duration | Purpose |
|------|-------|------------|----------|---------|
| Demo | `demo` (default) | 1 (La Reunion) | ~5 minutes | Peer reviewer reproduction |
| Full | `full` | All 101 | ~1--2 hours | Production dashboard |

In demo mode, the restore step filters data to keep only configured departments (default: `DVF_DEMO_DEPARTMENTS=974`). All downstream steps (export, upload, BigQuery load, dbt transforms) work with the filtered dataset. The BigQuery partitioning and clustering configuration is identical in both modes.

### Chunked Ingestion (Full-France)

For full-France ingestion, the chunked ingestion module (`ingestion/chunked_ingest.py`) processes department SQL files in configurable batches to avoid PostgreSQL WAL overflow and shared memory exhaustion.

**Chunk cycle:**
1. Reset PostgreSQL data schemas (preserve `dvf_plus_annexe`)
2. Restore chunk's department SQL files via `psql`
3. Export all tables to CSV
4. Move CSVs to a chunk subdirectory (`data/export/chunk_NNN/`)
5. Upload chunk CSVs to per-table GCS subdirectories (`raw/dvf/{table}/chunk_NNN.csv`)
6. Delete local chunk directory to free disk space
7. Save progress to `data/chunked_progress.json` (atomic write-to-temp-then-rename)

**Key design decisions:**
- **O(N) upload**: each chunk uploads only its own files to table-specific subdirectories, avoiding the O(N^2) cost of re-uploading accumulated files
- **Crash-safe resume**: a JSON progress file tracks completed department filenames; on restart, already-processed departments are skipped
- **Configurable chunk size**: `DVF_CHUNK_SIZE` (default: 10) controls the trade-off between RAM usage and restart overhead
- **PostgreSQL tuning**: `docker/postgres/postgresql.conf` configures shared_buffers, work_mem, and WAL settings for bulk-load performance; `shm_size: 2g` in docker-compose.yml prevents shared memory exhaustion

**BigQuery wildcard loading**: `load_to_bigquery.py` automatically detects chunked layouts and uses wildcard URIs (`gs://bucket/raw/dvf/mutation/*`) with an explicit all-STRING schema to avoid autodetect type conflicts across chunk files (e.g., Corsican department codes 2A/2B vs numeric codes).
