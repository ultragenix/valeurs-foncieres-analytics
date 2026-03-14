# Architecture

Technical architecture for the DVF+ France real estate analytics pipeline. This document covers validated and implemented components (Parts 1--4). Planned components are noted where relevant.

## System Overview

The pipeline ingests France's DVF+ real estate transaction dataset (PostgreSQL dump, 17 tables, ~20M transactions) into a cloud-native analytics stack on GCP. Data flows from Cerema's open data portal through an ephemeral PostgreSQL container, into GCS as a data lake, and finally into BigQuery as the analytical data warehouse.

```
+-------------------+     +-----------------------------+     +-------------------+
|  External Sources |     |  Local Machine (Docker)     |     |  GCP europe-west9 |
|                   |     |                             |     |                   |
|  Cerema Box       |---->|  PostgreSQL 16 + PostGIS    |---->|  GCS Bucket       |
|  (DVF+ .7z dump)  |     |  (ephemeral container)      |     |  (raw CSV/GeoJSON)|
|                   |     |                             |     |         |         |
|  Etalab           |---->|  data/export/*.csv          |---->|         v         |
|  (GeoJSON bounds) |     |  data/geojson/*.geojson     |     |  BigQuery         |
+-------------------+     +-----------------------------+     |  dvf_raw (loaded) |
                                                              |  dvf_staging (dbt)|
                                                              |  dvf_analytics    |
                                                              |         |         |
                                                              |         v         |
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
| `google_bigquery_dataset.staging` | BigQuery dataset | `dvf_staging` -- dbt staging views |
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
| `kestra` | `kestra/kestra:v0.21.1` | DAG-based pipeline orchestrator | `127.0.0.1:8080` | Planned (Part 7) |

All ports are bound to `127.0.0.1` (localhost only). All services have healthchecks configured.

The DVF PostgreSQL container is **ephemeral** -- it has no persistent volume. It is started for ingestion (restore SQL dump + export to CSV), then destroyed. It is not part of the runtime stack.

### 3. Ingestion Package (`ingestion/`)

The `ingestion` package contains 7 modules that form the data pipeline from source to BigQuery. All modules share configuration through `ingestion/config.py`.

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

Restores SQL files into the ephemeral PostgreSQL container using `psql` subprocess calls. In demo mode (`DVF_MODE=demo`), filters data to keep only the configured departments (default: Paris 75, Marseille 13) by deleting rows outside those departments from all tables with a `coddep` column.

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
- CSV files to `raw/dvf/` prefix
- GeoJSON files to `raw/geojson/` prefix

Uses the shared `get_gcs_client()` from config. Progress displayed with tqdm.

**Input**: `data/export/*.csv` + `data/geojson/*.geojson`
**Output**: Files in `gs://<bucket>/raw/dvf/` and `gs://<bucket>/raw/geojson/`

#### 3.7 Load to BigQuery (`load_to_bigquery.py`)

Loads all data from GCS into BigQuery raw tables (`dvf_raw` dataset). Handles two data formats:

**CSV loading** (DVF+ tables):
- Discovers all `.csv` blobs under `raw/dvf/` prefix in GCS
- Table name derived from filename stem (e.g., `mutation.csv` becomes table `mutation`)
- Uses `autodetect=True` for schema inference
- Uses `WRITE_TRUNCATE` for idempotent reloads
- The `mutation` table gets special treatment: integer range partitioning on `anneemut` (2014--2026, interval 1) and clustering on `coddep`, `codtypbien`

**GeoJSON loading** (administrative boundaries):
- Discovers all `.geojson` blobs under `raw/geojson/` prefix
- Downloads and parses each GeoJSON FeatureCollection in memory
- Each feature is converted to a flat dict row: properties as columns + geometry as JSON string
- Loaded via `load_table_from_json` with a partial schema ensuring `geometry` is STRING
- File-to-table mapping: `departements-1000m.geojson` / `departments.geojson` to `geo_departments`, `communes-1000m.geojson` / `communes.geojson` to `geo_communes`
- Geometry-to-GEOGRAPHY conversion will happen in the dbt staging layer via `ST_GEOGFROMGEOJSON()`

**Input**: GCS bucket contents
**Output**: BigQuery tables in `dvf_raw` dataset

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

### 5. Planned Components

The following components are designed but not yet implemented:

**dbt Transformations (Part 5)**: Staging views to clean each source table, an intermediate join model (`int_transactions__enriched`), and Kimball star schema marts (`fct_transactions`, `dim_communes`, `dim_property_types`, `dim_dates`, `dim_geography`). See [DATA_SOURCES.md](DATA_SOURCES.md) for the target schema.

**Looker Studio Dashboard (Part 6)**: Interactive dashboard connected to BigQuery marts with at minimum 2 tiles: transaction count by property type and price evolution by year.

**Kestra Orchestration (Part 7)**: End-to-end DAG wiring all pipeline steps into a single orchestrated flow with error handling and retry logic.

## Networking

| Service | Port | Binding | Protocol |
|---------|------|---------|----------|
| PostgreSQL (ephemeral) | 5432 | `127.0.0.1` only | TCP |
| Kestra UI (planned) | 8080 | `127.0.0.1` only | HTTP |
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
- No secrets in code, Terraform files, or documentation

## Data Volumes

| Stage | Demo mode (2 depts) | Full mode (all France) |
|-------|--------------------|-----------------------|
| SQL dump download | ~100 MB | ~4--5 GB |
| PostgreSQL restore | ~200 MB disk | ~15 GB disk |
| CSV export | ~50 MB | ~5--10 GB |
| GeoJSON download | ~10 MB | ~10 MB |
| GCS storage | ~60 MB | ~5--10 GB |
| BigQuery raw tables | ~60 MB | ~5--10 GB |

## Pipeline Modes

The `DVF_MODE` environment variable controls the pipeline scope:

| Mode | Value | Departments | Duration | Purpose |
|------|-------|------------|----------|---------|
| Demo | `demo` (default) | 2 (Paris + Marseille) | ~10 minutes | Peer reviewer reproduction |
| Full | `full` | All 101 | ~1--2 hours | Production dashboard |

In demo mode, the restore step filters data to keep only configured departments (default: `DVF_DEMO_DEPARTMENTS=75,13`). All downstream steps (export, upload, BigQuery load) work with the filtered dataset. The BigQuery partitioning and clustering configuration is identical in both modes.
