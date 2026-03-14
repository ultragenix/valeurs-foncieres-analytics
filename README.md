# Valeurs Foncieres Analytics

End-to-end data pipeline for French real estate transaction analytics using the DVF+ dataset (20M+ transactions, 2014--2025).

## Problem Statement

France publishes one of the most comprehensive open real estate transaction datasets in the world: **DVF+** (Demandes de Valeurs Foncieres), maintained by [Cerema](https://www.cerema.fr/). It covers every notarized property sale since January 2014 -- over 20 million transactions across 17 relational tables. The data includes transaction prices, property types, land and built areas, locations, and cadastral references for the entire country (metropolitan France and overseas territories).

However, DVF+ is distributed as a raw PostgreSQL dump weighing 4--5 GB. Analyzing it requires restoring a full relational database, understanding a complex multi-table schema, and joining 4+ tables to produce meaningful analytics. There is no ready-to-use analytical platform for exploring historical price trends, comparing regions, analyzing property type distributions, or understanding market dynamics at scale.

This project builds a **cloud-native analytics pipeline on GCP** that ingests the DVF+ SQL dump, transforms it through a Kimball star schema using dbt, and exposes the results in an interactive Looker Studio dashboard. A peer reviewer can clone this repository, provision the infrastructure with Terraform, and run the full pipeline with a single `make run` command.

## Architecture

```
                                LOCAL MACHINE (Docker)
                                +--------------------------+
                                |  PostgreSQL 16 + PostGIS |
CEREMA BOX                      |  (ephemeral container)   |
+-----------+   download_dvf.py |                          |
| DVF+ SQL  | -----(HTTP)-----> |  restore_dump.py         |
| dump (.7z)|                   |  export_tables.py -> CSV |
+-----------+                   +------------+-------------+
                                             |
                                upload_to_gcs.py
                                             |
                                             v
                                GCP (europe-west9)
                                +-----------------------------------+
ETALAB / IGN                    |                                   |
+------------+  download_geojson|  GCS Bucket (data lake)           |
| GeoJSON    | -(python)------> |  gs://...-dvf-data-lake/          |
| dept/comm  |                  |    raw/dvf/*.csv                  |
+------------+                  |    raw/geojson/*.geojson          |
                                +----------------+------------------+
                                                 |
                                load_to_bigquery.py
                                                 |
                                                 v
                                +-----------------------------------+
                                |  BigQuery                         |
                                |                                   |
                                |  dvf_raw       (raw tables)       |
                                |    mutation (partitioned/clustered)|
                                |    disposition, local, parcelle...|
                                |    geo_departments, geo_communes  |
                                |  dvf_staging   (dbt views)        |
                                |    stg_dvf__mutations, etc. (6)   |
                                |  dvf_analytics (dbt marts)        |
                                |    fct_transactions               |
                                |      partitioned: year            |
                                |      clustered: dept, type        |
                                |    dim_communes                   |
                                |    dim_property_types             |
                                |    dim_dates, dim_geography       |
                                +----------------+------------------+
                                                 |
                                      Looker Studio
                                                 |
                                                 v
                                +-----------------------------------+
                                |  Dashboard (shareable URL)        |
                                |    Tile 1: tx count by prop type  |
                                |    Tile 2: price evolution / year |
                                |    Tile 3: price/m2 by dept      |
                                +-----------------------------------+

ORCHESTRATION: Kestra DAG wraps all steps into a single end-to-end pipeline
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Cloud Provider | GCP (`europe-west9` -- Paris) | Hosting infrastructure, free tier eligible |
| Infrastructure as Code | Terraform >= 1.5 | Provision GCS bucket, BigQuery datasets, service account |
| Data Lake | Google Cloud Storage (GCS) | Raw data landing zone (CSV + GeoJSON) |
| Data Warehouse | BigQuery | Serverless analytics with partitioning and clustering |
| Transformations | dbt-core + dbt-bigquery | Kimball star schema: staging, intermediate, marts |
| Orchestration | Kestra (Docker) | DAG-based pipeline execution |
| Dashboard | Looker Studio | Interactive tiles, shareable URL, native BigQuery connector |
| Containerization | Docker Compose v2 | Ephemeral PostgreSQL for dump restore, Kestra runtime |
| Language | Python 3.11+ | Ingestion and export scripts |
| Package Manager | uv | Fast Python dependency management |
| Temporary Database | PostgreSQL 16 + PostGIS 3.4 | Restore DVF+ SQL dump, export tables to CSV |

## Data Model

The project implements a **Kimball star schema** in BigQuery with one fact table and four dimension tables, built using dbt-bigquery (12 models: 6 staging views, 1 intermediate view, 5 mart tables):

```
                    +-------------------+
                    |   dim_dates       |
                    +-------------------+
                           |
+-------------------+      |      +-------------------+
| dim_communes      |------+------| dim_property_types|
+-------------------+      |      +-------------------+
                           |
                    +------+--------+
                    | fct_transactions|
                    +-----------+---+
                                |
                    +-------------------+
                    |  dim_geography    |
                    +-------------------+
```

**`fct_transactions`** -- One row per real estate transaction (mutation). Includes transaction price, land area, built area, computed price per square meter, property type, location, and VEFA (off-plan sale) flag. Partitioned by `transaction_year` (integer range, 2014--2026), clustered by `department_code` and `property_type_code`. Written to `dvf_analytics` dataset.

**`dim_communes`** -- Commune dimension built from parcelle data joined with GeoJSON commune names. Contains INSEE code, commune name, and department code.

**`dim_property_types`** -- Property type hierarchy based on the GnDVF classification. Level 1 splits built properties (code starting with 1) from unbuilt land (code starting with 2).

**`dim_dates`** -- Date spine generated with `dbt_utils.date_spine`, covering 2014-01-01 to 2025-12-31 with year, quarter, month, month name, day-of-week, and is_weekend attributes.

**`dim_geography`** -- Geographic boundaries from GeoJSON (departments and communes) unioned into a single table with geo_level indicator. Includes BigQuery GEOGRAPHY type for map visualizations and computed centroids.

### Data Flow

```
PostgreSQL (temp)       GCS (raw CSV)                  BigQuery (dvf_raw)         dbt (staging/marts)
-----------------       ---------------                ------------------         -------------------
mutation           -->  raw/dvf/mutation.csv       -->  mutation (part/clust) -->  stg_dvf__mutations
disposition        -->  raw/dvf/disposition.csv    -->  disposition           -->  stg_dvf__dispositions
local              -->  raw/dvf/local.csv          -->  local                 -->  stg_dvf__locals
disposition_parcelle->  raw/dvf/disp_parcelle.csv  -->  disposition_parcelle  -->  stg_dvf__parcelles
parcelle           -->  raw/dvf/parcelle.csv       -->  parcelle              |
adresse            -->  raw/dvf/adresse.csv        -->  adresse               |
ann_* (5 tables)   -->  raw/dvf/ann_*.csv          -->  ann_* (5 tables)      |
                                                                               |
dept.geojson       -->  raw/geojson/dept-1000m...  -->  geo_departments       -->  stg_geo__departments
communes.geojson   -->  raw/geojson/comm-1000m...  -->  geo_communes          -->  stg_geo__communes
                                                                               |
                                                                               v
                                                                    int_transactions__enriched
                                                                               |
                                                                               v
                                                                    fct_transactions (dvf_analytics)
                                                                    dim_communes
                                                                    dim_property_types
                                                                    dim_dates
                                                                    dim_geography
                                                                               |
                                                                               v
                                                                    Looker Studio Dashboard
```

Kestra orchestration wraps all steps into a single end-to-end DAG. Run via `make pipeline` (Kestra API) or `make run` (local sequential fallback).

## BigQuery Optimization

Partitioning and clustering are applied at two layers:

**Raw layer** (`dvf_raw.mutation`): Integer range partitioning on `anneemut` (year, range 2014--2026) and clustering on `coddep` (department code) and `codtypbien` (property type code). Applied at load time by `load_to_bigquery.py`.

**Mart layer** (`dvf_analytics.fct_transactions`): Integer range partitioning on `transaction_year` (2014--2026) and clustering on `department_code` and `property_type_code`. Applied by the dbt materialization config.

Both layers use the same strategy because the query patterns are consistent across raw exploration and dashboard queries. This combination reduces bytes scanned by up to 95% for typical dashboard queries that filter by year and department.

For a detailed explanation with query examples and cost impact analysis, see [docs/PARTITIONING.md](docs/PARTITIONING.md). For the full technical architecture, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Prerequisites

Before starting, ensure you have the following installed and configured:

| Prerequisite | Version | Installation |
|-------------|---------|-------------|
| GCP Account | -- | [console.cloud.google.com](https://console.cloud.google.com/) (free tier works) |
| gcloud CLI | latest | [cloud.google.com/sdk/docs/install](https://cloud.google.com/sdk/docs/install) |
| Terraform | >= 1.5 | [developer.hashicorp.com/terraform/install](https://developer.hashicorp.com/terraform/install) |
| Docker + Docker Compose | v2 | [docs.docker.com/get-docker](https://docs.docker.com/get-docker/) |
| Python | 3.11+ | [python.org/downloads](https://www.python.org/downloads/) |
| uv | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Make | any | Pre-installed on Linux/macOS; on Windows use WSL |

You will also need:
- A GCP project with billing enabled (BigQuery free tier: 1 TB queries/month, 10 GB storage)
- A GCP service account key (JSON) with Storage Object Admin and BigQuery Data Editor roles

## Quick Start

### 1. Clone and configure

```bash
git clone <this-repository-url>
cd valeurs-foncieres-analytics
cp .env.example .env
```

Edit `.env` with your GCP project details:

```bash
# Required: set your GCP project ID and bucket name
GCP_PROJECT_ID=your-gcp-project-id
GCS_BUCKET_NAME=your-project-id-dvf-data-lake
GOOGLE_APPLICATION_CREDENTIALS=./gcp-sa-key.json

# Demo mode: choose which department(s) to load (default: 75,13)
# Use the department code matching your downloaded region (e.g. 974 for La Reunion)
DVF_DEMO_DEPARTMENTS=75,13
```

### 2. Create a GCP service account

1. Go to [console.cloud.google.com](https://console.cloud.google.com/) and create a project (or use an existing one)
2. Enable the required APIs: **BigQuery API** and **Cloud Storage API**
3. Go to **IAM & Admin > Service Accounts** and create a new service account
4. Grant the following roles:
   - `Storage Object Admin` (for GCS bucket read/write)
   - `BigQuery Data Editor` (for creating tables and loading data)
   - `BigQuery Job User` (for running queries)
5. Create a JSON key for the service account and save it as `./gcp-sa-key.json` in the project root

```bash
# The file should be at the project root (it is gitignored)
cp ~/Downloads/your-service-account-key.json ./gcp-sa-key.json
```

### 3. Install dependencies and initialize Terraform

```bash
make setup
```

This runs three steps: copies `.env.example` to `.env` (if not already done), installs Python dependencies with `uv pip install -r requirements.txt`, and runs `terraform init` in the `terraform/` directory.

### 4. Provision GCP infrastructure

```bash
make terraform-apply
```

This creates:
- A GCS bucket for the raw data lake
- Three BigQuery datasets (`dvf_raw`, `dvf_staging`, `dvf_analytics`)
- A service account with Storage Object Admin and BigQuery Data Editor/Job User roles

### 5. Download the DVF+ data (manual step)

The DVF+ SQL dump is hosted on Cerema Box and requires a manual download:

1. Download the `.7z` archive for your target region:

| Region | Direct link | File size | Department codes | Use case |
|--------|------------|-----------|-----------------|----------|
| `R04_La_Reunion` | [Download](https://cerema.app.box.com/v/dvfplus-opendata/folder/347155412504) | ~38 MB | `974` | Fastest demo (~5 min) |
| `R11_Ile_de_France` | [Browse](https://cerema.app.box.com/v/dvfplus-opendata) | ~700 MB | `75,77,78,91,92,93,94,95` | Paris region |
| `National` (11 files) | [Browse](https://cerema.app.box.com/v/dvfplus-opendata) | ~4-5 GB | All | Full France production |

> For other regions, browse [cerema.app.box.com/v/dvfplus-opendata](https://cerema.app.box.com/v/dvfplus-opendata) and navigate to the latest folder.

4. Place the downloaded `.7z` file(s) in the `data/` directory:

```bash
mkdir -p data
cp ~/Downloads/DVFPlus_*.7z* data/
```

5. Update `DVF_DEMO_DEPARTMENTS` in `.env` to match your downloaded region:

```bash
# For La Reunion:
DVF_DEMO_DEPARTMENTS=974

# For Ile-de-France (Paris only):
DVF_DEMO_DEPARTMENTS=75

# For full France (no filtering):
DVF_MODE=full
```

> **Note**: The automatic download (`make ingest-download`) attempts to fetch the dump via HTTP but may fail if the Cerema URL has changed. The manual download is the reliable method.

### 6. Run the full pipeline

```bash
make run
```

This single command executes the entire pipeline sequentially: extracts and restores the DVF+ SQL dump, starts an ephemeral PostgreSQL container, exports the data to CSV, downloads GeoJSON administrative boundaries, uploads everything to GCS, loads into BigQuery, runs all dbt transformations and tests, then shuts down PostgreSQL.

For Kestra-based orchestration (optional), start Kestra with `make docker-up-kestra`, deploy the flow with `make kestra-deploy`, and trigger via `make pipeline`.

**Pipeline modes** (set `DVF_MODE` in `.env`):

| Mode | Scope | Duration | Use Case |
|------|-------|----------|----------|
| `demo` (default) | Configured departments only | ~5-15 minutes | Peer review -- proves pipeline works end-to-end |
| `full` | All of France (~20M transactions) | ~1-2 hours | Production dashboard with complete dataset |

For detailed step descriptions, dependencies, and error handling, see [docs/PIPELINE.md](docs/PIPELINE.md).

### 7. View the dashboard

**Dashboard URL**: [https://lookerstudio.google.com/reporting/b0b00d24-9d2f-4164-86f2-79e72340f4ac](https://lookerstudio.google.com/reporting/b0b00d24-9d2f-4164-86f2-79e72340f4ac)

The Looker Studio dashboard connects directly to the `dvf_analytics` BigQuery dataset. It includes 4 tiles across 2 pages (transaction count, total value, average price evolution, transaction volume) and 2 interactive filters (year, property type). Setup instructions are in [docs/DASHBOARD.md](docs/DASHBOARD.md).

Reviewers can access the dashboard via the link above without running the pipeline.

To validate that the dashboard tiles return correct data:

```bash
make dashboard-validate
```

## Environment Variables

All configuration is managed through `.env` (see [.env.example](.env.example) for the template):

| Variable | Description | Default |
|----------|-------------|---------|
| `GCP_PROJECT_ID` | Your GCP project ID | *(required)* |
| `GCP_REGION` | GCP region | `europe-west9` |
| `GCS_BUCKET_NAME` | GCS bucket for raw data lake | *(required)* |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON key | `./gcp-sa-key.json` |
| `BQ_DATASET_RAW` | BigQuery dataset for raw tables | `dvf_raw` |
| `BQ_DATASET_STAGING` | BigQuery dataset for dbt staging | `dvf_staging` |
| `BQ_DATASET_ANALYTICS` | BigQuery dataset for dbt marts | `dvf_analytics` |
| `POSTGRES_USER` | Ephemeral PostgreSQL user | `dvf` |
| `POSTGRES_PASSWORD` | Ephemeral PostgreSQL password | `dvf_local_only` |
| `POSTGRES_DB` | Ephemeral PostgreSQL database | `dvf` |
| `POSTGRES_HOST` | Ephemeral PostgreSQL host | `localhost` |
| `POSTGRES_PORT` | Ephemeral PostgreSQL port | `5432` |
| `DVF_MODE` | Pipeline mode: `demo` or `full` | `demo` |
| `DVF_DEMO_DEPARTMENTS` | Departments for demo mode | `75,13` |
| `KESTRA_PORT` | Kestra web UI port | `8080` |
| `DBT_PROFILES_DIR` | Path to dbt profiles directory | `./dbt_dvf` |

## Project Structure

```
valeurs-foncieres-analytics/
├── Makefile                     # Build targets: setup, terraform-*, docker-*, ingest-*, dbt-*, clean
├── .env.example                 # Environment variables template (16 variables)
├── docker-compose.yml           # PostgreSQL (ephemeral) + Kestra v0.21.1 + Kestra PostgreSQL
├── requirements.txt             # Python dependencies (GCS, BigQuery, dbt, psycopg2, py7zr, etc.)
│
├── terraform/
│   ├── main.tf                  # GCS bucket + BigQuery datasets + service account + IAM
│   ├── variables.tf             # Configurable parameters (project_id, region, bucket, datasets)
│   └── outputs.tf               # Resource references (bucket URL, dataset IDs, SA email)
│
├── docker/
│   └── postgres/
│       └── Dockerfile           # PostgreSQL 16 + PostGIS 3.4 (auto-enables PostGIS extension)
│
├── ingestion/                   # Python ingestion package (Parts 2-4)
│   ├── __init__.py              # Package marker
│   ├── config.py                # Shared configuration (loads .env, typed constants, connections)
│   ├── download_dvf.py          # Download DVF+ SQL dump from Cerema (auto or manual .7z/.sql)
│   ├── restore_dump.py          # Execute SQL files via psql, verify tables, demo filtering
│   ├── export_tables.py         # COPY tables to CSV (geometry->lat/lon, array->scalar handling)
│   ├── download_geojson.py      # Download admin boundary GeoJSON from Etalab (dept + communes)
│   ├── upload_to_gcs.py         # Upload CSV + GeoJSON to GCS (raw/dvf/ and raw/geojson/)
│   └── load_to_bigquery.py      # Load CSV + GeoJSON from GCS into BigQuery raw tables
│
├── dbt_dvf/                     # dbt project (Part 5)
│   ├── dbt_project.yml          # Project config: staging/intermediate as views, marts as tables
│   ├── profiles.yml             # BigQuery connection (uses env vars for credentials)
│   ├── packages.yml             # dbt_utils >= 1.1.0
│   ├── macros/
│   │   └── generate_schema_name.sql  # Custom schema routing (staging->dvf_staging, marts->dvf_analytics)
│   └── models/
│       ├── sources.yml          # Source definitions for all 13 dvf_raw BigQuery tables
│       ├── staging/             # 6 views: stg_dvf__mutations, dispositions, locals, parcelles + 2 geo
│       ├── intermediate/        # 1 view: int_transactions__enriched (joins 4 staging models)
│       └── marts/               # 5 tables: fct_transactions, dim_communes, dim_property_types,
│                                #           dim_dates, dim_geography (written to dvf_analytics)
│
├── kestra/                      # Kestra orchestration flows
│   └── flows/
│       └── dvf_pipeline.yml     # End-to-end DAG (8 tasks, parallel export+geojson)
│
├── docs/
│   ├── BRIEF.md                 # Project requirements and scope
│   ├── DATA_SOURCES.md          # DVF+ data reference (17 tables, columns, joins, quality rules)
│   ├── ARCHITECTURE.md          # Technical architecture deep-dive
│   ├── PIPELINE.md              # Pipeline documentation (steps, dependencies)
│   ├── PARTITIONING.md          # BigQuery partitioning/clustering rationale
│   └── DASHBOARD.md             # Looker Studio tile specs, setup instructions, validation queries
│
└── tests/                       # Unit tests (not tracked in git)
    ├── test_download_dvf.py     # 34 tests: download, manual file handling, archive extraction
    ├── test_restore_dump.py     # 36 tests: psql execution, demo filtering, verification
    ├── test_export_tables.py    # CSV export, query building, geometry/array handling
    ├── test_download_geojson.py # GeoJSON download, validation, skip logic
    ├── test_upload_to_gcs.py    # GCS upload, file collection, bucket validation
    ├── test_bigquery_loading.py # 57 tests: CSV/GeoJSON loading, partitioning, config validation
    └── qa/                      # Independent QA audit tests
```

## Zoomcamp Evaluation Criteria

This project targets the maximum score of **28/28** across all 7 evaluation criteria for the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) final project:

| # | Criterion | Points | Implementation | Status |
|---|-----------|--------|----------------|--------|
| 1 | **Problem description** | 4/4 | Clearly described in this README: raw DVF+ dump transformed into an analytics-ready star schema | Done |
| 2 | **Cloud** | 4/4 | GCP infrastructure provisioned with Terraform (GCS + BigQuery + service account + IAM) | Done |
| 3 | **Data ingestion** | 4/4 | End-to-end pipeline: download, restore, export, upload to GCS, load to BigQuery. Orchestrated via Kestra DAG or `make run` | Done |
| 4 | **Data warehouse** | 4/4 | BigQuery with integer range partitioning (year) + clustering (department, property type) at both raw and mart layers -- see [docs/PARTITIONING.md](docs/PARTITIONING.md) | Done |
| 5 | **Transformations** | 4/4 | dbt-bigquery: 6 staging views, 1 intermediate join, 5 Kimball star schema mart tables, 62 data tests | Done |
| 6 | **Dashboard** | 4/4 | Looker Studio with 2+ tiles: transaction count by property type, price evolution by year, price/m2 by department. See [docs/DASHBOARD.md](docs/DASHBOARD.md) | Done |
| 7 | **Reproducibility** | 4/4 | Makefile + Docker + Terraform + `.env.example` + step-by-step README; `make setup && make terraform-apply && make run` (manual DVF+ download required -- see Quick Start step 4) | Done |

## Data Sources

### DVF+ Open Data (primary)

| Attribute | Value |
|-----------|-------|
| Publisher | [Cerema](https://www.cerema.fr/) (DGALN) |
| Content | All notarized real estate transactions in France |
| Period | January 2014 -- June 2025 |
| Volume | ~20M+ transactions across 17 relational tables (12 main + 5 annexe) |
| Format | PostgreSQL/PostGIS SQL dump (~4--5 GB, distributed as .7z archive) |
| Download | [cerema.app.box.com/v/dvfplus-opendata](https://cerema.app.box.com/v/dvfplus-opendata) |
| License | [Licence Ouverte v2.0](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) (free reuse) |
| Documentation | [doc-datafoncier.cerema.fr/doc/dv3f/](https://doc-datafoncier.cerema.fr/doc/dv3f/) |

The ingestion pipeline exports 11 tables to CSV: 6 principal/secondary tables (`mutation`, `disposition`, `local`, `disposition_parcelle`, `parcelle`, `adresse`) and 5 annexe reference tables (`ann_nature_mutation`, `ann_type_local`, `ann_cgi`, `ann_nature_culture`, `ann_nature_culture_speciale`). Geometry columns (PostGIS points) are extracted as latitude/longitude floats; heavy polygon geometries are dropped. Array columns are reduced to their first element.

For a complete reference of all 17 tables, columns, join keys, and data quality rules, see [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md).

### Administrative Boundaries (secondary)

| Attribute | Value |
|-----------|-------|
| Publisher | [Etalab](https://www.etalab.gouv.fr/) / [IGN](https://www.ign.fr/) |
| Content | GeoJSON polygons for all ~35,000 communes and 101 departments |
| Format | GeoJSON (1km generalization) |
| Download | [etalab-datasets.geo.data.gouv.fr](https://etalab-datasets.geo.data.gouv.fr/contours-administratifs/2024/geojson/) |
| License | Licence Ouverte v2.0 (IGN Admin Express) |

The pipeline downloads `departements-1000m.geojson` (~340 KB, 101 departments) and `communes-1000m.geojson` (~10 MB, ~35,000 communes) and uploads them to GCS for use in BigQuery geographic analysis and Looker Studio choropleth maps.

## Makefile Targets

```bash
make help               # Show all available targets
make setup              # Copy .env, install Python deps (uv), terraform init
make terraform-init     # Initialize Terraform providers
make terraform-plan     # Preview Terraform changes
make terraform-apply    # Provision GCP resources (GCS + BigQuery + SA)
make terraform-destroy  # Destroy all Terraform-managed GCP resources
make docker-up          # Start ephemeral PostgreSQL container
make docker-down        # Stop and remove all containers
make docker-up-kestra   # Start Kestra orchestrator (+ its internal PostgreSQL)
make ingest-download    # Download DVF+ SQL dump from Cerema
make ingest-restore     # Restore DVF+ SQL dump into PostgreSQL container
make ingest-export      # Export PostgreSQL tables to CSV
make ingest-geojson     # Download GeoJSON admin boundaries from Etalab
make ingest-upload      # Upload CSV + GeoJSON to GCS
make bq-load            # Load CSV + GeoJSON from GCS into BigQuery raw tables
make dbt-deps           # Install dbt packages (dbt_utils)
make dbt-run            # Run all dbt models (staging + intermediate + marts)
make dbt-test           # Run all dbt tests
make dbt-build          # Full dbt workflow: deps + run + test
make dashboard-validate # Validate dashboard data by running tile queries against BigQuery
make run                # Run full pipeline (sequential, no Kestra required)
make pipeline           # Run pipeline via Kestra API (requires Kestra running)
make pipeline-local     # Run full pipeline locally (sequential, no Kestra required)
make kestra-deploy      # Deploy flow YAML to Kestra via API
make test               # Run all Python tests (uv run python -m pytest tests/ -v)
make clean              # Tear down everything (containers + GCP resources)
```

## Acknowledgments

- **[Cerema](https://www.cerema.fr/)** for the DVF+ open data (Licence Ouverte v2.0)
- **[Etalab](https://www.etalab.gouv.fr/) / [IGN](https://www.ign.fr/)** for the administrative boundary GeoJSON files (Licence Ouverte v2.0)
- **[DataTalksClub](https://github.com/DataTalksClub/data-engineering-zoomcamp)** for the Data Engineering Zoomcamp course and project framework

## Future Improvements

- Streaming ingestion to avoid the ephemeral PostgreSQL dump-and-restore step
- Runtime data quality monitoring with dbt elementary or Great Expectations
- Incremental dbt models for `fct_transactions` to reduce rebuild cost at scale
- CI/CD pipeline (GitHub Actions) for automated `dbt build` on pull requests
- Native BigQuery GeoJSON ingestion to replace the STRING-to-GEOGRAPHY staging step

## License

MIT
