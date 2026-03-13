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
| DVF+ SQL  | -----(curl)-----> |  pg_restore --> tables   |
| dump      |                   |  COPY TO --> CSV files   |
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
| dept/comm  |                  |    raw/mutations/*.csv            |
+------------+                  |    raw/dispositions/*.csv         |
                                |    raw/locals/*.csv               |
                                |    raw/parcelles/*.csv            |
                                |    raw/geojson/*.geojson          |
                                +----------------+------------------+
                                                 |
                                load_to_bigquery.py
                                                 |
                                                 v
                                +-----------------------------------+
                                |  BigQuery                         |
                                |                                   |
                                |  dvf_raw       (raw tables)       |
                                |  dvf_staging   (dbt views)        |
                                |  dvf_analytics (dbt marts)        |
                                |    fct_transactions               |
                                |      partitioned: year            |
                                |      clustered: dept, type        |
                                |    dim_communes                   |
                                |    dim_property_types             |
                                |    dim_dates                      |
                                |    dim_geography                  |
                                +----------------+------------------+
                                                 |
                                      Looker Studio (connector)
                                                 |
                                                 v
                                +-----------------------------------+
                                |  Dashboard (shareable URL)        |
                                |    Tile 1: tx count by prop type  |
                                |    Tile 2: price evolution / year |
                                |    Tile 3: price/m2 by dept      |
                                +-----------------------------------+

ORCHESTRATION: Kestra DAG ties all steps into a single end-to-end pipeline
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

The project implements a **Kimball star schema** in BigQuery with one fact table and four dimension tables:

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

**`fct_transactions`** -- One row per real estate transaction (mutation). Includes transaction price, land area, built area, computed price per square meter, property type, location, and VEFA (off-plan sale) flag. Partitioned by `transaction_year` (integer range), clustered by `department_code` and `property_type_code`.

**`dim_communes`** -- Commune reference table with INSEE code, department code, and postal code.

**`dim_property_types`** -- Property type hierarchy based on the GnDVF classification (built vs. unbuilt, with subtypes: house, apartment, outbuilding, commercial premises, land).

**`dim_dates`** -- Date spine from 2014-01-01 to 2025-12-31 with year, quarter, month, and day-of-week attributes.

**`dim_geography`** -- Geographic boundaries from GeoJSON (departments and communes) with names, region codes, and geometry for map visualizations.

### Transformation Lineage

```
PostgreSQL (temp)       GCS (raw CSV)        BigQuery raw          dbt staging              dbt intermediate        dbt marts
-----------------       -------------        ------------          -----------              ----------------        ---------
mutation           -->  mutations.csv   -->  dvf_raw.mutation -->  stg_dvf__mutations   -+
disposition        -->  dispositions.csv-->  dvf_raw.disposition-> stg_dvf__dispositions |
local              -->  locals.csv      -->  dvf_raw.local    -->  stg_dvf__locals      +-> int_transactions  -->  fct_transactions
disposition_parcelle->  parcelles.csv   -->  dvf_raw.parcelle -->  stg_dvf__parcelles   |     __enriched          dim_communes
                                                                                        |                         dim_property_types
dept.geojson       -->  geojson/        -->  dvf_raw.geo_dept -->  stg_geo__departments -+                         dim_dates
communes.geojson   -->  geojson/        -->  dvf_raw.geo_comm -->  stg_geo__communes    +---------------------->  dim_geography
```

## BigQuery Optimization

The fact table `fct_transactions` uses **integer range partitioning** on `transaction_year` and **clustering** on `department_code` and `property_type_code`. This combination reduces bytes scanned by up to 95% for typical dashboard queries that filter by year and department.

For a detailed explanation with query examples and cost impact analysis, see [docs/PARTITIONING.md](docs/PARTITIONING.md).

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
git clone https://github.com/<your-username>/valeurs-foncieres-analytics.git
cd valeurs-foncieres-analytics
cp .env.example .env
```

Edit `.env` with your GCP project details:

```bash
# Required: set your GCP project ID and bucket name
GCP_PROJECT_ID=your-gcp-project-id
GCS_BUCKET_NAME=your-project-id-dvf-data-lake
GOOGLE_APPLICATION_CREDENTIALS=./gcp-sa-key.json
```

Place your GCP service account key at `./gcp-sa-key.json`.

### 2. Install dependencies and initialize Terraform

```bash
make setup
```

This runs three steps: copies `.env.example` to `.env` (if not already done), installs Python dependencies with `uv pip install -r requirements.txt`, and runs `terraform init` in the `terraform/` directory.

### 3. Provision GCP infrastructure

```bash
make terraform-apply
```

This creates:
- A GCS bucket for the raw data lake
- Three BigQuery datasets (`dvf_raw`, `dvf_staging`, `dvf_analytics`)
- A service account with Storage Object Admin and BigQuery Data Editor/Job User roles

### 4. Run the pipeline

```bash
make run
```

This triggers the end-to-end pipeline: download DVF+ dump from Cerema, restore into an ephemeral PostgreSQL container, export tables to CSV, upload to GCS, load into BigQuery raw tables, and run dbt transformations.

**Pipeline modes** (set `DVF_MODE` in `.env`):

| Mode | Scope | Duration | Use Case |
|------|-------|----------|----------|
| `demo` (default) | 1--2 departments (Paris + Marseille) | ~10 minutes | Peer review -- proves pipeline works end-to-end |
| `full` | All of France (~20M transactions) | ~1--2 hours | Production dashboard with complete dataset |

### 5. View the dashboard

Dashboard URL will be added after implementation.

Reviewers can access the dashboard via a shareable Looker Studio link without running the full pipeline -- the dashboard points to the production BigQuery dataset populated with the complete France data.

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
| `POSTGRES_PORT` | Ephemeral PostgreSQL port | `5432` |
| `DVF_MODE` | Pipeline mode: `demo` or `full` | `demo` |
| `DVF_DEMO_DEPARTMENTS` | Departments for demo mode | `75,13` |
| `KESTRA_PORT` | Kestra web UI port | `8080` |
| `DBT_PROFILES_DIR` | Path to dbt profiles directory | `./dbt_dvf` |

## Project Structure

```
valeurs-foncieres-analytics/
├── Makefile                     # Build targets: setup, terraform-*, docker-*, run, clean
├── .env.example                 # Environment variables template
├── docker-compose.yml           # PostgreSQL (ephemeral) + Kestra + Kestra PostgreSQL
├── requirements.txt             # Python dependencies (GCS, BigQuery, dbt, psycopg2, etc.)
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
├── ingestion/                   # Python scripts for data loading (Parts 2-4)
│   ├── download_dvf.py          # Download DVF+ SQL dump from Cerema Box
│   ├── restore_dump.py          # pg_restore into ephemeral PostgreSQL container
│   ├── export_tables.py         # COPY tables to CSV (geometry + array handling)
│   ├── upload_to_gcs.py         # Upload CSV/GeoJSON to GCS
│   ├── load_to_bigquery.py      # Load from GCS into BigQuery raw tables
│   └── download_geojson.py      # Download admin boundaries GeoJSON from Etalab/IGN
│
├── kestra/
│   └── flows/
│       └── dvf_pipeline.yml     # End-to-end DAG: download -> restore -> export -> GCS -> BQ -> dbt
│
├── dbt_dvf/                     # dbt project (Part 5)
│   ├── dbt_project.yml
│   ├── profiles.yml             # BigQuery connection (uses env vars)
│   ├── packages.yml
│   ├── models/
│   │   ├── sources.yml
│   │   ├── staging/             # Clean individual source tables
│   │   │   ├── stg_dvf__mutations.sql
│   │   │   ├── stg_dvf__dispositions.sql
│   │   │   ├── stg_dvf__locals.sql
│   │   │   ├── stg_dvf__parcelles.sql
│   │   │   ├── stg_geo__departments.sql
│   │   │   └── stg_geo__communes.sql
│   │   ├── intermediate/        # Multi-table joins
│   │   │   └── int_transactions__enriched.sql
│   │   └── marts/               # Kimball star schema
│   │       ├── fct_transactions.sql
│   │       ├── dim_communes.sql
│   │       ├── dim_property_types.sql
│   │       ├── dim_dates.sql
│   │       └── dim_geography.sql
│   ├── seeds/
│   ├── tests/
│   └── macros/
│
├── docs/
│   ├── BRIEF.md                 # Project requirements and scope
│   ├── DATA_SOURCES.md          # DVF+ data reference (tables, columns, joins)
│   └── PARTITIONING.md          # BigQuery partitioning/clustering rationale
│
└── tests/                       # Python integration tests
```

## Zoomcamp Evaluation Criteria

This project targets the maximum score of **28/28** across all 7 evaluation criteria for the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) final project:

| # | Criterion | Points | Implementation |
|---|-----------|--------|----------------|
| 1 | **Problem description** | 4/4 | Clearly described in this README: raw DVF+ dump transformed into an analytics-ready star schema |
| 2 | **Cloud** | 4/4 | GCP infrastructure provisioned with Terraform (GCS + BigQuery + service account + IAM) |
| 3 | **Data ingestion** | 4/4 | End-to-end DAG orchestrated with Kestra: download, restore, export, upload to GCS, load to BigQuery |
| 4 | **Data warehouse** | 4/4 | BigQuery with integer range partitioning (year) + clustering (department, property type) -- see [docs/PARTITIONING.md](docs/PARTITIONING.md) |
| 5 | **Transformations** | 4/4 | dbt-bigquery: multi-table staging, intermediate join, Kimball star schema marts |
| 6 | **Dashboard** | 4/4 | Looker Studio with 2+ tiles: transaction count by property type, price evolution by year, price/m2 by department |
| 7 | **Reproducibility** | 4/4 | Makefile + Docker + Terraform + `.env.example` + step-by-step README; `make setup && make terraform-apply && make run` |

## Data Sources

### DVF+ Open Data (primary)

| Attribute | Value |
|-----------|-------|
| Publisher | [Cerema](https://www.cerema.fr/) (DGALN) |
| Content | All notarized real estate transactions in France |
| Period | January 2014 -- June 2025 |
| Volume | ~20M+ transactions across 17 relational tables |
| Format | PostgreSQL/PostGIS SQL dump (~4--5 GB) |
| Download | [cerema.app.box.com/v/dvfplus-opendata](https://cerema.app.box.com/v/dvfplus-opendata) |
| License | [Licence Ouverte v2.0](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) (free reuse) |
| Documentation | [doc-datafoncier.cerema.fr/doc/dv3f/](https://doc-datafoncier.cerema.fr/doc/dv3f/) |

For a complete reference of all 17 tables, columns, join keys, and data quality rules, see [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md).

### Administrative Boundaries (secondary)

| Attribute | Value |
|-----------|-------|
| Publisher | [Etalab](https://www.etalab.gouv.fr/) / [IGN](https://www.ign.fr/) |
| Content | GeoJSON polygons for all ~35,000 communes and 101 departments |
| Format | GeoJSON (1km generalization) |
| Download | [etalab-datasets.geo.data.gouv.fr](https://etalab-datasets.geo.data.gouv.fr/contours-administratifs/2023/geojson/) |
| License | Licence Ouverte v2.0 (IGN Admin Express) |

## Makefile Targets

```bash
make help               # Show all available targets
make setup              # Copy .env, install Python deps, terraform init
make terraform-init     # Initialize Terraform providers
make terraform-plan     # Preview Terraform changes
make terraform-apply    # Provision GCP resources (GCS + BigQuery + SA)
make terraform-destroy  # Destroy all Terraform-managed GCP resources
make docker-up          # Start ephemeral PostgreSQL container
make docker-down        # Stop and remove all containers
make docker-up-kestra   # Start Kestra orchestrator
make run                # Run full pipeline (end-to-end)
make dbt-run            # Run dbt transformations
make test               # Run all tests
make clean              # Tear down everything (containers + GCP resources)
```

## License

MIT
