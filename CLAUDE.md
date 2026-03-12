# Project Configuration — DVF+ France

## Project Context
DVF+ France is a data engineering project that ingests, transforms, and visualizes
France's complete real estate transaction dataset (Cerema DVF+ Open Data, ~20M transactions).
Built as a final project for the DataTalks Club Data Engineering Zoomcamp 2026.

## Stack
- **Cloud**: GCP (project: valeurs-foncieres-analytics, region: europe-west9)
- **IaC**: Terraform
- **Data Lake**: Google Cloud Storage (GCS)
- **Data Warehouse**: BigQuery (partitioned + clustered)
- **Orchestration**: Kestra (Docker)
- **Transformations**: dbt-bigquery (Kimball dimensional model)
- **Dashboard**: Looker Studio
- **Language**: Python 3.11+
- **Containerization**: Docker Compose
- **Temporary DB**: PostgreSQL 16 + PostGIS (ephemeral, for SQL dump restore + export)

## Multi-Agent Workflow
This project uses a portable multi-agent development framework.
See `AGENTS_USAGE.md` for full documentation.

## Coordination Files (agents ALWAYS read these first)
- `PLAN.md` — Development plan, parts breakdown, acceptance criteria
- `STATE.md` — Live project state, progress, history
- `CORRECTIONS.md` — Pending corrections backlog
- `docs/BRIEF.md` — Project requirements and Zoomcamp criteria

## Code Standards

### Language
- **ALL code, comments, docstrings, commits, documentation in English**
- Reason: 3 peer reviewers will evaluate this project

### Python
- Python 3.11+ with type hints everywhere
- Functions < 25 lines, single responsibility
- Explicit error handling, no bare `except:`
- Linter: ruff
- Constants in UPPER_SNAKE_CASE
- No hardcoded values — use environment variables or constants

### SQL / dbt
- SQL keywords in UPPERCASE, identifiers in lowercase
- CTEs (WITH) over nested subqueries
- All queries parameterized in Python (never string concatenation)
- dbt naming convention:
  - `stg_dvf__[entity]` — staging (clean, type, filter)
  - `int_[entity]__[transform]` — intermediate (join, enrich)
  - `fct_[entity]` — fact tables
  - `dim_[entity]` — dimension tables
- dbt tests required: unique, not_null on primary keys minimum
- Materialization strategy: staging = view, marts = table

### BigQuery
- Dataset: `dvf_raw` (raw loaded data), `dvf_staging`, `dvf_marts`
- Partitioning: by `anneemut` (mutation year) — integer range partition
- Clustering: by `coddep` (department code), `codtypbien` (property type)
- Reason: most queries filter by time range and geographic area

### Data Format & Scope
- Source format: **SQL dump** (PostgreSQL/PostGIS, 17 relational tables) from Cerema Box
- Pipeline: download dump → Docker PG restore → export to CSV/Parquet → GCS → BigQuery
- Two pipeline modes via `DVF_MODE` env var:
  - `demo` (default): 1-2 departments, ~50 MB, ~10 min — for reviewer reproducibility
  - `full`: all France, ~5 GB, ~1-2h — for production dataset + Looker Studio dashboard
- PostgreSQL container is **ephemeral** — used during ingestion only, not part of runtime stack
- PostGIS geometries: extract as lat/lon floats (BigQuery has no native PostGIS)
- Latest version: October 2025 (Jan 2014 — Jun 2025)
- Download URL: https://cerema.app.box.com/v/dvfplus-opendata

### Deadline
- **Hard deadline: April 21, 2026** (Zoomcamp final project submission, second window)

### Infrastructure
- Terraform: all GCP resources (GCS bucket, BigQuery datasets, service account)
- Docker Compose: Kestra + dbt (not BigQuery/GCS — those are managed services)
- Environment variables prefixed by `DVF_` (e.g., DVF_GCP_PROJECT, DVF_GCS_BUCKET)
- All secrets in `.env`, never in code or Terraform state
- `.env.example` with all required variables (no values)

### Git
- Conventional commits: `feat(part-N)`, `fix(part-N)`, `refactor`, `docs`, `chore`
- One commit per logical change
- Never commit `.env`, credentials, or large data files
- `.gitignore`: .env, *.csv, *.sql.gz, terraform.tfstate*, .terraform/

## Zoomcamp Scoring Targets
| Criterion | Target | How |
|-----------|--------|-----|
| Problem description | 4/4 | Detailed README |
| Cloud + IaC | 4/4 | GCP + Terraform |
| Data ingestion | 4/4 | Kestra multi-step DAG → GCS → BigQuery |
| Data warehouse | 4/4 | Partitioned + clustered with explanation |
| Transformations | 4/4 | dbt-bigquery staging → marts |
| Dashboard | 4/4 | Looker Studio 2+ tiles |
| Reproducibility | 4/4 | Makefile + README + Docker + Terraform |
