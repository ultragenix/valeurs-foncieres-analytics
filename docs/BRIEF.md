# BRIEF.md — DVF+ France: Real Estate Transaction Analytics
## Date: 2026-03-12

---

## 1. PROBLEM STATEMENT

### Problem
France publishes detailed real estate transaction data (DVF+) covering every property sale since 2014 — over 20 million transactions across 17 relational tables. However, this rich dataset is delivered as raw PostgreSQL dumps that are difficult to analyze without significant data engineering effort. There is no ready-to-use analytical platform that allows users to explore historical price trends, compare regions, analyze property types, and understand market dynamics at scale.

### Target Users
- **Zoomcamp peer reviewers** (3 students) — will evaluate code quality, reproducibility, and architecture
- **Data analysts** — would use the final dashboard to explore French real estate trends
- **Future self** — the France-wide dataset serves as the foundation for a La Réunion-specific product (ReunIA, separate project)

### Success Criteria
- Score 28/28 on all 7 Zoomcamp evaluation criteria (4 points each)
- A peer reviewer can clone the repo, run `terraform apply` + pipeline, and see a working dashboard within 30 minutes
- Dashboard shows at least 2 meaningful tiles with real DVF+ data
- Pipeline is fully orchestrated end-to-end (no manual steps)

---

## 2. DATA SOURCES

See `docs/DATA_SOURCES.md` for the full data source reference.

---

## 3. FEATURES (MoSCoW)

### Must Have (Zoomcamp criteria)
1. **IaC with Terraform** — GCS bucket + BigQuery datasets (raw, staging, marts) + service account provisioning
2. **Data ingestion pipeline** — Multi-step DAG orchestrated with Kestra:
   - Download DVF+ SQL dump from Cerema Box
   - Restore into temporary PostgreSQL container (Docker)
   - Export key tables to CSV via `COPY TO`
   - Upload exported files to GCS (data lake)
   - Load from GCS into BigQuery raw tables
3. **BigQuery DWH with optimization** — Tables partitioned by mutation year + clustered by department code and property type (with written explanation of why)
4. **dbt transformations** — staging → intermediate → marts (Kimball dimensional model):
   - Staging: clean each source table independently (`stg_dvf__mutations`, `stg_dvf__dispositions`, `stg_dvf__locals`, `stg_dvf__parcelles`)
   - Intermediate: join mutation + disposition + local + parcelle into enriched transaction (`int_transactions__enriched`)
   - Marts: `fct_transactions`, `dim_communes`, `dim_property_types`, `dim_dates`
5. **Looker Studio dashboard** — Minimum 2 tiles as per Zoomcamp specs:
   - Tile 1 (categorical distribution): transaction count by property type (maison, appartement, terrain, local, dépendance)
   - Tile 2 (temporal distribution): transaction volume and/or median price evolution by year (2014-2025)
   - Tile 3 (bonus): median price/m² by department
   - All tiles must have clear titles, references (data source, period), and be easy to understand
   - Dashboard accessible via shareable Looker Studio URL (no install needed for reviewers)
6. **Reproducibility** — Complete README with step-by-step instructions, Makefile, Docker for PostgreSQL + Kestra + dbt

### Should Have
7. **4+ dashboard tiles** — add filters (department, year), price/m² distribution, geographic comparison
8. **Data quality tests in dbt** — unique, not_null, accepted_values, relationships
9. **CI-friendly structure** — clear separation of concerns, .env.example

### Could Have
10. **Incremental dbt models** — for efficient future updates
11. **Kestra scheduling** — cron trigger for semiannual updates
12. **Department-level filtering** — ability to run pipeline for specific departments only

---

## 4. TECHNICAL CONSTRAINTS

### Stack

| Component | Technology | Justification |
|-----------|------------|---------------|
| Cloud provider | GCP | Zoomcamp standard, free tier available |
| IaC | Terraform | Zoomcamp Module 1, reproducible infra |
| Data lake | Google Cloud Storage (GCS) | Raw data landing zone, cheap storage |
| Data warehouse | BigQuery | Zoomcamp Module 3, serverless, partitioning/clustering |
| Orchestration | Kestra | Zoomcamp Module 2, DAG-based, Docker-native |
| Transformations | dbt-bigquery | Zoomcamp Module 4, Kimball modeling, testable |
| Dashboard | Looker Studio | Free, native BigQuery connector, shareable URL |
| Containerization | Docker / Docker Compose | Reproducibility for PostgreSQL restore + Kestra + dbt |
| Language | Python 3.11+ | Ingestion scripts, export orchestration |
| Temporary DB | PostgreSQL 16 + PostGIS | Restore SQL dump, export to CSV (ephemeral container) |

### GCP Project
- Project ID: `valeurs-foncieres-analytics` (note: GCP may add a suffix, e.g. `valeurs-foncieres-analytics-12345`)
- Region: `europe-west9` (Paris — closest to data source)

### Infrastructure Estimates
| Resource | Estimate |
|----------|----------|
| GCS storage | ~5-10 GB (exported CSV files) |
| BigQuery storage | ~5-10 GB (raw + staging + marts tables) |
| BigQuery queries | Free tier (1 TB/month) should suffice |
| Terraform state | Local (or GCS backend if preferred) |
| Docker (local) | PostgreSQL container needs ~2-4 GB RAM during restore, ephemeral |
| Local disk (temp) | ~10-15 GB during restore+export phase, cleaned after upload |

### External Requirements (Zoomcamp Evaluation)
| Criterion | Points | Target | Solution |
|-----------|--------|--------|----------|
| Problem description | 4 | Well described, clear problem | Detailed README |
| Cloud | 4 | Cloud + IaC | GCP + Terraform |
| Data ingestion | 4 | End-to-end orchestrated pipeline | Kestra multi-step DAG: download → PG restore → export → GCS → BigQuery |
| Data warehouse | 4 | Partitioned + clustered with explanation | BigQuery: partition by year, cluster by dept + type |
| Transformations | 4 | dbt or similar | dbt-bigquery: multi-table staging → joins → Kimball marts |
| Dashboard | 4 | 2+ tiles | Looker Studio with 3+ tiles |
| Reproducibility | 4 | Clear instructions, easy to run | Makefile + README + Docker + Terraform |

---

## 5. TIMELINE

### Milestones
| Milestone | Target | Deliverable |
|-----------|--------|-------------|
| Infrastructure | Mar 17-21 | Terraform + Docker Compose + GCS + BigQuery ready |
| Ingestion | Mar 24-28 | DVF+ downloaded, restored, exported, in GCS, loaded to BigQuery raw |
| Transformations | Mar 31 — Apr 4 | dbt models: staging → intermediate → marts, tests passing |
| Dashboard | Apr 7-11 | Looker Studio with 2+ tiles connected to marts |
| Documentation | Apr 14-18 | README, reproduction instructions, final polish |
| Buffer | Apr 18-21 | Fix issues, reviewer dry-run, submission |

**Hard deadline**: April 21, 2026 (Zoomcamp final project submission, second window).

### What Must Be Perfect
- README reproduction steps (reviewer experience)
- Pipeline runs end-to-end without manual intervention
- Dashboard is accessible via a shareable link
- Partitioning/clustering explanation is clear and justified

### What Can Be Simplified
- Kestra: a working DAG is enough (no complex scheduling needed)
- Dashboard: 2 solid tiles beat 5 mediocre ones

### Pipeline Execution Modes
The pipeline supports two execution modes via the `DVF_MODE` environment variable:

**`DVF_MODE=demo`** (default — for peer reviewers):
- Downloads only 1-2 departments (~50-100 MB total)
- Full pipeline runs in ~10 minutes on any machine
- Proves the pipeline works end-to-end (reproducibility = 4/4)
- This is what `make run` does out of the box

**`DVF_MODE=full`** (for the project owner):
- Downloads full France SQL dump (~4-5 GB)
- Full pipeline runs in ~1-2 hours
- Populates BigQuery with ~20M transactions
- Looker Studio dashboard points to this full dataset
- Reviewer sees the full dashboard via shareable URL without running full ingestion

**Result for the reviewer**: run `make run` (demo, fast), verify the pipeline works, then click the Looker Studio link to see the dashboard on the full France dataset. Best of both worlds.

---

## 6. NON-FUNCTIONAL REQUIREMENTS

### Performance
- Demo mode pipeline (1-2 departments): < 10 minutes end-to-end
- Full mode pipeline (all France): < 2 hours end-to-end
- PostgreSQL restore: < 30 minutes (full France dump)
- BigQuery queries on marts: < 30 seconds
- Dashboard tiles: < 10 seconds load time

### Security
- No credentials in code or Terraform files
- GCP service account with minimal permissions (Storage Object Admin + BigQuery Data Editor)
- `.env` in `.gitignore`, `.env.example` documents all required variables
- No personal data exposed (DVF is aggregated but addresses can re-identify — don't index publicly)

### Reproducibility (critical for Zoomcamp)
- `terraform apply` provisions all GCP resources
- `make setup` or equivalent bootstraps the environment (pulls Docker images, creates .env)
- `make run` or equivalent triggers the full pipeline (download → restore → export → GCS → BigQuery → dbt)
- README assumes the reviewer has: GCP account, Docker + Docker Compose, Terraform, Make
- All secrets via .env (with .env.example template)
- PostgreSQL container is ephemeral — starts during ingestion, stops after export. No persistent DB to manage.

### Code Quality
- English everywhere (code, comments, docs, commits)
- Type hints in Python
- dbt tests on all models
- Linter clean (ruff)
- No hardcoded project IDs, paths, or credentials

---

## 7. PROJECT STRUCTURE

```
valeurs-foncieres-analytics/
├── terraform/
│   ├── main.tf                 # GCS bucket + BigQuery datasets + service account + IAM
│   ├── variables.tf            # Configurable parameters
│   └── outputs.tf              # Resource references
├── docker-compose.yml          # PostgreSQL (ephemeral) + Kestra v0.21.1
├── .env.example                # Environment variables template (16 variables)
├── Makefile                    # 23 build/pipeline targets
├── CLAUDE.md                   # Project conventions
│
├── docker/
│   └── postgres/
│       └── Dockerfile          # PostgreSQL 16 + PostGIS 3.4
│
├── ingestion/
│   ├── __init__.py
│   ├── config.py               # Shared configuration (loads .env, typed constants)
│   ├── download_dvf.py         # Download DVF+ SQL dump from Cerema
│   ├── restore_dump.py         # Restore SQL dump into PostgreSQL
│   ├── export_tables.py        # Export PostgreSQL tables to CSV
│   ├── download_geojson.py     # Download admin boundary GeoJSON from Etalab
│   ├── upload_to_gcs.py        # Upload CSV + GeoJSON to GCS
│   └── load_to_bigquery.py     # Load from GCS into BigQuery raw tables
│
├── kestra/
│   └── flows/
│       └── dvf_pipeline.yml    # End-to-end DAG (8 tasks)
│
├── dbt_dvf/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── sources.yml
│       ├── staging/            # 6 views
│       ├── intermediate/       # 1 view
│       └── marts/              # 5 tables
│
├── docs/
│   ├── BRIEF.md                # This document
│   ├── DATA_SOURCES.md         # Data reference
│   ├── ARCHITECTURE.md         # Technical architecture
│   ├── PIPELINE.md             # Pipeline documentation
│   ├── PARTITIONING.md         # Partitioning/clustering rationale
│   └── DASHBOARD.md            # Dashboard tile specifications
│
├── tests/                      # Python unit + QA tests
└── requirements.txt
```

---

## 8. ARCHITECTURE SUMMARY

**This is a cloud-native data engineering project** that ingests France's complete real estate transaction dataset (DVF+ SQL dump, 17 relational tables, ~20M transactions) into a modern analytics stack on GCP. The pipeline demonstrates end-to-end data engineering: restoring a relational database, extracting and transforming multi-table data, loading into a cloud data warehouse, and building a dimensional model for analytics.

The pipeline follows the natural data flow:

1. **Infrastructure** -- Terraform provisions GCS + BigQuery datasets + service account
2. **Ingestion** -- Download DVF+ SQL dump, restore into temporary PostgreSQL container (Docker)
3. **Export** -- Extract key tables from PostgreSQL to CSV, upload to GCS (data lake)
4. **Loading** -- Load from GCS into BigQuery raw tables (partitioned + clustered)
5. **Transformation** -- dbt joins mutation + disposition + local + parcelle into staging, intermediate, and mart layers (Kimball star schema)
6. **Dashboard** -- Looker Studio connected to BigQuery marts
7. **Documentation** -- README with complete reproduction instructions

**Dual mode strategy**: `DVF_MODE=demo` (default) runs on 1-2 departments for fast reviewer reproduction. `DVF_MODE=full` runs on all France for the production dashboard. Reviewer runs demo locally, sees full data in Looker Studio link.

**Hard deadline**: April 21, 2026.

**Main constraint**: Everything in English for peer review. Reproducibility is critical -- if a reviewer cannot run it in 30 minutes, points are lost.

**Key architectural decisions**:
- PostgreSQL container is **ephemeral** -- used only during ingestion to restore and export. Not part of the runtime stack.
- PostGIS geometry columns are extracted as lat/lon floats for BigQuery compatibility (BigQuery has no native PostGIS).
- BigQuery partitioned by `year` (from `datemut`) and clustered by `coddep` (department) + `codtypbien` (property type). This optimizes time-series analysis and geographic filtering.
- dbt does the real modeling work: joining 4+ source tables into a clean star schema. This is where the data engineering value lives.
- Kestra orchestrates the full pipeline as a single DAG with 8 tasks, including parallel execution of independent steps.
