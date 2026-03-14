# Pipeline Documentation

Data pipeline for DVF+ France real estate transactions. This document covers all implemented pipeline steps (Parts 2--5). Kestra orchestration (Part 7) will wrap these steps into a single DAG.

## Pipeline Overview

```
Step 1          Step 2           Step 3          Step 4         Step 5         Step 6         Step 7
download_dvf -> restore_dump --> export_tables -> download_geo   upload_gcs --> bq_load -----> dbt_build
(HTTP)          (psql)           (COPY TO CSV)   (HTTP)          (GCS API)     (BQ API)       (dbt-bigquery)
                     |                                |              ^             |              |
                     v                                v              |             v              v
               PostgreSQL                        data/geojson/      |       BigQuery        BigQuery
               (ephemeral)                                          |       dvf_raw         dvf_staging
                     |                                              |                      dvf_analytics
                     v                                              |                          |
               data/export/*.csv -----------------------------------+                          v
                                                                                        Looker Studio
```

## Step Details

### Step 1: Download DVF+ SQL Dump

| Attribute | Value |
|-----------|-------|
| Script | `ingestion/download_dvf.py` |
| Makefile target | `make ingest-download` |
| Input | Cerema Box URL (HTTPS) |
| Output | `.sql` files in `data/` |
| Duration | ~2--5 minutes (full), ~1 minute (demo) |
| Idempotent | Yes -- skips download if `.sql` file already exists |

**Command:**

```bash
uv run python -m ingestion.download_dvf
```

The script first tries the data.gouv.fr redirect URL, then falls back to Cerema Box. The downloaded `.7z` archive is extracted to produce `.sql` files. For manual downloads, pass `--file path/to/file.7z` or `--file path/to/file.sql`.

**Dependencies:** Internet access to `data.gouv.fr` or `cerema.app.box.com`.

### Step 2: Restore SQL Dump

| Attribute | Value |
|-----------|-------|
| Script | `ingestion/restore_dump.py` |
| Makefile target | `make ingest-restore` |
| Input | `.sql` files in `data/` |
| Output | Populated PostgreSQL tables |
| Duration | ~5--30 minutes depending on dump size |
| Idempotent | Yes -- uses `DROP IF EXISTS` in SQL |

**Prerequisites:** PostgreSQL container must be running (`make docker-up`).

**Command:**

```bash
uv run python -m ingestion.restore_dump
```

Executes each `.sql` file via `psql` subprocess. After restore, verifies that principal tables (`mutation`, `disposition`, `disposition_parcelle`, `local`) exist and logs row counts. In demo mode, deletes rows outside configured departments from all tables with a `coddep` column.

**Dependencies:** Running PostgreSQL container on `localhost:5432`.

### Step 3: Export Tables to CSV

| Attribute | Value |
|-----------|-------|
| Script | `ingestion/export_tables.py` |
| Makefile target | `make ingest-export` |
| Input | PostgreSQL tables |
| Output | 11 CSV files in `data/export/` |
| Duration | ~2--10 minutes |
| Idempotent | Yes -- overwrites existing CSV files |

**Prerequisites:** PostgreSQL container running with restored data (Steps 1--2).

**Command:**

```bash
uv run python -m ingestion.export_tables
```

Exports 11 tables with special handling:
- Geometry point columns extracted as `latitude`/`longitude` floats
- Polygon geometry columns dropped
- Array columns reduced to first element
- Simple tables (no special columns) use plain `COPY TO`

**Tables exported:** `mutation`, `disposition`, `local`, `disposition_parcelle`, `parcelle`, `adresse`, `ann_nature_mutation`, `ann_type_local`, `ann_cgi`, `ann_nature_culture`, `ann_nature_culture_speciale`

**Dependencies:** Running PostgreSQL container with restored data.

### Step 4: Download GeoJSON Boundaries

| Attribute | Value |
|-----------|-------|
| Script | `ingestion/download_geojson.py` |
| Makefile target | `make ingest-geojson` |
| Input | Etalab URLs (HTTPS) |
| Output | 2 GeoJSON files in `data/geojson/` |
| Duration | ~30 seconds |
| Idempotent | Yes -- skips download if files exist and are valid |

**Command:**

```bash
uv run python -m ingestion.download_geojson
```

Downloads `departements-1000m.geojson` (~340 KB) and `communes-1000m.geojson` (~10 MB) from Etalab's 2024 administrative boundary dataset.

**Dependencies:** Internet access to `etalab-datasets.geo.data.gouv.fr`. No PostgreSQL dependency -- can run in parallel with Steps 1--3.

### Step 5: Upload to GCS

| Attribute | Value |
|-----------|-------|
| Script | `ingestion/upload_to_gcs.py` |
| Makefile target | `make ingest-upload` |
| Input | `data/export/*.csv` + `data/geojson/*.geojson` |
| Output | Files in `gs://<bucket>/raw/dvf/` and `gs://<bucket>/raw/geojson/` |
| Duration | ~1--5 minutes |
| Idempotent | Yes -- overwrites existing blobs |

**Prerequisites:** Steps 3 and 4 complete. GCS bucket provisioned (`make terraform-apply`).

**Command:**

```bash
uv run python -m ingestion.upload_to_gcs
```

Uploads CSV files to `raw/dvf/` prefix and GeoJSON files to `raw/geojson/` prefix in the configured GCS bucket.

**Dependencies:** GCS bucket exists, `GOOGLE_APPLICATION_CREDENTIALS` set, `GCS_BUCKET_NAME` configured.

### Step 6: Load into BigQuery

| Attribute | Value |
|-----------|-------|
| Script | `ingestion/load_to_bigquery.py` |
| Makefile target | `make bq-load` |
| Input | GCS bucket contents (`raw/dvf/*.csv` + `raw/geojson/*.geojson`) |
| Output | 13 tables in `dvf_raw` BigQuery dataset |
| Duration | ~1--5 minutes |
| Idempotent | Yes -- uses `WRITE_TRUNCATE` (replaces table contents on each run) |

**Prerequisites:** Step 5 complete. BigQuery datasets provisioned (`make terraform-apply`).

**Command:**

```bash
uv run python -m ingestion.load_to_bigquery
```

**CSV loading:**
- Discovers all `.csv` blobs under `raw/dvf/` prefix
- Table name = filename stem (e.g., `mutation.csv` becomes `dvf_raw.mutation`)
- Schema autodetected by BigQuery
- The `mutation` table is partitioned by `anneemut` (integer range, 2014--2026) and clustered by `coddep`, `codtypbien`

**GeoJSON loading:**
- Discovers all `.geojson` blobs under `raw/geojson/` prefix
- Parses FeatureCollection, flattens each feature to a row (properties + geometry as JSON string)
- Maps filenames to tables: `departements-1000m.geojson` to `geo_departments`, `communes-1000m.geojson` to `geo_communes`
- Geometry stored as STRING -- conversion to GEOGRAPHY type happens in dbt staging layer

**Dependencies:** BigQuery datasets exist, GCS bucket contains data, `GCP_PROJECT_ID` and `BQ_DATASET_RAW` configured.

### Step 7: dbt Transformations

| Attribute | Value |
|-----------|-------|
| Tool | dbt-core + dbt-bigquery |
| Makefile target | `make dbt-build` (deps + run + test) |
| Input | BigQuery `dvf_raw` tables (from Step 6) |
| Output | 6 views in `dvf_staging`, 5 tables in `dvf_analytics` |
| Duration | ~2--5 minutes |
| Idempotent | Yes -- views are recreated, tables use `CREATE OR REPLACE` |

**Prerequisites:** Step 6 complete. BigQuery datasets provisioned. `DBT_PROFILES_DIR` set in `.env`.

**Commands:**

```bash
# Full workflow (recommended)
make dbt-build

# Individual steps
make dbt-deps    # Install dbt packages (dbt_utils)
make dbt-run     # Run all 12 dbt models
make dbt-test    # Run all 62 dbt tests
```

**Model execution order** (managed by dbt's dependency graph):

1. **Staging views** (6 models, in parallel): `stg_dvf__mutations`, `stg_dvf__dispositions`, `stg_dvf__locals`, `stg_dvf__parcelles`, `stg_geo__departments`, `stg_geo__communes`
2. **Intermediate view** (1 model): `int_transactions__enriched` -- depends on 4 staging models
3. **Mart tables** (5 models): `fct_transactions`, `dim_communes`, `dim_property_types`, `dim_dates`, `dim_geography` -- depend on staging and/or intermediate models

**dbt data tests** (62 tests): unique, not_null, relationships, accepted_values, expression_is_true. All tests run automatically during `make dbt-build`.

**Dependencies:** BigQuery raw tables loaded, `GOOGLE_APPLICATION_CREDENTIALS` set, `GCP_PROJECT_ID` set, `DBT_PROFILES_DIR` set.

## Step Dependencies

```
Step 1 (download_dvf) ----> Step 2 (restore_dump) ----> Step 3 (export_tables) -+
                                                                                 |
Step 4 (download_geojson) --------------------------------------------------+   |
                                                                            |   |
                                                                            v   v
                                                                    Step 5 (upload_gcs)
                                                                            |
                                                                            v
                                                                    Step 6 (bq_load)
                                                                            |
                                                                            v
                                                                    Step 7 (dbt_build)
                                                                            |
                                                                            v
                                                                    Looker Studio
```

Steps 1--3 are sequential (each depends on the previous). Step 4 is independent and can run in parallel with Steps 1--3. Step 5 requires both Step 3 and Step 4 to complete. Step 6 requires Step 5. Step 7 requires Step 6.

## Error Handling

Each step implements the following error handling patterns:

| Pattern | Implementation |
|---------|---------------|
| Config validation | Each script validates required environment variables before starting |
| Exit codes | All scripts exit with code 1 on failure, 0 on success |
| Logging | All scripts use Python `logging` module (no `print` statements) |
| Idempotency | All steps can be re-run safely (overwrite mode) |
| Progress bars | Long-running steps display `tqdm` progress bars |
| Partial failure | BigQuery loading processes all blobs and reports total; GeoJSON with no features is skipped with a warning |
| dbt tests | dbt reports test failures with details; `make dbt-build` stops on test failure |

## Running the Full Pipeline (Manual)

Until Kestra orchestration is implemented (Part 7), run the pipeline manually:

```bash
# 1. Provision infrastructure (one-time)
make setup
make terraform-apply

# 2. Start ephemeral PostgreSQL
make docker-up

# 3. Ingest data (sequential steps)
make ingest-download
make ingest-restore
make ingest-export
make ingest-geojson
make ingest-upload

# 4. Stop PostgreSQL (no longer needed)
make docker-down

# 5. Load into BigQuery
make bq-load

# 6. Run dbt transformations
make dbt-build

# 7. Validate dashboard data
make dashboard-validate
```

## Planned: Kestra Orchestration (Part 7)

The Kestra DAG (`kestra/flows/dvf_pipeline.yml`) will wrap all steps into a single orchestrated pipeline with:
- Sequential task execution following the dependency graph
- Error handling and retry logic
- Logging and monitoring via Kestra UI
- Trigger support (manual and scheduled)

The DAG will be accessible via the Kestra web UI at `http://localhost:8080` (after `make docker-up-kestra`).
