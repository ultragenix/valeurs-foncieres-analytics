# =============================================================================
# DVF+ Analytics — Makefile
# =============================================================================
# Orchestration shortcuts for the DVF data pipeline.
# Run `make help` to see all available targets.
#
# Quick start:
#   make setup           # One-time: copy .env, install deps, init Terraform
#   make terraform-apply # Provision GCP resources (GCS + BigQuery)
#   make run             # Run the full pipeline (download -> dbt)
#
# The pipeline can run two ways:
#   make run             # Local sequential mode (no Kestra needed)
#   make pipeline        # Via Kestra API (requires `make docker-up-kestra`)
# =============================================================================

# Load .env variables into Make's environment (optional file — no error if missing)
-include .env
export

# Override these paths so all scripts find credentials and dbt profiles
GOOGLE_APPLICATION_CREDENTIALS := $(CURDIR)/gcp-sa-key.json
DBT_PROFILES_DIR := $(CURDIR)/dbt_dvf

.PHONY: help setup terraform-init terraform-plan terraform-apply terraform-destroy \
       docker-up docker-down docker-up-kestra ingest-download ingest-restore \
       ingest-export ingest-geojson ingest-upload ingest-chunked bq-load \
       dbt-deps dbt-run dbt-test dbt-build \
       dashboard-validate check-data \
       run pipeline pipeline-local kestra-deploy test clean

# =============================================================================
# General
# =============================================================================

help: ## Show this help message
	@grep -Eh '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Initial project setup (copy .env, install deps, terraform init)
	@test -f .env || cp .env.example .env
	uv venv
	uv pip install -r requirements.txt
	cd terraform && terraform init

# =============================================================================
# Infrastructure (Terraform — GCS bucket, BigQuery datasets, IAM)
# =============================================================================

terraform-init: ## Initialize Terraform providers
	cd terraform && terraform init

terraform-plan: ## Preview Terraform changes
	cd terraform && terraform plan \
		-var="project_id=$(GCP_PROJECT_ID)" \
		-var="gcs_bucket_name=$(GCS_BUCKET_NAME)"

terraform-apply: ## Apply Terraform changes (provision GCP resources)
	cd terraform && terraform apply \
		-var="project_id=$(GCP_PROJECT_ID)" \
		-var="gcs_bucket_name=$(GCS_BUCKET_NAME)"

terraform-destroy: ## Destroy all Terraform-managed GCP resources
	cd terraform && terraform destroy \
		-var="project_id=$(GCP_PROJECT_ID)" \
		-var="gcs_bucket_name=$(GCS_BUCKET_NAME)"

# =============================================================================
# Docker (PostgreSQL for ingestion, Kestra for orchestration)
# =============================================================================

docker-up: ## Start ephemeral PostgreSQL container
	docker compose up -d postgres

docker-down: ## Stop and remove all containers
	docker compose down

docker-up-kestra: ## Start Kestra orchestrator (+ its internal PostgreSQL)
	docker compose up -d kestra

# =============================================================================
# Ingestion (download, restore, export, upload — individual steps)
# =============================================================================

ingest-download: ## Download DVF+ SQL dump from Cerema
	uv run python -m ingestion.download_dvf

ingest-restore: ## Restore DVF+ SQL dump into PostgreSQL container
	uv run python -m ingestion.restore_dump

ingest-export: ## Export PostgreSQL tables to CSV
	uv run python -m ingestion.export_tables

ingest-geojson: ## Download GeoJSON admin boundaries
	uv run python -m ingestion.download_geojson

ingest-upload: ## Upload CSV + GeoJSON to GCS
	uv run python -m ingestion.upload_to_gcs

ingest-chunked: ## Run chunked full-France ingestion (resumable, crash-safe)
	uv run python -m ingestion.chunked_ingest

# =============================================================================
# BigQuery (load raw data from GCS)
# =============================================================================

bq-load: ## Load CSV + GeoJSON from GCS into BigQuery raw tables
	uv run python -m ingestion.load_to_bigquery

# =============================================================================
# Full Pipeline (end-to-end: download -> dbt)
# =============================================================================

run: pipeline-local ## Run full pipeline (sequential, no Kestra)

pipeline: ## Run pipeline via Kestra API (requires Kestra running)
	@echo "Triggering DVF pipeline via Kestra API..."
	curl -s -X POST http://localhost:$${KESTRA_PORT:-8080}/api/v1/executions/dvf/dvf-pipeline \
		-H "Content-Type: multipart/form-data" \
		-F "mode=$${DVF_MODE:-demo}"
	@echo ""
	@echo "Pipeline triggered. Monitor at http://localhost:$${KESTRA_PORT:-8080}"

check-data: ## Verify DVF+ data is present before running pipeline
	@if find data -name '*.sql' -print -quit 2>/dev/null | grep -q . || ls data/*.7z* 1>/dev/null 2>&1; then \
		echo "DVF+ data found in data/."; \
	else \
		echo ""; \
		echo "ERROR: No DVF+ data found in data/"; \
		echo ""; \
		echo "Download the DVF+ SQL dump manually (no account needed):"; \
		echo "  1. Visit https://cerema.app.box.com/v/dvfplus-opendata/folder/347155412504"; \
		echo "  2. Download La Reunion .7z (38 MB, recommended for review)"; \
		echo "  3. Place it in the data/ directory:"; \
		echo "     mkdir -p data && cp ~/Downloads/DVFPlus_*.7z* data/"; \
		echo ""; \
		echo "See README.md Step 4 for details."; \
		exit 1; \
	fi

pipeline-local: check-data ## Run full pipeline locally (sequential, no Kestra required)
	@echo "=== DVF Pipeline (local sequential mode) ==="
	$(MAKE) ingest-download
	$(MAKE) docker-up
	@echo "Waiting for PostgreSQL to be ready (TCP)..."
	@docker compose exec postgres sh -c 'until pg_isready -h localhost -U $${POSTGRES_USER:-dvf} -d $${POSTGRES_DB:-dvf}; do sleep 1; done'
	$(MAKE) ingest-restore
	$(MAKE) ingest-export
	$(MAKE) ingest-geojson
	$(MAKE) docker-down
	$(MAKE) ingest-upload
	$(MAKE) bq-load
	$(MAKE) dbt-build
	@echo "=== Pipeline complete ==="

# =============================================================================
# Kestra Orchestration (deploy flows, trigger runs)
# =============================================================================

kestra-deploy: ## Deploy flow YAML to Kestra via API
	@echo "Deploying DVF pipeline flow to Kestra..."
	curl -s -X PUT http://localhost:$${KESTRA_PORT:-8080}/api/v1/flows \
		-H "Content-Type: application/x-yaml" \
		-d @kestra/flows/dvf_pipeline.yml
	@echo ""
	@echo "Flow deployed. View at http://localhost:$${KESTRA_PORT:-8080}"

# =============================================================================
# dbt (transformations: staging views, intermediate models, mart tables)
# =============================================================================

dbt-deps: ## Install dbt packages (dbt_utils)
	cd dbt_dvf && uv run dbt deps

dbt-run: ## Run all dbt models (staging + intermediate + marts)
	cd dbt_dvf && uv run dbt run

dbt-test: ## Run all dbt tests
	cd dbt_dvf && uv run dbt test

dbt-build: ## Full dbt workflow: deps + run + test
	cd dbt_dvf && uv run dbt deps && uv run dbt run && uv run dbt test

# =============================================================================
# Validation and Testing
# =============================================================================

dashboard-validate: ## Validate dashboard data by running tile queries against BigQuery
	@echo "=== Tile 1: Transaction Count by Property Type ==="
	bq query --use_legacy_sql=false --project_id=$(shell grep GCP_PROJECT_ID .env | cut -d= -f2 | cut -d'#' -f1 | tr -d ' ') \
		'SELECT property_type_label, COUNT(*) AS cnt FROM dvf_analytics.fct_transactions GROUP BY 1 ORDER BY cnt DESC LIMIT 10'
	@echo ""
	@echo "=== Tile 2: Transaction Volume by Year ==="
	bq query --use_legacy_sql=false --project_id=$(shell grep GCP_PROJECT_ID .env | cut -d= -f2 | cut -d'#' -f1 | tr -d ' ') \
		'SELECT transaction_year, COUNT(*) AS cnt, ROUND(AVG(transaction_price_eur), 0) AS avg_price FROM dvf_analytics.fct_transactions GROUP BY 1 ORDER BY 1'

test: ## Run all tests
	uv run python -m pytest tests/ -v

# =============================================================================
# Cleanup (tear down all local and cloud resources)
# =============================================================================

clean: ## Tear down everything (containers + GCP resources)
	docker compose down -v
	cd terraform && terraform destroy -auto-approve \
		-var="project_id=$(GCP_PROJECT_ID)" \
		-var="gcs_bucket_name=$(GCS_BUCKET_NAME)"
