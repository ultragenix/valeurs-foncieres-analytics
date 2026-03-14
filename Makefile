.PHONY: help setup terraform-init terraform-plan terraform-apply terraform-destroy \
       docker-up docker-down docker-up-kestra ingest-download ingest-restore \
       ingest-export ingest-geojson ingest-upload bq-load \
       dbt-deps dbt-seed dbt-run dbt-test dbt-build \
       run test clean

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Initial project setup (copy .env, install deps, terraform init)
	@test -f .env || cp .env.example .env
	uv pip install -r requirements.txt
	cd terraform && terraform init

terraform-init: ## Initialize Terraform providers
	cd terraform && terraform init

terraform-plan: ## Preview Terraform changes
	cd terraform && terraform plan

terraform-apply: ## Apply Terraform changes (provision GCP resources)
	cd terraform && terraform apply

terraform-destroy: ## Destroy all Terraform-managed GCP resources
	cd terraform && terraform destroy

docker-up: ## Start ephemeral PostgreSQL container
	docker compose up -d postgres

docker-down: ## Stop and remove all containers
	docker compose down

docker-up-kestra: ## Start Kestra orchestrator (+ its internal PostgreSQL)
	docker compose up -d kestra

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

bq-load: ## Load CSV + GeoJSON from GCS into BigQuery raw tables
	uv run python -m ingestion.load_to_bigquery

run: ## Run full pipeline (placeholder -- filled in later parts)
	@echo "Pipeline not yet implemented. See PLAN.md for progress."

dbt-deps: ## Install dbt packages (dbt_utils)
	cd dbt_dvf && uv run dbt deps

dbt-seed: ## Load seed reference data into BigQuery
	cd dbt_dvf && uv run dbt seed

dbt-run: ## Run all dbt models (staging + intermediate + marts)
	cd dbt_dvf && uv run dbt run

dbt-test: ## Run all dbt tests
	cd dbt_dvf && uv run dbt test

dbt-build: ## Full dbt workflow: deps + seed + run + test
	cd dbt_dvf && uv run dbt deps && uv run dbt seed && uv run dbt run && uv run dbt test

test: ## Run all tests
	uv run python -m pytest tests/ -v

clean: ## Tear down everything (containers + GCP resources)
	docker compose down -v
	cd terraform && terraform destroy -auto-approve
