.PHONY: help setup terraform-init terraform-plan terraform-apply terraform-destroy \
       docker-up docker-down docker-up-kestra ingest-download ingest-restore \
       run dbt-run test clean

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

run: ## Run full pipeline (placeholder -- filled in later parts)
	@echo "Pipeline not yet implemented. See PLAN.md for progress."

dbt-run: ## Run dbt transformations (placeholder -- filled in later parts)
	@echo "dbt not yet configured. See Part 5 in PLAN.md."

test: ## Run all tests (placeholder -- filled in later parts)
	@echo "Tests not yet implemented. See PLAN.md for progress."

clean: ## Tear down everything (containers + GCP resources)
	docker compose down -v
	cd terraform && terraform destroy -auto-approve
