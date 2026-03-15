# =============================================================================
# DVF+ Analytics — GCP Infrastructure (Terraform)
# =============================================================================
# Provisions all cloud resources for the DVF data pipeline:
#   - GCS bucket:       raw data lake (CSV + GeoJSON landing zone)
#   - BigQuery datasets: raw, staging (dbt views), analytics (dbt marts)
#   - Service account:  shared identity for ingestion scripts, dbt, and Kestra
#   - IAM bindings:     least-privilege access (Storage + BigQuery only)
#
# Usage:
#   cd terraform
#   terraform init
#   terraform plan
#   terraform apply
# =============================================================================

terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# Authenticate using a service account JSON key file.
# The key path is defined in variables.tf (default: ../gcp-sa-key.json).
provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials_file)
}

# -----------------------------------------------------------------------------
# GCS bucket -- raw data lake (CSV + GeoJSON landing zone)
# -----------------------------------------------------------------------------
resource "google_storage_bucket" "data_lake" {
  name                        = var.gcs_bucket_name
  location                    = var.region
  storage_class               = "STANDARD"       # Standard tier — adequate for batch ingestion workloads
  uniform_bucket_level_access = true              # Simplify ACLs: IAM-only access control (no per-object ACLs)
  force_destroy               = true              # Allow terraform destroy even if bucket contains objects
}

# -----------------------------------------------------------------------------
# BigQuery datasets -- raw, staging, analytics
# -----------------------------------------------------------------------------
# Three-layer architecture following the ELT pattern:
#   raw       -> CSV data loaded as-is from GCS (source of truth)
#   staging   -> dbt views that clean, cast, and rename raw columns
#   analytics -> dbt mart tables in a Kimball star schema (facts + dimensions)
#
# All datasets use delete_contents_on_destroy = true so that
# `terraform destroy` can clean up without manual table deletion.
# -----------------------------------------------------------------------------

resource "google_bigquery_dataset" "raw" {
  dataset_id                 = var.bq_dataset_raw
  friendly_name              = "DVF Raw Data"
  description                = "Raw DVF+ tables loaded from GCS CSV files"
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "staging" {
  dataset_id                 = var.bq_dataset_staging
  friendly_name              = "DVF Staging"
  description                = "dbt staging views -- cleaned individual source tables"
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id                 = var.bq_dataset_analytics
  friendly_name              = "DVF Analytics"
  description                = "dbt mart tables -- Kimball star schema (facts + dimensions)"
  location                   = var.region
  delete_contents_on_destroy = true
}

# -----------------------------------------------------------------------------
# Service account -- used by ingestion scripts, dbt, and Kestra
# -----------------------------------------------------------------------------
# A single service account is shared across all pipeline components.
# The JSON key for this account must be downloaded manually from the GCP
# console and saved as gcp-sa-key.json in the project root (see README).
# -----------------------------------------------------------------------------

resource "google_service_account" "dvf_pipeline" {
  account_id   = "dvf-pipeline"
  display_name = "DVF Pipeline Service Account"
  description  = "Service account for DVF data pipeline (GCS + BigQuery access)"
}

# -----------------------------------------------------------------------------
# IAM bindings -- least-privilege access for the pipeline service account
# -----------------------------------------------------------------------------
# Three roles are required:
#   1. Storage Object Admin  -> upload/overwrite CSV and GeoJSON in the bucket
#   2. BigQuery Data Editor  -> create/write tables in all three datasets
#   3. BigQuery Job User     -> run load jobs and dbt queries
# -----------------------------------------------------------------------------

# IAM: Storage Object Admin on the data lake bucket
# Grants: create, read, update, delete objects in the GCS bucket only.
resource "google_storage_bucket_iam_member" "pipeline_storage_admin" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dvf_pipeline.email}"
}

# IAM: BigQuery Data Editor on the project (for loading and transforming data)
# Grants: create/update/delete tables and views across all datasets.
resource "google_project_iam_member" "pipeline_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dvf_pipeline.email}"
}

# IAM: BigQuery Job User on the project (for running queries and load jobs)
# Grants: submit query and load jobs. Required by both ingestion and dbt.
resource "google_project_iam_member" "pipeline_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dvf_pipeline.email}"
}
