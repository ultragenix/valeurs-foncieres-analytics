terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# -----------------------------------------------------------------------------
# GCS bucket -- raw data lake (CSV + GeoJSON landing zone)
# -----------------------------------------------------------------------------
resource "google_storage_bucket" "data_lake" {
  name                        = var.gcs_bucket_name
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
}

# -----------------------------------------------------------------------------
# BigQuery datasets -- raw, staging, analytics
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
resource "google_service_account" "dvf_pipeline" {
  account_id   = "dvf-pipeline"
  display_name = "DVF Pipeline Service Account"
  description  = "Service account for DVF data pipeline (GCS + BigQuery access)"
}

# IAM: Storage Object Admin on the data lake bucket
resource "google_storage_bucket_iam_member" "pipeline_storage_admin" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dvf_pipeline.email}"
}

# IAM: BigQuery Data Editor on the project (for loading and transforming data)
resource "google_project_iam_member" "pipeline_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dvf_pipeline.email}"
}

# IAM: BigQuery Job User on the project (for running queries and load jobs)
resource "google_project_iam_member" "pipeline_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dvf_pipeline.email}"
}
