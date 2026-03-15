# =============================================================================
# DVF+ Analytics — Terraform Variables
# =============================================================================
# These variables configure the GCP infrastructure for the DVF pipeline.
# Required variables (no default) must be provided via terraform.tfvars
# or -var flags. Optional variables have sensible defaults.
# =============================================================================

variable "project_id" {
  description = "GCP project ID where all resources will be created. Find it at https://console.cloud.google.com/. Must match GCP_PROJECT_ID in .env."
  type        = string
}

variable "credentials_file" {
  description = "Relative path to the GCP service account JSON key file. The key must have permissions to create GCS buckets, BigQuery datasets, service accounts, and IAM bindings."
  type        = string
  default     = "../gcp-sa-key.json"
}

variable "region" {
  description = "GCP region for all resources. Default is europe-west9 (Paris), chosen for proximity to the Cerema DVF data source."
  type        = string
  default     = "europe-west9"
}

variable "gcs_bucket_name" {
  description = "Name of the GCS bucket used as the raw data lake. Must be globally unique across all of GCP. Convention: <project-id>-dvf-data-lake."
  type        = string
}

variable "bq_dataset_raw" {
  description = "BigQuery dataset ID for raw tables. Receives CSV data loaded directly from GCS by the ingestion scripts."
  type        = string
  default     = "dvf_raw"
}

variable "bq_dataset_staging" {
  description = "BigQuery dataset ID for dbt staging models. Contains views that clean, cast, and rename raw columns."
  type        = string
  default     = "dvf_staging"
}

variable "bq_dataset_analytics" {
  description = "BigQuery dataset ID for dbt mart models. Contains the Kimball star schema (fct_transactions + dimension tables) used by the Looker Studio dashboard."
  type        = string
  default     = "dvf_analytics"
}
