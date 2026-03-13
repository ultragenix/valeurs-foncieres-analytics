variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west9"
}

variable "gcs_bucket_name" {
  description = "GCS bucket for raw data lake"
  type        = string
}

variable "bq_dataset_raw" {
  description = "BigQuery dataset for raw tables"
  type        = string
  default     = "dvf_raw"
}

variable "bq_dataset_staging" {
  description = "BigQuery dataset for dbt staging"
  type        = string
  default     = "dvf_staging"
}

variable "bq_dataset_analytics" {
  description = "BigQuery dataset for dbt marts"
  type        = string
  default     = "dvf_analytics"
}
