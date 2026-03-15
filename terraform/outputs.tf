# =============================================================================
# DVF+ Analytics — Terraform Outputs
# =============================================================================
# These outputs are displayed after `terraform apply` and can be referenced
# by other Terraform modules or used to verify that provisioning succeeded.
# =============================================================================

output "gcs_bucket_url" {
  description = "GCS bucket URL (gs:// format) for the raw data lake. Use this in ingestion scripts to upload CSV and GeoJSON files."
  value       = "gs://${google_storage_bucket.data_lake.name}"
}

output "bq_dataset_raw_id" {
  description = "BigQuery raw dataset ID. Ingestion scripts load CSV data into tables in this dataset."
  value       = google_bigquery_dataset.raw.dataset_id
}

output "bq_dataset_staging_id" {
  description = "BigQuery staging dataset ID. dbt creates cleaning views in this dataset."
  value       = google_bigquery_dataset.staging.dataset_id
}

output "bq_dataset_analytics_id" {
  description = "BigQuery analytics (mart) dataset ID. dbt creates the Kimball star schema tables here. The Looker Studio dashboard reads from this dataset."
  value       = google_bigquery_dataset.analytics.dataset_id
}

output "service_account_email" {
  description = "Email of the pipeline service account. Download its JSON key from the GCP console and save as gcp-sa-key.json in the project root."
  value       = google_service_account.dvf_pipeline.email
}
