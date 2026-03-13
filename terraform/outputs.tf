output "gcs_bucket_url" {
  description = "GCS bucket URL for the raw data lake"
  value       = "gs://${google_storage_bucket.data_lake.name}"
}

output "bq_dataset_raw_id" {
  description = "BigQuery raw dataset fully-qualified ID"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "bq_dataset_staging_id" {
  description = "BigQuery staging dataset fully-qualified ID"
  value       = google_bigquery_dataset.staging.dataset_id
}

output "bq_dataset_analytics_id" {
  description = "BigQuery analytics dataset fully-qualified ID"
  value       = google_bigquery_dataset.analytics.dataset_id
}

output "service_account_email" {
  description = "Service account email for the DVF pipeline"
  value       = google_service_account.dvf_pipeline.email
}
