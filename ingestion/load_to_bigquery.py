"""Load CSV and GeoJSON files from GCS into BigQuery raw tables.

Loads DVF+ CSV exports into ``dvf_raw`` with autodetect schema.  The
``mutation`` table gets integer-range partitioning on ``anneemut`` and
clustering on ``coddep`` + ``codtypbien``.  GeoJSON administrative
boundary files are parsed and loaded as BigQuery tables with geometry
stored as a JSON string (converted to GEOGRAPHY in the dbt layer).
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import PurePosixPath
from typing import Any

from ingestion.config import (
    BQ_DATASET_RAW,
    GCP_PROJECT_ID,
    GCS_BUCKET_NAME,
    GCS_DVF_PREFIX,
    GCS_GEOJSON_PREFIX,
    get_gcs_client,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

MUTATION_TABLE: str = "mutation"
PARTITION_FIELD: str = "anneemut"
PARTITION_START: int = 2014
PARTITION_END: int = 2026
PARTITION_INTERVAL: int = 1
CLUSTERING_FIELDS: list[str] = ["coddep", "codtypbien"]

GEO_DEPARTMENTS_TABLE: str = "geo_departments"
GEO_COMMUNES_TABLE: str = "geo_communes"

GEOJSON_FILE_MAP: dict[str, str] = {
    "departements-1000m.geojson": GEO_DEPARTMENTS_TABLE,
    "departments.geojson": GEO_DEPARTMENTS_TABLE,
    "communes-1000m.geojson": GEO_COMMUNES_TABLE,
    "communes.geojson": GEO_COMMUNES_TABLE,
}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def _validate_config() -> bool:
    """Check that required GCP config values are set. Return True if valid."""
    if not GCP_PROJECT_ID:
        logger.error("GCP_PROJECT_ID is not configured.")
        return False
    if not GCS_BUCKET_NAME:
        logger.error("GCS_BUCKET_NAME is not configured.")
        return False
    if not BQ_DATASET_RAW:
        logger.error("BQ_DATASET_RAW is not configured.")
        return False
    return True


# ---------------------------------------------------------------------------
# Naming helpers
# ---------------------------------------------------------------------------
def _table_id(table_name: str) -> str:
    """Return the fully-qualified BigQuery table id."""
    return f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.{table_name}"


def _table_name_from_blob(blob: Any) -> str:
    """Extract the BigQuery table name from a GCS blob path."""
    return PurePosixPath(blob.name).stem


def _resolve_geojson_table(blob: Any) -> str | None:
    """Map a GeoJSON blob name to its BigQuery target table, or None."""
    filename = PurePosixPath(blob.name).name
    return GEOJSON_FILE_MAP.get(filename)


# ---------------------------------------------------------------------------
# GeoJSON parsing
# ---------------------------------------------------------------------------
def _extract_feature_row(feature: dict[str, Any]) -> dict[str, Any]:
    """Convert a single GeoJSON feature into a flat dict row."""
    row: dict[str, Any] = {}
    properties = feature.get("properties") or {}
    row.update(properties)
    geometry = feature.get("geometry")
    if geometry:
        row["geometry"] = json.dumps(geometry)
    return row


def _extract_rows_from_geojson(
    geojson: dict[str, Any],
) -> list[dict[str, Any]]:
    """Extract all features from a GeoJSON FeatureCollection as rows."""
    features = geojson.get("features", [])
    return [_extract_feature_row(f) for f in features]


def _download_geojson_blob(blob: Any) -> dict[str, Any]:
    """Download a GeoJSON blob and parse it as JSON."""
    text = blob.download_as_text()
    return json.loads(text)


# ---------------------------------------------------------------------------
# BigQuery clients (lazy)
# ---------------------------------------------------------------------------
def _get_bq_client() -> Any:
    """Create and return a BigQuery client."""
    from google.cloud import bigquery  # noqa: WPS433

    return bigquery.Client(project=GCP_PROJECT_ID)


def _get_gcs_client() -> Any:
    """Create and return a GCS storage client."""
    return get_gcs_client()


# ---------------------------------------------------------------------------
# Job config builders
# ---------------------------------------------------------------------------
def _build_geojson_schema() -> list[Any]:
    """Return a minimal schema with a geometry STRING field."""
    from google.cloud import bigquery  # noqa: WPS433

    return [bigquery.SchemaField("geometry", "STRING", mode="NULLABLE")]


def _build_csv_config(table_name: str) -> Any:
    """Build a LoadJobConfig for CSV loading.

    The mutation table gets range partitioning and clustering.
    """
    from google.cloud import bigquery  # noqa: WPS433

    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    if table_name == MUTATION_TABLE:
        config.range_partitioning = bigquery.RangePartitioning(
            field=PARTITION_FIELD,
            range_=bigquery.PartitionRange(
                start=PARTITION_START,
                end=PARTITION_END,
                interval=PARTITION_INTERVAL,
            ),
        )
        config.clustering_fields = CLUSTERING_FIELDS
    return config


def _build_geojson_config() -> Any:
    """Build a LoadJobConfig for GeoJSON (newline-delimited JSON) loading."""
    from google.cloud import bigquery  # noqa: WPS433

    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=_build_geojson_schema(),
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )


# ---------------------------------------------------------------------------
# Blob listing
# ---------------------------------------------------------------------------
def _list_csv_blobs(client: Any) -> list[Any]:
    """List CSV blobs under the DVF prefix in GCS."""
    bucket = client.bucket(GCS_BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=GCS_DVF_PREFIX)
    return [b for b in blobs if b.name.endswith(".csv")]


def _list_geojson_blobs(client: Any) -> list[Any]:
    """List GeoJSON blobs under the geojson prefix in GCS."""
    bucket = client.bucket(GCS_BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=GCS_GEOJSON_PREFIX)
    return [b for b in blobs if b.name.endswith(".geojson")]


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------
def _load_csv_blob(bq_client: Any, blob: Any) -> int:
    """Load a single CSV blob into BigQuery. Returns row count."""
    table_name = _table_name_from_blob(blob)
    uri = f"gs://{GCS_BUCKET_NAME}/{blob.name}"
    dest = _table_id(table_name)
    config = _build_csv_config(table_name)

    logger.info("Loading %s -> %s", uri, dest)
    job = bq_client.load_table_from_uri(uri, dest, job_config=config)
    job.result()

    table = bq_client.get_table(dest)
    logger.info("Loaded %s: %s rows", table_name, f"{table.num_rows:,}")
    return table.num_rows


def _load_geojson_blob(bq_client: Any, blob: Any) -> int:
    """Load a single GeoJSON blob into BigQuery. Returns row count."""
    table_name = _resolve_geojson_table(blob)
    if table_name is None:
        logger.warning("Unknown GeoJSON file: %s -- skipping.", blob.name)
        return 0

    geojson = _download_geojson_blob(blob)
    rows = _extract_rows_from_geojson(geojson)
    if not rows:
        logger.warning("No features in %s -- skipping.", blob.name)
        return 0

    dest = _table_id(table_name)
    config = _build_geojson_config()

    logger.info("Loading %d features -> %s", len(rows), dest)
    job = bq_client.load_table_from_json(rows, dest, job_config=config)
    job.result()

    table = bq_client.get_table(dest)
    logger.info("Loaded %s: %s rows", table_name, f"{table.num_rows:,}")
    return table.num_rows


def _load_all_csvs(bq_client: Any, blobs: list[Any]) -> int:
    """Load all CSV blobs into BigQuery. Returns total row count."""
    total = 0
    for blob in blobs:
        total += _load_csv_blob(bq_client, blob)
    return total


def _load_all_geojson(bq_client: Any, blobs: list[Any]) -> int:
    """Load all GeoJSON blobs into BigQuery. Returns total row count."""
    total = 0
    for blob in blobs:
        total += _load_geojson_blob(bq_client, blob)
    return total


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def _discover_blobs(
    gcs_client: Any,
) -> tuple[list[Any], list[Any]]:
    """Discover CSV and GeoJSON blobs in GCS. Returns (csv, geojson)."""
    csv_blobs = _list_csv_blobs(gcs_client)
    geojson_blobs = _list_geojson_blobs(gcs_client)
    logger.info(
        "Found %d CSV blob(s) and %d GeoJSON blob(s) to load.",
        len(csv_blobs),
        len(geojson_blobs),
    )
    return csv_blobs, geojson_blobs


def load_to_bigquery() -> int:
    """Load all GCS data into BigQuery raw tables. Returns total row count."""
    if not _validate_config():
        return 0

    gcs_client = _get_gcs_client()
    bq_client = _get_bq_client()
    csv_blobs, geojson_blobs = _discover_blobs(gcs_client)

    if len(csv_blobs) + len(geojson_blobs) == 0:
        logger.warning("No blobs found in GCS bucket %s.", GCS_BUCKET_NAME)
        return 0

    total_rows = _load_all_csvs(bq_client, csv_blobs)
    total_rows += _load_all_geojson(bq_client, geojson_blobs)
    logger.info("BigQuery load complete: %s total rows.", f"{total_rows:,}")
    return total_rows


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s -- %(message)s",
    )
    count = load_to_bigquery()
    if count == 0:
        logger.error("No data was loaded into BigQuery.")
        sys.exit(1)
    logger.info("Successfully loaded %s rows.", f"{count:,}")


if __name__ == "__main__":
    main()
