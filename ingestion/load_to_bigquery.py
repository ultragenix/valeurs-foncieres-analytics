"""Load CSV and GeoJSON files from GCS into BigQuery raw tables.

Loads DVF+ CSV exports from GCS into the ``dvf_raw`` BigQuery dataset.
The ``mutation`` table gets integer-range partitioning on ``anneemut``
and clustering on ``coddep`` + ``codtypbien`` for query performance.
GeoJSON administrative boundary files are parsed and loaded as BigQuery
tables with geometry stored as a JSON string (converted to GEOGRAPHY
downstream in the dbt staging layer).

Supports both flat layout (``raw/dvf/mutation.csv``) and chunked layout
(``raw/dvf/mutation/chunk_001.csv``). Chunked tables are loaded via
BigQuery wildcard URIs with an explicit all-STRING schema to avoid
autodetect type conflicts across files (e.g. Corsican department codes
``2A``/``2B`` vs numeric codes).

Inputs:
    - CSV objects in ``gs://bucket/raw/dvf/``.
    - GeoJSON objects in ``gs://bucket/raw/geojson/``.
Outputs:
    - BigQuery tables in the ``dvf_raw`` dataset.

Dependencies:
    google-cloud-bigquery  -- for BigQuery load jobs.
    google-cloud-storage   -- for listing and reading GCS blobs.
"""

from __future__ import annotations

import datetime
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
    setup_logging,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

MUTATION_TABLE: str = "mutation"
PARTITION_FIELD: str = "anneemut"
PARTITION_START: int = 2014
PARTITION_END: int = datetime.datetime.now(tz=datetime.timezone.utc).year + 1
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
    """Check that required GCP config values are set.

    Verifies ``GCP_PROJECT_ID``, ``GCS_BUCKET_NAME``, and
    ``BQ_DATASET_RAW`` are non-empty.

    Returns:
        True if all required values are configured, False otherwise.
    """
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
    """Return the fully-qualified BigQuery table ID.

    Args:
        table_name: Unqualified table name (e.g. ``mutation``).

    Returns:
        String in ``project.dataset.table`` format.
    """
    return f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.{table_name}"


def _table_name_from_blob(blob: Any) -> str:
    """Extract the BigQuery table name from a GCS blob path.

    Uses the file stem (e.g. ``mutation`` from ``raw/dvf/mutation.csv``).

    Args:
        blob: GCS ``Blob`` object.

    Returns:
        Table name string.
    """
    return PurePosixPath(blob.name).stem


def _resolve_geojson_table(blob: Any) -> str | None:
    """Map a GeoJSON blob name to its BigQuery target table.

    Args:
        blob: GCS ``Blob`` object.

    Returns:
        Target table name from ``GEOJSON_FILE_MAP``, or ``None`` if the
        filename is not recognized.
    """
    filename = PurePosixPath(blob.name).name
    return GEOJSON_FILE_MAP.get(filename)


# ---------------------------------------------------------------------------
# GeoJSON parsing
# ---------------------------------------------------------------------------
def _extract_feature_row(feature: dict[str, Any]) -> dict[str, Any]:
    """Convert a single GeoJSON feature into a flat dict row.

    Properties are flattened to top-level keys. The ``geometry`` field
    is serialized as a JSON string for BigQuery ingestion.

    Args:
        feature: A GeoJSON Feature dict.

    Returns:
        Flat dict with property keys plus an optional ``geometry`` key.
    """
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
    """Extract all features from a GeoJSON FeatureCollection as flat rows.

    Args:
        geojson: Parsed GeoJSON dict (expected to be a FeatureCollection).

    Returns:
        List of flat dicts, one per Feature.
    """
    features = geojson.get("features", [])
    return [_extract_feature_row(f) for f in features]


def _download_geojson_blob(blob: Any) -> dict[str, Any]:
    """Download a GeoJSON blob and parse it as JSON.

    Args:
        blob: GCS ``Blob`` object.

    Returns:
        Parsed JSON dict (GeoJSON FeatureCollection).
    """
    text = blob.download_as_text()
    return json.loads(text)


# ---------------------------------------------------------------------------
# BigQuery clients (lazy)
# ---------------------------------------------------------------------------
def _get_bq_client() -> Any:
    """Create and return a BigQuery client.

    Returns:
        A ``google.cloud.bigquery.Client`` instance (typed as ``Any``
        to avoid import-time dependency).
    """
    from google.cloud import bigquery  # noqa: WPS433

    return bigquery.Client(project=GCP_PROJECT_ID)


# ---------------------------------------------------------------------------
# Job config builders
# ---------------------------------------------------------------------------
def _build_geojson_schema() -> list[Any]:
    """Return a BigQuery schema for GeoJSON features with all STRING fields.

    All property fields are forced to STRING to prevent autodetect from
    misinterpreting codes like ``2A001`` (Corse) as integers.

    Returns:
        List of ``bigquery.SchemaField`` instances covering the expected
        GeoJSON property columns plus ``geometry``.
    """
    from google.cloud import bigquery  # noqa: WPS433

    return [
        bigquery.SchemaField("code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("nom", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("departement", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("epci", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("geometry", "STRING", mode="NULLABLE"),
    ]


HEADER_READ_BYTES: int = 4096


def _read_csv_header_from_gcs(gcs_client: Any, blob_name: str) -> list[str]:
    """Read the first line of a CSV blob to extract column names.

    Downloads only the first ``HEADER_READ_BYTES`` bytes instead of
    the entire blob to avoid loading multi-GB files into memory.

    Args:
        gcs_client: GCS ``Client`` instance.
        blob_name: Full GCS blob path (e.g. ``raw/dvf/mutation/chunk_001.csv``).

    Returns:
        List of column name strings parsed from the CSV header.
    """
    bucket = gcs_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(blob_name)
    chunk = blob.download_as_text(start=0, end=HEADER_READ_BYTES)
    first_line = chunk.split("\n", 1)[0].strip()
    return [col.strip().strip('"') for col in first_line.split(",")]


# Column forced to INT64 for range partitioning on the mutation table.
INT64_COLUMNS: set[str] = {PARTITION_FIELD}


def _build_explicit_schema(
    columns: list[str],
    table_name: str,
) -> list[Any]:
    """Build an explicit BigQuery schema for a CSV table.

    All columns default to STRING to avoid autodetect conflicts
    with Corsican department codes (``2A``/``2B``). The ``anneemut``
    column is forced to INT64 for mutation range partitioning.

    Args:
        columns: Ordered list of column names from the CSV header.
        table_name: Target table name (used to apply INT64 overrides).

    Returns:
        List of ``bigquery.SchemaField`` instances.
    """
    from google.cloud import bigquery  # noqa: WPS433

    schema = []
    for col in columns:
        if table_name == MUTATION_TABLE and col in INT64_COLUMNS:
            schema.append(bigquery.SchemaField(col, "INT64"))
        else:
            schema.append(bigquery.SchemaField(col, "STRING"))
    return schema


def _build_csv_config(table_name: str) -> Any:
    """Build a ``LoadJobConfig`` for CSV loading with autodetect.

    Used for single-file loading (demo mode). The mutation table
    gets range partitioning and clustering.

    Args:
        table_name: Target table name.

    Returns:
        Configured ``bigquery.LoadJobConfig`` instance.
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


def _build_wildcard_csv_config(
    table_name: str,
    schema: list[Any],
) -> Any:
    """Build a ``LoadJobConfig`` for wildcard CSV loading.

    Uses an explicit schema (all STRING except ``anneemut``) to
    avoid autodetect type conflicts across chunk files.

    Args:
        table_name: Target table name.
        schema: List of ``bigquery.SchemaField`` instances.

    Returns:
        Configured ``bigquery.LoadJobConfig`` instance.
    """
    from google.cloud import bigquery  # noqa: WPS433

    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        schema=schema,
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
    """Build a ``LoadJobConfig`` for GeoJSON (newline-delimited JSON) loading.

    Uses an explicit schema with all STRING fields and
    ``ignore_unknown_values=True`` for forward compatibility.

    Returns:
        Configured ``bigquery.LoadJobConfig`` instance.
    """
    from google.cloud import bigquery  # noqa: WPS433

    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=_build_geojson_schema(),
        ignore_unknown_values=True,
    )


# ---------------------------------------------------------------------------
# Blob listing
# ---------------------------------------------------------------------------
def _list_csv_blobs(client: Any) -> list[Any]:
    """List CSV blobs under the DVF prefix in GCS.

    Args:
        client: GCS ``Client`` instance.

    Returns:
        List of GCS ``Blob`` objects ending in ``.csv``.
    """
    bucket = client.bucket(GCS_BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=GCS_DVF_PREFIX)
    return [b for b in blobs if b.name.endswith(".csv")]


def _list_geojson_blobs(client: Any) -> list[Any]:
    """List GeoJSON blobs under the geojson prefix in GCS.

    Args:
        client: GCS ``Client`` instance.

    Returns:
        List of GCS ``Blob`` objects ending in ``.geojson``.
    """
    bucket = client.bucket(GCS_BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=GCS_GEOJSON_PREFIX)
    return [b for b in blobs if b.name.endswith(".geojson")]


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------
def _load_csv_blob(bq_client: Any, blob: Any) -> int:
    """Load a single CSV blob into BigQuery.

    Args:
        bq_client: BigQuery ``Client`` instance.
        blob: GCS ``Blob`` object pointing to a CSV file.

    Returns:
        Number of rows loaded into the destination table.
    """
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
    """Load a single GeoJSON blob into BigQuery.

    Downloads the GeoJSON from GCS, extracts features as flat rows,
    and loads them via ``load_table_from_json``.

    Args:
        bq_client: BigQuery ``Client`` instance.
        blob: GCS ``Blob`` object pointing to a GeoJSON file.

    Returns:
        Number of rows loaded, or 0 if the file is unknown or empty.
    """
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


def _table_name_from_blob_path(blob_name: str) -> str:
    """Extract the table name from a blob path.

    Handles flat layout (``raw/dvf/mutation.csv`` -> ``mutation``)
    and chunked layout (``raw/dvf/mutation/chunk_001.csv`` -> ``mutation``).

    Args:
        blob_name: Full GCS blob path string.

    Returns:
        Table name string derived from the path structure.
    """
    path = PurePosixPath(blob_name)
    prefix_parts = PurePosixPath(GCS_DVF_PREFIX).parts
    remaining = path.parts[len(prefix_parts):]
    if len(remaining) >= 2:
        return remaining[0]
    return path.stem


def _group_blobs_by_table(blobs: list[Any]) -> dict[str, list[Any]]:
    """Group CSV blobs by their table name.

    Handles both flat layout (``raw/dvf/mutation.csv`` -> ``mutation``)
    and chunked layout (``raw/dvf/mutation/chunk_001.csv`` -> ``mutation``).

    Args:
        blobs: List of GCS ``Blob`` objects.

    Returns:
        Dict mapping table name to a list of blobs for that table.
    """
    groups: dict[str, list[Any]] = {}
    for blob in blobs:
        table_name = _table_name_from_blob_path(blob.name)
        groups.setdefault(table_name, []).append(blob)
    return groups


def _load_table_blobs(
    bq_client: Any,
    table_name: str,
    blobs: list[Any],
) -> int:
    """Load one table from one or more GCS blobs.

    Uses single-blob loading for one file, wildcard loading for multiple.

    Args:
        bq_client: BigQuery ``Client`` instance.
        table_name: Target BigQuery table name.
        blobs: List of GCS ``Blob`` objects for this table.

    Returns:
        Number of rows loaded.
    """
    if len(blobs) == 1:
        return _load_csv_blob(bq_client, blobs[0])
    return _load_csv_wildcard(bq_client, table_name, blobs)


def _load_csv_wildcard(
    bq_client: Any,
    table_name: str,
    blobs: list[Any],
) -> int:
    """Load a table from multiple chunk files using wildcard URI.

    Reads the CSV header from the first chunk to build an explicit
    all-STRING schema, avoiding autodetect type conflicts across files
    (e.g. Corsican department codes ``2A``/``2B`` vs numeric codes).

    Args:
        bq_client: BigQuery ``Client`` instance.
        table_name: Target BigQuery table name.
        blobs: List of GCS ``Blob`` objects (multiple chunks).

    Returns:
        Number of rows loaded.
    """
    gcs_client = get_gcs_client()
    columns = _read_csv_header_from_gcs(gcs_client, blobs[0].name)
    schema = _build_explicit_schema(columns, table_name)
    uri = f"gs://{GCS_BUCKET_NAME}/{GCS_DVF_PREFIX}/{table_name}/*"
    dest = _table_id(table_name)
    config = _build_wildcard_csv_config(table_name, schema)
    logger.info("Loading %s (%d files) -> %s", uri, len(blobs), dest)
    job = bq_client.load_table_from_uri(uri, dest, job_config=config)
    job.result()
    table = bq_client.get_table(dest)
    logger.info("Loaded %s: %s rows", table_name, f"{table.num_rows:,}")
    return table.num_rows


def _load_all_csvs(bq_client: Any, blobs: list[Any]) -> int:
    """Load all CSV tables into BigQuery.

    Groups blobs by table name and loads each table, using wildcard
    URIs for tables with multiple chunk files.

    Args:
        bq_client: BigQuery ``Client`` instance.
        blobs: List of all CSV ``Blob`` objects from GCS.

    Returns:
        Total number of rows loaded across all tables.
    """
    table_groups = _group_blobs_by_table(blobs)
    total = 0
    for table_name, table_blobs in table_groups.items():
        total += _load_table_blobs(bq_client, table_name, table_blobs)
    return total


def _load_all_geojson(bq_client: Any, blobs: list[Any]) -> int:
    """Load all GeoJSON blobs into BigQuery.

    Args:
        bq_client: BigQuery ``Client`` instance.
        blobs: List of GeoJSON ``Blob`` objects from GCS.

    Returns:
        Total number of rows loaded across all GeoJSON tables.
    """
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
    """Discover CSV and GeoJSON blobs in the configured GCS bucket.

    Args:
        gcs_client: GCS ``Client`` instance.

    Returns:
        Tuple of ``(csv_blobs, geojson_blobs)``.
    """
    csv_blobs = _list_csv_blobs(gcs_client)
    geojson_blobs = _list_geojson_blobs(gcs_client)
    logger.info(
        "Found %d CSV blob(s) and %d GeoJSON blob(s) to load.",
        len(csv_blobs),
        len(geojson_blobs),
    )
    return csv_blobs, geojson_blobs


def load_to_bigquery() -> int:
    """Load all GCS data into BigQuery raw tables.

    Discovers CSV and GeoJSON blobs in the configured GCS bucket,
    loads each into the ``dvf_raw`` BigQuery dataset with appropriate
    schemas and partitioning.

    Returns:
        Total number of rows loaded across all tables, or 0 if
        configuration is invalid or no blobs are found.
    """
    if not _validate_config():
        return 0

    gcs_client = get_gcs_client()
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
    setup_logging()
    count = load_to_bigquery()
    if count == 0:
        logger.error("No data was loaded into BigQuery.")
        sys.exit(1)
    logger.info("Successfully loaded %s rows.", f"{count:,}")


if __name__ == "__main__":
    main()
