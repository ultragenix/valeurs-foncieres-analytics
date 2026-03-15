"""Upload CSV and GeoJSON files to Google Cloud Storage.

Uploads exported DVF+ CSV files to ``raw/dvf/`` and GeoJSON administrative
boundary files to ``raw/geojson/`` in the configured GCS bucket.

For chunked full-France ingestion, ``upload_chunk_to_gcs()`` uploads a
chunk's CSV files to per-table GCS subdirectories
(e.g., ``raw/dvf/mutation/chunk_001.csv``).
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

from tqdm import tqdm

from ingestion.config import (
    DATA_EXPORT_DIR,
    DATA_GEOJSON_DIR,
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

BYTES_PER_MB: int = 1_048_576
MIN_CSV_DATA_SIZE: int = 1024  # Skip header-only CSVs (< 1 KB)


# ---------------------------------------------------------------------------
# File collection
# ---------------------------------------------------------------------------
def _collect_csv_files() -> list[Path]:
    """Return sorted list of CSV files in the export directory."""
    if not DATA_EXPORT_DIR.exists():
        logger.warning("Export directory does not exist: %s", DATA_EXPORT_DIR)
        return []
    return sorted(DATA_EXPORT_DIR.glob("*.csv"))


def _collect_geojson_files() -> list[Path]:
    """Return sorted list of GeoJSON files in the geojson directory."""
    if not DATA_GEOJSON_DIR.exists():
        logger.warning("GeoJSON directory does not exist: %s", DATA_GEOJSON_DIR)
        return []
    return sorted(DATA_GEOJSON_DIR.glob("*.geojson"))


def _collect_files() -> tuple[list[Path], list[Path]]:
    """Collect all files to upload: (csv_files, geojson_files)."""
    csv_files = _collect_csv_files()
    geojson_files = _collect_geojson_files()
    logger.info(
        "Found %d CSV file(s) and %d GeoJSON file(s) to upload.",
        len(csv_files),
        len(geojson_files),
    )
    return csv_files, geojson_files


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------
def _upload_file(bucket: Any, local_path: Path, gcs_path: str) -> None:
    """Upload a single file to GCS and log its size."""
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(str(local_path))
    size_mb = local_path.stat().st_size / BYTES_PER_MB
    logger.info(
        "Uploaded %s -> gs://%s/%s (%.1f MB)",
        local_path.name,
        bucket.name,
        gcs_path,
        size_mb,
    )


def _upload_file_list(
    bucket: Any,
    files: list[Path],
    prefix: str,
    progress: tqdm,
) -> int:
    """Upload a list of files to *prefix* in the bucket. Returns count."""
    uploaded = 0
    for local_path in files:
        gcs_path = f"{prefix}/{local_path.name}"
        progress.set_postfix_str(local_path.name)
        _upload_file(bucket, local_path, gcs_path)
        progress.update(1)
        uploaded += 1
    return uploaded


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def _validate_bucket_name() -> bool:
    """Check that GCS_BUCKET_NAME is configured. Return True if valid."""
    if not GCS_BUCKET_NAME:
        logger.error(
            "GCS_BUCKET_NAME is not configured. "
            "Set it in .env or as an environment variable."
        )
        return False
    return True


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def upload_to_gcs() -> int:
    """Upload all CSV and GeoJSON files to GCS. Returns count of uploaded files."""
    if not _validate_bucket_name():
        return 0

    csv_files, geojson_files = _collect_files()
    total_files = len(csv_files) + len(geojson_files)

    if total_files == 0:
        logger.warning("No files to upload.")
        return 0

    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    uploaded = _upload_all(bucket, csv_files, geojson_files, total_files)

    logger.info(
        "Upload complete: %d file(s) to gs://%s/",
        uploaded,
        GCS_BUCKET_NAME,
    )
    return uploaded


def _upload_all(
    bucket: Any,
    csv_files: list[Path],
    geojson_files: list[Path],
    total_files: int,
) -> int:
    """Upload CSV and GeoJSON files with a unified progress bar."""
    progress = tqdm(total=total_files, desc="Uploading to GCS", unit="file")
    uploaded = 0

    uploaded += _upload_file_list(bucket, csv_files, GCS_DVF_PREFIX, progress)
    uploaded += _upload_file_list(
        bucket, geojson_files, GCS_GEOJSON_PREFIX, progress
    )

    progress.close()
    return uploaded


# ---------------------------------------------------------------------------
# Per-chunk upload (full-France chunked ingestion)
# ---------------------------------------------------------------------------
def _chunk_gcs_path(table_name: str, chunk_index: int) -> str:
    """Build the GCS path for a chunk's CSV file within a table subdir."""
    return f"{GCS_DVF_PREFIX}/{table_name}/chunk_{chunk_index + 1:03d}.csv"


def upload_chunk_to_gcs(chunk_dir: Path, chunk_index: int) -> int:
    """Upload a chunk's CSV files to per-table GCS subdirectories.

    Each CSV file in *chunk_dir* is uploaded to::

        gs://bucket/raw/dvf/{table_name}/chunk_{NNN}.csv

    For example, ``chunk_dir/mutation.csv`` with ``chunk_index=0``
    becomes ``gs://bucket/raw/dvf/mutation/chunk_001.csv``.

    Returns the count of files uploaded.
    """
    if not _validate_bucket_name():
        return 0

    csv_files = sorted(chunk_dir.glob("*.csv"))
    if not csv_files:
        logger.warning("No CSV files in chunk directory: %s", chunk_dir)
        return 0

    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    return _upload_chunk_files(bucket, csv_files, chunk_index)


def _is_header_only(csv_file: Path) -> bool:
    """Return True if a CSV file contains only a header (no data rows)."""
    return csv_file.stat().st_size < MIN_CSV_DATA_SIZE


def _upload_chunk_files(
    bucket: Any,
    csv_files: list[Path],
    chunk_index: int,
) -> int:
    """Upload each CSV file to its per-table GCS subdirectory."""
    uploaded = 0
    for csv_file in csv_files:
        if _is_header_only(csv_file):
            logger.debug("Skipping header-only file: %s", csv_file.name)
            continue
        table_name = csv_file.stem
        gcs_path = _chunk_gcs_path(table_name, chunk_index)
        _upload_file(bucket, csv_file, gcs_path)
        uploaded += 1
    logger.info(
        "Chunk %d: uploaded %d file(s) to gs://%s/%s/",
        chunk_index + 1,
        uploaded,
        GCS_BUCKET_NAME,
        GCS_DVF_PREFIX,
    )
    return uploaded


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    """CLI entry point."""
    setup_logging()
    count = upload_to_gcs()
    if count == 0:
        logger.error("No files were uploaded.")
        sys.exit(1)
    logger.info("Successfully uploaded %d file(s).", count)


if __name__ == "__main__":
    main()
