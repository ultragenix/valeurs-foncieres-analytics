"""Upload CSV and GeoJSON files to Google Cloud Storage.

Uploads exported DVF+ CSV files to ``raw/dvf/`` and GeoJSON administrative
boundary files to ``raw/geojson/`` in the configured GCS bucket.
"""

import logging
import sys
from pathlib import Path

from tqdm import tqdm

from ingestion.config import DATA_EXPORT_DIR, DATA_GEOJSON_DIR, GCS_BUCKET_NAME

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

GCS_DVF_PREFIX: str = "raw/dvf"
GCS_GEOJSON_PREFIX: str = "raw/geojson"


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
def _get_gcs_client():  # noqa: ANN201 -- returns google.cloud.storage.Client
    """Create and return a GCS storage client."""
    from google.cloud import storage  # noqa: WPS433 -- lazy import

    return storage.Client()


def _upload_file(bucket, local_path: Path, gcs_path: str) -> None:  # noqa: ANN001
    """Upload a single file to GCS and log its size."""
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(str(local_path))
    size_mb = local_path.stat().st_size / (1024 * 1024)
    logger.info(
        "Uploaded %s -> gs://%s/%s (%.1f MB)",
        local_path.name,
        bucket.name,
        gcs_path,
        size_mb,
    )


def _upload_file_list(
    bucket,  # noqa: ANN001
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

    client = _get_gcs_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    uploaded = _upload_all(bucket, csv_files, geojson_files, total_files)

    logger.info(
        "Upload complete: %d file(s) to gs://%s/",
        uploaded,
        GCS_BUCKET_NAME,
    )
    return uploaded


def _upload_all(
    bucket,  # noqa: ANN001
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
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s -- %(message)s",
    )
    count = upload_to_gcs()
    if count == 0:
        logger.error("No files were uploaded.")
        sys.exit(1)
    logger.info("Successfully uploaded %d file(s).", count)


if __name__ == "__main__":
    main()
