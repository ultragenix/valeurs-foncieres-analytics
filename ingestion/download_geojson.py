"""Download administrative boundary GeoJSON files from Etalab.

Downloads department and commune boundary polygons (1km generalization)
for use in BigQuery geographic analysis and Looker Studio choropleth maps.
"""

import json
import logging
import sys
from pathlib import Path

import requests
from tqdm import tqdm

from ingestion.config import DATA_GEOJSON_DIR

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

ETALAB_BASE_URL: str = (
    "https://etalab-datasets.geo.data.gouv.fr"
    "/contours-administratifs/2024/geojson"
)

GEOJSON_FILES: dict[str, str] = {
    "departements-1000m.geojson": f"{ETALAB_BASE_URL}/departements-1000m.geojson",
    "communes-1000m.geojson": f"{ETALAB_BASE_URL}/communes-1000m.geojson",
}

# Minimum expected feature counts for validation.
MIN_DEPARTMENT_FEATURES: int = 100
MIN_COMMUNE_FEATURES: int = 30_000

FEATURE_THRESHOLDS: dict[str, int] = {
    "departements-1000m.geojson": MIN_DEPARTMENT_FEATURES,
    "communes-1000m.geojson": MIN_COMMUNE_FEATURES,
}

# HTTP timeouts (connect, read) in seconds.
HTTP_CONNECT_TIMEOUT: int = 30
HTTP_READ_TIMEOUT: int = 300

# Chunk size for streaming download (1 MB).
DOWNLOAD_CHUNK_SIZE: int = 1_048_576


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------
def _should_skip_download(destination: Path) -> bool:
    """Return True if the file already exists with non-zero size."""
    if destination.exists() and destination.stat().st_size > 0:
        logger.info("File already exists, skipping download: %s", destination.name)
        return True
    return False


def _download_file(url: str, destination: Path) -> bool:
    """Stream-download a URL to *destination* with a tqdm progress bar.

    Returns True on success, False on failure.
    """
    try:
        response = requests.get(
            url,
            stream=True,
            timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
            allow_redirects=True,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Download failed for %s: %s", url, exc)
        return False

    _stream_to_file(response, destination)
    return True


def _stream_to_file(response: requests.Response, destination: Path) -> None:
    """Write streaming response content to *destination* with progress."""
    total_size = int(response.headers.get("content-length", 0))
    destination.parent.mkdir(parents=True, exist_ok=True)

    with (
        open(destination, "wb") as fh,
        tqdm(
            total=total_size, unit="B", unit_scale=True, desc=destination.name
        ) as bar,
    ):
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
            fh.write(chunk)
            bar.update(len(chunk))

    logger.info("Saved %s (%s bytes)", destination.name, f"{destination.stat().st_size:,}")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def _parse_geojson(file_path: Path) -> dict | None:
    """Load and parse a GeoJSON file. Returns None on failure."""
    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Invalid GeoJSON file %s: %s", file_path.name, exc)
        return None


def _validate_geojson(file_path: Path, min_features: int) -> bool:
    """Check that a GeoJSON file has at least *min_features* features."""
    data = _parse_geojson(file_path)
    if data is None:
        return False

    count = len(data.get("features", []))
    if count < min_features:
        logger.error(
            "%s has %d features (expected >= %d)",
            file_path.name, count, min_features,
        )
        return False

    logger.info("%s validated: %d features", file_path.name, count)
    return True


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def download_geojson() -> list[Path]:
    """Download department and commune GeoJSON files.

    Returns a list of paths to the downloaded files.
    """
    DATA_GEOJSON_DIR.mkdir(parents=True, exist_ok=True)
    downloaded: list[Path] = []

    for filename, url in GEOJSON_FILES.items():
        destination = DATA_GEOJSON_DIR / filename
        path = _download_and_validate(filename, url, destination)
        if path is not None:
            downloaded.append(path)

    logger.info("GeoJSON download complete: %d file(s)", len(downloaded))
    return downloaded


def _download_and_validate(
    filename: str, url: str, destination: Path
) -> Path | None:
    """Download one GeoJSON file if needed and validate it.

    Returns the path on success, None on failure.
    """
    if not _should_skip_download(destination):
        if not _download_file(url, destination):
            return None

    min_features = FEATURE_THRESHOLDS.get(filename, 0)
    if not _validate_geojson(destination, min_features):
        return None

    return destination


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s -- %(message)s",
    )
    paths = download_geojson()
    if not paths:
        logger.error("No GeoJSON files were downloaded successfully.")
        sys.exit(1)
    logger.info("GeoJSON files ready: %s", [p.name for p in paths])


if __name__ == "__main__":
    main()
