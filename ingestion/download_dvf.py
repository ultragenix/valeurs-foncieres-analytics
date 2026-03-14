"""Download DVF+ SQL dump from Cerema.

Supports two modes:
  - Automatic: tries known download URLs (data.gouv.fr redirect, Cerema Box).
  - Manual: user provides a pre-downloaded file via --file argument.

The downloaded .7z archive is extracted to produce .sql files in data/.
"""

import argparse
import logging
import shutil
import sys
from pathlib import Path

import requests
from tqdm import tqdm

from ingestion.config import DATA_DIR

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# data.gouv.fr resource URL that redirects to the Cerema Box file.
# This is the most stable public URL for the DVF+ open-data SQL dump.
DATA_GOUV_RESOURCE_URL: str = (
    "https://www.data.gouv.fr/api/1/datasets/r/2dc60d40-6e45-48a1-bac3-05bb593af534"
)

# Direct Cerema Box base page (for manual instructions).
CEREMA_BOX_PAGE_URL: str = "https://cerema.app.box.com/v/dvfplus-opendata"

# Cerema datafoncier documentation page.
CEREMA_DOC_URL: str = (
    "https://datafoncier.cerema.fr/donnees/autres-donnees-foncieres/dvfplus-open-data"
)

# Expected archive suffix.
ARCHIVE_SUFFIX: str = ".7z"

# HTTP request timeout in seconds.
HTTP_TIMEOUT_SECONDS: int = 30

# Chunk size for streaming download (1 MB).
DOWNLOAD_CHUNK_SIZE: int = 1_048_576


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------
def _find_existing_sql_files(directory: Path) -> list[Path]:
    """Return all .sql files in the given directory."""
    if not directory.exists():
        return []
    return sorted(directory.glob("*.sql"))


def _find_existing_archives(directory: Path) -> list[Path]:
    """Return all .7z files in the given directory."""
    if not directory.exists():
        return []
    return sorted(directory.glob(f"*{ARCHIVE_SUFFIX}"))


def _download_with_progress(url: str, destination: Path) -> bool:
    """Stream-download a URL to *destination* with a tqdm progress bar.

    Returns True on success, False on failure.
    """
    try:
        response = requests.get(
            url, stream=True, timeout=HTTP_TIMEOUT_SECONDS, allow_redirects=True
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Download from %s failed: %s", url, exc)
        return False

    total_size = int(response.headers.get("content-length", 0))
    logger.info(
        "Downloading %s (%s bytes) ...",
        url,
        f"{total_size:,}" if total_size else "unknown size",
    )

    destination.parent.mkdir(parents=True, exist_ok=True)
    with (
        open(destination, "wb") as fh,
        tqdm(total=total_size, unit="B", unit_scale=True, desc=destination.name) as bar,
    ):
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
            fh.write(chunk)
            bar.update(len(chunk))

    logger.info("Saved to %s", destination)
    return True


def _extract_7z(archive_path: Path, target_dir: Path) -> list[Path]:
    """Extract a .7z archive into *target_dir* and return extracted paths.

    Requires the ``py7zr`` package.
    """
    try:
        import py7zr  # noqa: WPS433 — lazy import, optional dep
    except ImportError:
        logger.error(
            "py7zr is not installed. Run: uv pip install py7zr"
        )
        sys.exit(1)

    logger.info("Extracting %s ...", archive_path)
    target_dir.mkdir(parents=True, exist_ok=True)

    with py7zr.SevenZipFile(archive_path, mode="r") as archive:
        archive.extractall(path=target_dir)

    extracted = sorted(target_dir.glob("*.sql"))
    logger.info("Extracted %d .sql file(s): %s", len(extracted), extracted)
    return extracted


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------
def _print_manual_instructions() -> None:
    """Log instructions for manual download from Cerema Box."""
    logger.info(
        "=== Manual download instructions ===\n"
        "1. Open %s\n"
        "2. Navigate to the latest folder (e.g. octobre_2025)\n"
        "3. Download the .7z file for the desired department(s) or full France\n"
        "4. Place the .7z file in: %s\n"
        "5. Re-run this script, or use: python -m ingestion.download_dvf --file <path>\n"
        "Alternatively, check %s for updated links.",
        CEREMA_BOX_PAGE_URL,
        DATA_DIR,
        CEREMA_DOC_URL,
    )


def download_dvf(manual_file: Path | None = None) -> list[Path]:
    """Download and extract the DVF+ SQL dump.

    Args:
        manual_file: Optional path to a pre-downloaded .7z or .sql file.

    Returns:
        List of .sql file paths ready for restore.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # ----- Case 1: SQL files already present (idempotent) -----
    existing_sql = _find_existing_sql_files(DATA_DIR)
    if existing_sql:
        logger.info(
            "SQL file(s) already present in %s — skipping download: %s",
            DATA_DIR,
            [f.name for f in existing_sql],
        )
        return existing_sql

    # ----- Case 2: User provided a manual file -----
    if manual_file is not None:
        manual_path = Path(manual_file).resolve()
        if not manual_path.exists():
            logger.error("Provided file does not exist: %s", manual_path)
            sys.exit(1)

        if manual_path.suffix == ".sql":
            dest = DATA_DIR / manual_path.name
            if dest != manual_path:
                shutil.copy2(manual_path, dest)
                logger.info("Copied %s to %s", manual_path, dest)
            return [dest]

        if manual_path.suffix == ".7z":
            dest_archive = DATA_DIR / manual_path.name
            if dest_archive != manual_path:
                shutil.copy2(manual_path, dest_archive)
            return _extract_7z(dest_archive, DATA_DIR)

        logger.error(
            "Unsupported file format: %s (expected .sql or .7z)", manual_path.suffix
        )
        sys.exit(1)

    # ----- Case 3: Try to extract already-downloaded archive -----
    existing_archives = _find_existing_archives(DATA_DIR)
    if existing_archives:
        logger.info("Found existing archive(s): %s", existing_archives)
        sql_files: list[Path] = []
        for archive in existing_archives:
            sql_files.extend(_extract_7z(archive, DATA_DIR))
        if sql_files:
            return sql_files

    # ----- Case 4: Attempt automatic download -----
    logger.info("Attempting download from data.gouv.fr redirect ...")
    archive_dest = DATA_DIR / "dvfplus.7z"

    if _download_with_progress(DATA_GOUV_RESOURCE_URL, archive_dest):
        # Check if the downloaded file is actually a .7z archive
        if archive_dest.stat().st_size > 0:
            try:
                return _extract_7z(archive_dest, DATA_DIR)
            except Exception as exc:
                logger.warning(
                    "Extraction failed (file may not be a .7z archive): %s", exc
                )
                # The URL might have returned an HTML page instead of the file.
                archive_dest.unlink(missing_ok=True)

    # ----- Case 5: Automatic download failed — manual instructions -----
    logger.warning("Automatic download was not successful.")
    _print_manual_instructions()
    sys.exit(1)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Download DVF+ SQL dump from Cerema."
    )
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Path to a manually downloaded .7z or .sql file.",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    args = _parse_args()
    manual = Path(args.file) if args.file else None
    sql_files = download_dvf(manual_file=manual)
    logger.info("Ready for restore. SQL files: %s", [f.name for f in sql_files])


if __name__ == "__main__":
    main()
