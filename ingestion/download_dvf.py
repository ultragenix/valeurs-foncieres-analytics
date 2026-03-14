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

from ingestion.config import DATA_DIR, setup_logging
from ingestion.http_utils import stream_download

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

# HTTP read timeout for large DVF archive downloads (seconds).
DVF_READ_TIMEOUT: int = 120


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
    return stream_download(url, destination, read_timeout=DVF_READ_TIMEOUT)


def _validate_archive_members(names: list[str]) -> None:
    """Validate archive member filenames for path traversal attacks.

    Raises ValueError if any member has an absolute path or contains '..'.
    """
    for name in names:
        if ".." in name or Path(name).is_absolute():
            msg = f"Unsafe archive member detected: {name}"
            raise ValueError(msg)


def _extract_7z(archive_path: Path, target_dir: Path) -> list[Path]:
    """Extract a .7z archive into *target_dir* and return extracted paths.

    Validates member filenames before extraction to prevent path traversal.
    Requires the ``py7zr`` package.
    """
    try:
        import py7zr  # noqa: WPS433 -- lazy import, optional dep
    except ImportError:
        logger.error(
            "py7zr is not installed. Run: uv pip install py7zr"
        )
        sys.exit(1)

    logger.info("Extracting %s ...", archive_path)
    target_dir.mkdir(parents=True, exist_ok=True)

    with py7zr.SevenZipFile(archive_path, mode="r") as archive:
        _validate_archive_members(archive.getnames())
        archive.extractall(path=target_dir)

    extracted = sorted(target_dir.glob("*.sql"))
    logger.info("Extracted %d .sql file(s): %s", len(extracted), extracted)
    return extracted


# ---------------------------------------------------------------------------
# Main workflow helpers
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


def _return_existing_sql(data_dir: Path) -> list[Path] | None:
    """Return existing SQL files if present, otherwise None."""
    existing_sql = _find_existing_sql_files(data_dir)
    if existing_sql:
        logger.info(
            "SQL file(s) already present in %s -- skipping download: %s",
            data_dir,
            [f.name for f in existing_sql],
        )
        return existing_sql
    return None


def _handle_manual_sql(manual_path: Path, data_dir: Path) -> list[Path]:
    """Copy a manual .sql file into data_dir and return its path."""
    dest = data_dir / manual_path.name
    if dest != manual_path:
        shutil.copy2(manual_path, dest)
        logger.info("Copied %s to %s", manual_path, dest)
    return [dest]


def _handle_manual_7z(manual_path: Path, data_dir: Path) -> list[Path]:
    """Copy a manual .7z archive into data_dir and extract it."""
    dest_archive = data_dir / manual_path.name
    if dest_archive != manual_path:
        shutil.copy2(manual_path, dest_archive)
    return _extract_7z(dest_archive, data_dir)


def _handle_manual_file(
    manual_file: Path, data_dir: Path
) -> list[Path]:
    """Process a user-provided .sql or .7z file.

    Exits with error if file does not exist or format is unsupported.
    """
    manual_path = Path(manual_file).resolve()
    if not manual_path.exists():
        logger.error("Provided file does not exist: %s", manual_path)
        sys.exit(1)

    if manual_path.suffix == ".sql":
        return _handle_manual_sql(manual_path, data_dir)
    if manual_path.suffix == ".7z":
        return _handle_manual_7z(manual_path, data_dir)

    logger.error(
        "Unsupported file format: %s (expected .sql or .7z)",
        manual_path.suffix,
    )
    sys.exit(1)


def _try_existing_archives(data_dir: Path) -> list[Path] | None:
    """Try to extract already-downloaded archives. Return SQL files or None."""
    existing_archives = _find_existing_archives(data_dir)
    if not existing_archives:
        return None

    logger.info("Found existing archive(s): %s", existing_archives)
    sql_files: list[Path] = []
    for archive in existing_archives:
        sql_files.extend(_extract_7z(archive, data_dir))
    return sql_files if sql_files else None


def _try_automatic_download(data_dir: Path) -> list[Path] | None:
    """Attempt automatic download from data.gouv.fr. Return SQL files or None."""
    import py7zr  # noqa: WPS433 -- needed for specific exception handling

    logger.info("Attempting download from data.gouv.fr redirect ...")
    archive_dest = data_dir / "dvfplus.7z"

    if not _download_with_progress(DATA_GOUV_RESOURCE_URL, archive_dest):
        return None

    if archive_dest.stat().st_size == 0:
        return None

    try:
        return _extract_7z(archive_dest, data_dir)
    except (py7zr.Bad7zFile, OSError) as exc:
        logger.warning(
            "Extraction failed (file may not be a .7z archive): %s", exc
        )
        archive_dest.unlink(missing_ok=True)
        return None


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------
def _fail_with_manual_instructions() -> None:
    """Log failure and print manual download instructions, then exit."""
    logger.warning("Automatic download was not successful.")
    _print_manual_instructions()
    sys.exit(1)


def download_dvf(manual_file: Path | None = None) -> list[Path]:
    """Download and extract the DVF+ SQL dump, returning .sql file paths."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    existing = _return_existing_sql(DATA_DIR)
    if existing:
        return existing

    if manual_file is not None:
        return _handle_manual_file(manual_file, DATA_DIR)

    from_archives = _try_existing_archives(DATA_DIR)
    if from_archives:
        return from_archives

    from_download = _try_automatic_download(DATA_DIR)
    if from_download:
        return from_download

    _fail_with_manual_instructions()
    return []  # unreachable, satisfies type checker


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
    setup_logging()
    args = _parse_args()
    manual = Path(args.file) if args.file else None
    sql_files = download_dvf(manual_file=manual)
    logger.info("Ready for restore. SQL files: %s", [f.name for f in sql_files])


if __name__ == "__main__":
    main()
