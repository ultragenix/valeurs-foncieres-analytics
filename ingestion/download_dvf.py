"""Download DVF+ SQL dump from Cerema.

Supports two modes:
  - Automatic: tries known download URLs (data.gouv.fr redirect, Cerema Box).
  - Manual: user provides a pre-downloaded file via ``--file`` argument.

The downloaded ``.7z`` archive is extracted to produce ``.sql`` files in
the ``data/`` directory. Handles nested archives (multi-part ``.7z.001``
containing an inner ``.7z``) and SQL files in subdirectories.

Inputs:
    - URL from data.gouv.fr / Cerema Box, or a local ``.7z``/``.sql`` file.
Outputs:
    - One or more ``.sql`` files in ``data/``.

Dependencies:
    py7zr  -- for extracting 7-Zip archives.
    ingestion.http_utils -- for streaming HTTP downloads.
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
    """Return all ``.sql`` files in the given directory, sorted by name.

    Args:
        directory: The directory to search (non-recursive).

    Returns:
        Sorted list of ``.sql`` file paths, or empty list if the
        directory does not exist.
    """
    if not directory.exists():
        return []
    return sorted(directory.glob("*.sql"))


def _find_existing_archives(directory: Path) -> list[Path]:
    """Return all ``.7z`` or ``.7z.001`` (multi-part) archives in *directory*.

    Args:
        directory: The directory to search (non-recursive).

    Returns:
        Sorted list of archive paths, or empty list if the directory
        does not exist.
    """
    if not directory.exists():
        return []
    archives = set(directory.glob(f"*{ARCHIVE_SUFFIX}"))
    archives.update(directory.glob(f"*{ARCHIVE_SUFFIX}.001"))
    return sorted(archives)


def _download_with_progress(url: str, destination: Path) -> bool:
    """Stream-download a URL to *destination* with a tqdm progress bar.

    Delegates to ``stream_download`` with ``DVF_READ_TIMEOUT``.

    Args:
        url: The HTTP(S) URL to download.
        destination: Local file path for the downloaded content.

    Returns:
        True on success, False on failure.
    """
    return stream_download(url, destination, read_timeout=DVF_READ_TIMEOUT)


def _validate_archive_members(names: list[str]) -> None:
    """Validate archive member filenames for path traversal attacks.

    Args:
        names: List of member filenames from the archive.

    Raises:
        ValueError: If any member has an absolute path or contains ``..``.
    """
    for name in names:
        if ".." in name or Path(name).is_absolute():
            msg = f"Unsafe archive member detected: {name}"
            raise ValueError(msg)


def _extract_7z(archive_path: Path, target_dir: Path) -> list[Path]:
    """Extract a ``.7z`` archive into *target_dir* and return ``.sql`` file paths.

    Handles nested archives (``.7z.001`` containing a ``.7z``) and SQL files
    inside subdirectories. Validates member filenames before extraction to
    prevent path traversal. Requires the ``py7zr`` package.

    Args:
        archive_path: Path to the ``.7z`` archive file.
        target_dir: Directory where contents will be extracted.

    Returns:
        List of extracted ``.sql`` file paths (moved to *target_dir* root).
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

    extracted = _collect_sql_files(target_dir)
    if extracted:
        return extracted

    # Handle nested archives (e.g. .7z.001 containing an inner .7z)
    return _extract_nested_archives(target_dir)


def _collect_sql_files(directory: Path) -> list[Path]:
    """Find ``.sql`` files recursively and move them to the top-level directory.

    Args:
        directory: Root directory to search.

    Returns:
        List of ``.sql`` file paths now located directly in *directory*.
    """
    sql_files: list[Path] = []
    for sql_file in sorted(directory.rglob("*.sql")):
        if sql_file.parent != directory:
            dest = directory / sql_file.name
            shutil.move(str(sql_file), str(dest))
            sql_files.append(dest)
        else:
            sql_files.append(sql_file)
    if sql_files:
        logger.info("Found %d .sql file(s): %s", len(sql_files), sql_files)
    return sql_files


def _extract_nested_archives(directory: Path) -> list[Path]:
    """Extract any inner ``.7z`` archives found after first extraction.

    Args:
        directory: Directory to search for nested ``.7z`` files.

    Returns:
        List of ``.sql`` file paths extracted from nested archives.
    """
    inner_archives = sorted(directory.glob("*.7z"))
    sql_files: list[Path] = []
    for inner in inner_archives:
        logger.info("Found nested archive: %s", inner.name)
        sql_files.extend(_extract_7z(inner, directory))
    if not sql_files:
        logger.warning("No .sql files found after extraction.")
    return sql_files


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
    """Return existing SQL files if present, otherwise None.

    Args:
        data_dir: Directory to check for ``.sql`` files.

    Returns:
        List of paths if SQL files exist, ``None`` otherwise.
    """
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
    """Copy a user-provided ``.sql`` file into *data_dir* and return its path.

    Args:
        manual_path: Resolved path to the user-provided SQL file.
        data_dir: Target directory for the copy.

    Returns:
        Single-element list with the destination path.
    """
    dest = data_dir / manual_path.name
    if dest != manual_path:
        shutil.copy2(manual_path, dest)
        logger.info("Copied %s to %s", manual_path, dest)
    return [dest]


def _handle_manual_7z(manual_path: Path, data_dir: Path) -> list[Path]:
    """Copy a user-provided ``.7z`` archive into *data_dir* and extract it.

    Args:
        manual_path: Resolved path to the user-provided archive.
        data_dir: Target directory for the copy and extraction.

    Returns:
        List of extracted ``.sql`` file paths.
    """
    dest_archive = data_dir / manual_path.name
    if dest_archive != manual_path:
        shutil.copy2(manual_path, dest_archive)
    return _extract_7z(dest_archive, data_dir)


def _handle_manual_file(
    manual_file: Path, data_dir: Path
) -> list[Path]:
    """Process a user-provided ``.sql`` or ``.7z`` file.

    Args:
        manual_file: Path to the user-provided file (resolved internally).
        data_dir: Target directory for copying and extraction.

    Returns:
        List of resulting ``.sql`` file paths.

    Raises:
        SystemExit: If the file does not exist or its format is unsupported.
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
    """Try to extract already-downloaded archives in *data_dir*.

    Args:
        data_dir: Directory to search for existing ``.7z`` archives.

    Returns:
        List of extracted ``.sql`` file paths, or ``None`` if no archives
        are found or extraction yields no SQL files.
    """
    existing_archives = _find_existing_archives(data_dir)
    if not existing_archives:
        return None

    logger.info("Found existing archive(s): %s", existing_archives)
    sql_files: list[Path] = []
    for archive in existing_archives:
        sql_files.extend(_extract_7z(archive, data_dir))
    return sql_files if sql_files else None


def _try_automatic_download(data_dir: Path) -> list[Path] | None:
    """Attempt automatic download from data.gouv.fr and extract the archive.

    Downloads the DVF+ archive via ``DATA_GOUV_RESOURCE_URL``, which
    redirects to the Cerema Box hosting.

    Args:
        data_dir: Directory where the archive is saved and extracted.

    Returns:
        List of extracted ``.sql`` file paths, or ``None`` if the
        download or extraction fails.
    """
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
    """Download and extract the DVF+ SQL dump, returning ``.sql`` file paths.

    Resolution order:
      1. Reuse existing ``.sql`` files in ``DATA_DIR``.
      2. Use *manual_file* if provided.
      3. Extract existing archives in ``DATA_DIR``.
      4. Attempt automatic download from data.gouv.fr.
      5. Print manual download instructions and exit.

    Args:
        manual_file: Optional path to a user-provided ``.sql`` or ``.7z``
            file, bypassing automatic download.

    Returns:
        List of ``.sql`` file paths ready for ``restore_dump``.

    Raises:
        SystemExit: If no SQL files can be obtained by any method.
    """
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
    """Parse command-line arguments.

    Returns:
        Namespace with optional ``file`` attribute (str or None).
    """
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
