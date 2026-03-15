"""Chunked full-France DVF+ ingestion with crash-safe resume.

Processes department SQL files in configurable batches (default 10) to
avoid PostgreSQL WAL (Write-Ahead Log) overflow and shared memory
exhaustion on resource-constrained hosts. Each chunk cycle:

  1. Reset PostgreSQL data tables (except annexe, shared across chunks).
  2. Restore the chunk's department SQL files via ``psql``.
  3. Export tables to CSV in a per-chunk subdirectory.
  4. Upload chunk CSVs to per-table GCS subdirectories.
  5. Delete local chunk directory to free disk space.
  6. Update the JSON progress file.

The progress file (``data/chunked_progress.json``) tracks completed
department filenames, enabling crash-safe resume without re-processing
already-uploaded departments.

Inputs:
    - Extracted national DVF+ ``.sql`` files in ``data/``.
Outputs:
    - Per-table chunked CSV objects in GCS (e.g.
      ``raw/dvf/mutation/chunk_001.csv``).
    - Progress file ``data/chunked_progress.json``.

Dependencies:
    ingestion.restore_dump  -- for SQL restore helpers.
    ingestion.export_tables -- for PostgreSQL-to-CSV export.
    ingestion.upload_to_gcs -- for GCS upload.
"""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

from ingestion.config import (
    DATA_DIR,
    DATA_EXPORT_DIR,
    DVF_CHUNK_SIZE,
    DVF_PROGRESS_FILE,
    get_pg_connection,
    setup_logging,
)
from ingestion.export_tables import export_tables
from ingestion.restore_dump import (
    _create_compatibility_views,
    _create_data_tables,
    _ensure_postgis,
    _find_sql_files,
    _run_psql_file,
)
from ingestion.upload_to_gcs import upload_chunk_to_gcs

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# Subdirectory where national DVF+ dump extracts SQL files.
LIVRAISON_SUBDIR: str = "1_DONNEES_LIVRAISON"

# Prefix for chunk export subdirectories.
CHUNK_DIR_PREFIX: str = "chunk_"


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------
def _discover_department_files(
    data_dir: Path,
) -> tuple[Path | None, list[Path]]:
    """Find the annexe SQL file and department SQL files.

    Searches both *data_dir* and its ``1_DONNEES_LIVRAISON/``
    subdirectory. The annexe file is identified by ``annexe`` in its
    filename.

    Args:
        data_dir: Base data directory to search.

    Returns:
        Tuple of ``(annexe_path, sorted_department_files)`` where
        ``annexe_path`` is ``None`` if no annexe file is found.
    """
    all_sql = _find_sql_files(data_dir)
    annexe_file = _extract_annexe_file(all_sql)
    dept_files = _extract_department_files(all_sql)
    return annexe_file, dept_files


def _extract_annexe_file(sql_files: list[Path]) -> Path | None:
    """Return the first SQL file containing ``annexe`` in its name.

    Args:
        sql_files: List of SQL file paths to search.

    Returns:
        Path to the annexe file, or ``None`` if not found.
    """
    for sql_file in sql_files:
        if "annexe" in sql_file.name.lower():
            return sql_file
    return None


def _extract_department_files(sql_files: list[Path]) -> list[Path]:
    """Return SQL files that are NOT the annexe file, sorted by name.

    Args:
        sql_files: List of all SQL file paths.

    Returns:
        Sorted list of department data SQL files.
    """
    return sorted(
        [f for f in sql_files if "annexe" not in f.name.lower()],
        key=lambda p: p.name,
    )


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------
def _read_progress(progress_file: Path) -> dict[str, Any]:
    """Read the JSON progress file.

    Returns a default dict if the file is missing or contains invalid
    JSON. The default has empty ``completed_departments`` and
    ``tables_exported`` fields.

    Args:
        progress_file: Path to the JSON progress file.

    Returns:
        Dict with ``completed_departments`` (list of filenames) and
        ``tables_exported`` (dict of table name to row count).
    """
    default: dict[str, Any] = {
        "completed_departments": [],
        "tables_exported": {},
    }
    if not progress_file.exists():
        return default
    try:
        with open(progress_file, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            return default
        return data
    except (json.JSONDecodeError, OSError):
        logger.warning("Invalid progress file %s -- starting fresh.", progress_file)
        return default


def _write_progress(progress_file: Path, progress: dict[str, Any]) -> None:
    """Atomically write progress to JSON using write-to-temp-then-rename.

    Writes to a temporary file in the same directory, then renames it
    to the target path. This prevents corruption if the process is
    killed mid-write (the rename is atomic on POSIX filesystems).

    Args:
        progress_file: Target path for the progress JSON file.
        progress: Dict to serialize (must be JSON-serializable).
    """
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=str(progress_file.parent),
        suffix=".tmp",
    )
    try:
        with open(fd, "w", encoding="utf-8") as fh:
            json.dump(progress, fh, indent=2)
        shutil.move(tmp_path, str(progress_file))
    except Exception:
        Path(tmp_path).unlink(missing_ok=True)
        raise


# ---------------------------------------------------------------------------
# Chunk management
# ---------------------------------------------------------------------------
def _filter_remaining(
    department_files: list[Path],
    completed: list[str],
) -> list[Path]:
    """Exclude already-completed department files by filename.

    Args:
        department_files: All department SQL file paths.
        completed: List of filenames already processed.

    Returns:
        Filtered list of paths not yet completed.
    """
    completed_set = set(completed)
    return [f for f in department_files if f.name not in completed_set]


def _group_into_chunks(
    files: list[Path],
    chunk_size: int,
) -> list[list[Path]]:
    """Split *files* into groups of *chunk_size*.

    The last group may be smaller than *chunk_size*.

    Args:
        files: List of file paths to split.
        chunk_size: Maximum number of files per group.

    Returns:
        List of file-path lists (chunks).
    """
    return [
        files[i : i + chunk_size]
        for i in range(0, len(files), chunk_size)
    ]


# ---------------------------------------------------------------------------
# PostgreSQL reset
# ---------------------------------------------------------------------------
def _get_dvf_data_schemas(conn: Any) -> list[str]:
    """Return all ``dvf_plus_*`` data schemas (excluding ``dvf_plus_annexe``).

    Args:
        conn: Open psycopg2 connection.

    Returns:
        Sorted list of data schema names.
    """
    query = """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name LIKE 'dvf_plus_%%'
          AND schema_name != 'dvf_plus_annexe'
        ORDER BY schema_name;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return [row[0] for row in cur.fetchall()]


def _reset_data_tables(conn: Any) -> None:
    """DROP ``dvf_plus_*`` data schemas and ``dvf`` compatibility views.

    Preserves ``dvf_plus_annexe`` (annexe data is shared across all
    chunks). The ``dvf`` schema is dropped to clear stale views; it
    will be recreated by ``_create_compatibility_views``.

    Args:
        conn: Open psycopg2 connection (changes are committed).
    """
    data_schemas = _get_dvf_data_schemas(conn)
    with conn.cursor() as cur:
        for schema in data_schemas:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        cur.execute("DROP SCHEMA IF EXISTS dvf CASCADE;")
    conn.commit()
    logger.info(
        "Reset: dropped %d data schema(s) and dvf views.", len(data_schemas)
    )


# ---------------------------------------------------------------------------
# Chunk restore
# ---------------------------------------------------------------------------
def _restore_chunk(
    annexe_file: Path | None,
    department_files: list[Path],
    is_first_chunk: bool,
) -> None:
    """Restore annexe (first chunk only) and department files into PG.

    Ensures PostGIS is available, creates data tables from COPY
    command parsing, restores SQL files via psql, and creates
    compatibility views for downstream export.

    Args:
        annexe_file: Path to the annexe SQL file, or ``None``.
        department_files: Department SQL files for this chunk.
        is_first_chunk: If True, the annexe file is included in the
            restore (it only needs to be loaded once).
    """
    conn = get_pg_connection()
    try:
        _ensure_postgis(conn)
        all_files = _build_restore_file_list(
            annexe_file, department_files, is_first_chunk
        )
        data_schema = _create_data_tables(conn, all_files)
    finally:
        conn.close()

    _execute_restore_files(all_files)
    _create_views_if_national(data_schema)


def _build_restore_file_list(
    annexe_file: Path | None,
    department_files: list[Path],
    is_first_chunk: bool,
) -> list[Path]:
    """Build the ordered list of SQL files to restore for this chunk.

    Args:
        annexe_file: Path to the annexe SQL file, or ``None``.
        department_files: Department SQL files for this chunk.
        is_first_chunk: If True, prepend the annexe file.

    Returns:
        Ordered list of SQL files to execute.
    """
    files: list[Path] = []
    if is_first_chunk and annexe_file is not None:
        files.append(annexe_file)
    files.extend(department_files)
    return files


def _execute_restore_files(sql_files: list[Path]) -> None:
    """Execute each SQL file via psql.

    Args:
        sql_files: Ordered list of SQL file paths to execute.
    """
    for sql_file in sql_files:
        logger.info("Restoring %s ...", sql_file.name)
        _run_psql_file(sql_file)


def _create_views_if_national(data_schema: str | None) -> None:
    """Create compatibility views if the dump uses national format.

    Args:
        data_schema: National data schema name, or ``None`` for demo
            format (in which case this function is a no-op).
    """
    if data_schema is None:
        return
    conn = get_pg_connection()
    try:
        _create_compatibility_views(conn, data_schema)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Per-chunk export and upload
# ---------------------------------------------------------------------------
def _chunk_dir(chunk_index: int) -> Path:
    """Return the path for a chunk's export directory.

    Args:
        chunk_index: Zero-based chunk index (displayed as 1-based).

    Returns:
        Path like ``data/export/chunk_001/``.
    """
    return DATA_EXPORT_DIR / f"{CHUNK_DIR_PREFIX}{chunk_index + 1:03d}"


def _move_exports_to_chunk_dir(chunk_index: int) -> Path:
    """Move exported CSV files from ``DATA_EXPORT_DIR`` to chunk subdirectory.

    Args:
        chunk_index: Zero-based chunk index.

    Returns:
        Path to the chunk subdirectory containing the moved files.
    """
    target = _chunk_dir(chunk_index)
    target.mkdir(parents=True, exist_ok=True)
    for csv_file in DATA_EXPORT_DIR.glob("*.csv"):
        shutil.move(str(csv_file), str(target / csv_file.name))
    return target


def _count_chunk_rows(chunk_dir: Path) -> dict[str, int]:
    """Count data rows per table in a chunk directory (lines minus header).

    Args:
        chunk_dir: Path to the chunk subdirectory containing CSV files.

    Returns:
        Dict mapping table name (CSV stem) to row count.
    """
    counts: dict[str, int] = {}
    for csv_file in sorted(chunk_dir.glob("*.csv")):
        table_name = csv_file.stem
        with open(csv_file, "r", encoding="utf-8") as fh:
            line_count = sum(1 for _ in fh)
        counts[table_name] = max(line_count - 1, 0)
    return counts


def _upload_chunk(chunk_dir: Path, chunk_index: int) -> int:
    """Upload this chunk's files to GCS and clean up local directory.

    After upload, the local chunk directory is deleted to free disk space.

    Args:
        chunk_dir: Path to the chunk subdirectory.
        chunk_index: Zero-based chunk index.

    Returns:
        Number of files uploaded to GCS.
    """
    count = upload_chunk_to_gcs(chunk_dir, chunk_index)
    shutil.rmtree(chunk_dir)
    return count


# ---------------------------------------------------------------------------
# Chunk loop logging
# ---------------------------------------------------------------------------
def _log_chunk_summary(
    chunk_index: int,
    total_chunks: int,
    dept_names: list[str],
    row_counts: dict[str, int],
    elapsed: float,
) -> None:
    """Log a summary after a chunk completes.

    Args:
        chunk_index: Zero-based chunk index.
        total_chunks: Total number of chunks.
        dept_names: Department filenames in this chunk.
        row_counts: Dict of table name to row count.
        elapsed: Wall-clock seconds for this chunk.
    """
    total_rows = sum(row_counts.values())
    logger.info(
        "Chunk %d/%d complete: %d departments, %s rows, %.1f seconds.",
        chunk_index + 1,
        total_chunks,
        len(dept_names),
        f"{total_rows:,}",
        elapsed,
    )


def _update_progress_after_chunk(
    progress: dict[str, Any],
    dept_names: list[str],
    row_counts: dict[str, int],
) -> dict[str, Any]:
    """Update the progress dict with newly completed departments.

    Appends department names to ``completed_departments`` and
    accumulates row counts in ``tables_exported``.

    Args:
        progress: Mutable progress dict (modified in place).
        dept_names: Department filenames completed in this chunk.
        row_counts: Dict of table name to row count for this chunk.

    Returns:
        The updated *progress* dict (same reference).
    """
    progress["completed_departments"].extend(dept_names)
    existing = progress.get("tables_exported", {})
    for table, count in row_counts.items():
        existing[table] = existing.get(table, 0) + count
    progress["tables_exported"] = existing
    return progress


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
def chunked_ingest() -> None:
    """Run chunked full-France ingestion with crash-safe resume.

    Discovers department SQL files, reads progress, groups remaining
    files into chunks, and processes each chunk: reset PG, restore,
    export to chunk directory, upload to GCS, delete local, update progress.
    """
    annexe_file, dept_files = _discover_department_files(DATA_DIR)
    logger.info(
        "Discovered %d department file(s), annexe: %s",
        len(dept_files),
        annexe_file.name if annexe_file else "none",
    )

    progress = _read_progress(DVF_PROGRESS_FILE)
    remaining = _filter_remaining(dept_files, progress["completed_departments"])
    _log_resume_status(remaining, dept_files, progress)

    chunks = _group_into_chunks(remaining, DVF_CHUNK_SIZE)
    _process_all_chunks(annexe_file, chunks, progress)


def _log_resume_status(
    remaining: list[Path],
    all_files: list[Path],
    progress: dict[str, Any],
) -> None:
    """Log how many departments are remaining vs completed.

    Args:
        remaining: Department files still to process.
        all_files: All discovered department files.
        progress: Progress dict (used for context, not modified).
    """
    completed = len(all_files) - len(remaining)
    if completed > 0:
        logger.info(
            "Resuming: %d completed, %d remaining.", completed, len(remaining)
        )
    else:
        logger.info("Starting fresh: %d departments to process.", len(remaining))


def _process_all_chunks(
    annexe_file: Path | None,
    chunks: list[list[Path]],
    progress: dict[str, Any],
) -> None:
    """Process each chunk sequentially.

    Args:
        annexe_file: Path to the annexe SQL file, or ``None``.
        chunks: List of department file groups (one per chunk).
        progress: Mutable progress dict (updated after each chunk).
    """
    if not chunks:
        logger.info("No departments to process.")
        return

    total_completed = len(progress.get("completed_departments", []))
    for idx, chunk_files in enumerate(chunks):
        is_first = total_completed == 0 and idx == 0
        _process_single_chunk(
            annexe_file, chunk_files, is_first, idx, len(chunks), progress
        )


def _reset_if_not_first(is_first_chunk: bool) -> None:
    """Reset data tables if this is not the first chunk.

    The first chunk starts with an empty database, so no reset is needed.
    Subsequent chunks must drop and recreate the data schema to avoid
    accumulating data across chunks.

    Args:
        is_first_chunk: True if this is the first chunk being processed.
    """
    if not is_first_chunk:
        conn = get_pg_connection()
        try:
            _reset_data_tables(conn)
        finally:
            conn.close()


def _finalize_chunk(
    chunk_index: int,
    total_chunks: int,
    dept_names: list[str],
    row_counts: dict[str, int],
    start: float,
    progress: dict[str, Any],
) -> None:
    """Log summary and save progress after a successful chunk.

    Args:
        chunk_index: Zero-based chunk index.
        total_chunks: Total number of chunks.
        dept_names: Department filenames completed in this chunk.
        row_counts: Dict of table name to row count for this chunk.
        start: ``time.monotonic()`` timestamp when the chunk started.
        progress: Mutable progress dict (updated and written to disk).
    """
    elapsed = time.monotonic() - start
    _log_chunk_summary(chunk_index, total_chunks, dept_names, row_counts, elapsed)
    _update_progress_after_chunk(progress, dept_names, row_counts)
    _write_progress(DVF_PROGRESS_FILE, progress)


def _process_single_chunk(
    annexe_file: Path | None,
    chunk_files: list[Path],
    is_first_chunk: bool,
    chunk_index: int,
    total_chunks: int,
    progress: dict[str, Any],
) -> None:
    """Process one chunk: reset PG, restore SQL, export CSV, upload, save.

    This is the core loop body for chunked ingestion. Each call handles
    one batch of department files end-to-end.

    Args:
        annexe_file: Path to the annexe SQL file, or ``None``.
        chunk_files: Department SQL files for this chunk.
        is_first_chunk: True if this is the first chunk (annexe loaded).
        chunk_index: Zero-based chunk index.
        total_chunks: Total number of chunks.
        progress: Mutable progress dict (updated after completion).
    """
    dept_names = [f.name for f in chunk_files]
    logger.info("Chunk %d/%d: processing %s", chunk_index + 1, total_chunks, dept_names)
    start = time.monotonic()
    _reset_if_not_first(is_first_chunk)
    _restore_chunk(annexe_file, chunk_files, is_first_chunk)
    export_tables()
    chunk_path = _move_exports_to_chunk_dir(chunk_index)
    row_counts = _count_chunk_rows(chunk_path)
    _upload_chunk(chunk_path, chunk_index)
    _finalize_chunk(chunk_index, total_chunks, dept_names, row_counts, start, progress)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    """CLI entry point."""
    setup_logging()
    chunked_ingest()


if __name__ == "__main__":
    main()
