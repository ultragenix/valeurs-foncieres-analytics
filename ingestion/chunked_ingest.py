"""Chunked full-France DVF+ ingestion with crash-safe resume.

Processes department SQL files in configurable batches to avoid
PostgreSQL WAL overflow and shared memory exhaustion. Each chunk
cycle: restore into PG, export to CSV (append mode), upload to GCS.
A JSON progress file tracks completed departments for resume.
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
from ingestion.export_tables import ALL_EXPORT_TABLES, export_tables
from ingestion.restore_dump import (
    _create_compatibility_views,
    _create_data_tables,
    _ensure_postgis,
    _find_sql_files,
    _run_psql_file,
)
from ingestion.upload_to_gcs import upload_to_gcs

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# Subdirectory where national DVF+ dump extracts SQL files.
LIVRAISON_SUBDIR: str = "1_DONNEES_LIVRAISON"


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------
def _discover_department_files(
    data_dir: Path,
) -> tuple[Path | None, list[Path]]:
    """Find the annexe SQL file and department SQL files.

    Searches both *data_dir* and its ``1_DONNEES_LIVRAISON/``
    subdirectory. The annexe file is identified by ``annexe`` in its
    filename. Returns ``(annexe_path, sorted_department_files)``.
    """
    all_sql = _find_sql_files(data_dir)
    annexe_file = _extract_annexe_file(all_sql)
    dept_files = _extract_department_files(all_sql)
    return annexe_file, dept_files


def _extract_annexe_file(sql_files: list[Path]) -> Path | None:
    """Return the first SQL file containing 'annexe' in its name."""
    for sql_file in sql_files:
        if "annexe" in sql_file.name.lower():
            return sql_file
    return None


def _extract_department_files(sql_files: list[Path]) -> list[Path]:
    """Return SQL files that are NOT the annexe file, sorted by name."""
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
    killed mid-write.
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
    """Exclude already-completed department files by filename."""
    completed_set = set(completed)
    return [f for f in department_files if f.name not in completed_set]


def _group_into_chunks(
    files: list[Path],
    chunk_size: int,
) -> list[list[Path]]:
    """Split *files* into groups of *chunk_size*."""
    return [
        files[i : i + chunk_size]
        for i in range(0, len(files), chunk_size)
    ]


# ---------------------------------------------------------------------------
# PostgreSQL reset
# ---------------------------------------------------------------------------
def _get_dvf_data_schemas(conn: Any) -> list[str]:
    """Return all dvf_plus_* data schemas (excluding dvf_plus_annexe)."""
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
    """DROP dvf_plus_* data schemas and dvf compatibility views.

    Preserves ``dvf_plus_annexe`` (annexe data is shared across all
    chunks). Recreates empty ``dvf`` schema for compatibility views.
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
    compatibility views.
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
    """Build the ordered list of SQL files to restore for this chunk."""
    files: list[Path] = []
    if is_first_chunk and annexe_file is not None:
        files.append(annexe_file)
    files.extend(department_files)
    return files


def _execute_restore_files(sql_files: list[Path]) -> None:
    """Execute each SQL file via psql."""
    for sql_file in sql_files:
        logger.info("Restoring %s ...", sql_file.name)
        _run_psql_file(sql_file)


def _create_views_if_national(data_schema: str | None) -> None:
    """Create compatibility views if the dump uses national format."""
    if data_schema is None:
        return
    conn = get_pg_connection()
    try:
        _create_compatibility_views(conn, data_schema)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Chunk export (CSV append mode)
# ---------------------------------------------------------------------------
def _export_chunk(is_first_chunk: bool) -> dict[str, int]:
    """Export all tables to CSV, appending for subsequent chunks.

    First chunk: exports normally (writes CSV headers). Subsequent
    chunks: exports to temp files, then appends rows (skipping
    the header line) to the main CSV files. Returns row counts per
    table.
    """
    if is_first_chunk:
        export_tables()
        return _count_exported_rows()

    _export_to_temp_files()
    row_counts = _append_temp_to_main()
    _cleanup_temp_files()
    return row_counts


def _count_exported_rows() -> dict[str, int]:
    """Count rows in each exported CSV file (excluding header)."""
    counts: dict[str, int] = {}
    for table in ALL_EXPORT_TABLES:
        csv_path = DATA_EXPORT_DIR / f"{table}.csv"
        if csv_path.exists():
            counts[table] = _count_file_data_lines(csv_path)
    return counts


def _count_file_data_lines(csv_path: Path) -> int:
    """Count data lines in a CSV file (total lines minus header)."""
    with open(csv_path, "r", encoding="utf-8") as fh:
        line_count = sum(1 for _ in fh)
    return max(line_count - 1, 0)


def _export_to_temp_files() -> None:
    """Export tables to temporary CSV files for append processing.

    Renames existing main CSV files out of the way, runs the normal
    export (which writes to the main filenames), then renames the
    new files to ``{table}_chunk.csv`` and restores the originals.
    """
    _rename_main_to_backup()
    export_tables()
    _rename_exported_to_chunk()
    _restore_backup_to_main()


def _rename_main_to_backup() -> None:
    """Rename existing main CSV files to .bak for safekeeping."""
    for table in ALL_EXPORT_TABLES:
        main = DATA_EXPORT_DIR / f"{table}.csv"
        if main.exists():
            main.rename(DATA_EXPORT_DIR / f"{table}.csv.bak")


def _rename_exported_to_chunk() -> None:
    """Rename freshly exported CSV files to _chunk.csv."""
    for table in ALL_EXPORT_TABLES:
        exported = DATA_EXPORT_DIR / f"{table}.csv"
        if exported.exists():
            exported.rename(DATA_EXPORT_DIR / f"{table}_chunk.csv")


def _restore_backup_to_main() -> None:
    """Restore .bak files back to their original names."""
    for table in ALL_EXPORT_TABLES:
        backup = DATA_EXPORT_DIR / f"{table}.csv.bak"
        if backup.exists():
            backup.rename(DATA_EXPORT_DIR / f"{table}.csv")


def _append_temp_to_main() -> dict[str, int]:
    """Append rows from chunk temp files to main CSV files.

    Skips the first line (header) of each temp file. Returns the
    number of appended rows per table.
    """
    row_counts: dict[str, int] = {}
    for table in ALL_EXPORT_TABLES:
        chunk_path = DATA_EXPORT_DIR / f"{table}_chunk.csv"
        main_path = DATA_EXPORT_DIR / f"{table}.csv"
        if chunk_path.exists() and main_path.exists():
            row_counts[table] = _append_skipping_header(chunk_path, main_path)
    return row_counts


def _append_skipping_header(chunk_path: Path, main_path: Path) -> int:
    """Append all lines except the first from *chunk_path* to *main_path*.

    Returns the number of lines appended.
    """
    appended = 0
    with open(chunk_path, "r", encoding="utf-8") as src:
        next(src, None)  # skip header
        with open(main_path, "a", encoding="utf-8") as dst:
            for line in src:
                dst.write(line)
                appended += 1
    return appended


def _cleanup_temp_files() -> None:
    """Delete temporary chunk CSV files."""
    for table in ALL_EXPORT_TABLES:
        chunk_path = DATA_EXPORT_DIR / f"{table}_chunk.csv"
        chunk_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# GCS upload
# ---------------------------------------------------------------------------
def _upload_current_state() -> int:
    """Upload the current accumulated CSV files to GCS.

    Returns the number of files uploaded. Each upload overwrites the
    previous version in the bucket.
    """
    return upload_to_gcs()


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
    """Log a summary after a chunk completes."""
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
    """Update the progress dict with newly completed departments."""
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
    export (append), upload to GCS, update progress.
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
    """Log how many departments are remaining vs completed."""
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
    """Process each chunk sequentially."""
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
    """Reset data tables if this is not the first chunk."""
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
    """Log summary and save progress after a successful chunk."""
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
    """Process one chunk: reset, restore, export, upload, save progress."""
    dept_names = [f.name for f in chunk_files]
    logger.info("Chunk %d/%d: processing %s", chunk_index + 1, total_chunks, dept_names)
    start = time.monotonic()
    _reset_if_not_first(is_first_chunk)
    _restore_chunk(annexe_file, chunk_files, is_first_chunk)
    row_counts = _export_chunk(is_first_chunk)
    _upload_current_state()
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
