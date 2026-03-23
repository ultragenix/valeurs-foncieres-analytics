"""Re-ingest departments 01-10 that were lost during chunked resume.

Targeted script that restores only the missing department SQL files into
PostgreSQL, exports to CSV, and uploads to GCS as chunk_000 (a slot that
does not collide with existing chunks 001-009).

After running this script, execute:
    make bq-load      # reloads ALL chunks (000-009) via wildcard
    make dbt-build    # rebuilds analytics tables

Prerequisites:
    - PostgreSQL container must be running (make docker-up)
    - GCP credentials configured (GOOGLE_APPLICATION_CREDENTIALS)
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from ingestion.config import (
    DATA_DIR,
    DATA_EXPORT_DIR,
    get_pg_connection,
    setup_logging,
)
from ingestion.restore_dump import (
    _create_compatibility_views,
    _create_data_tables,
    _ensure_postgis,
    _run_psql_file,
)
from ingestion.export_tables import export_tables
from ingestion.upload_to_gcs import upload_chunk_to_gcs

logger = logging.getLogger(__name__)

LIVRAISON_DIR = DATA_DIR / "1_DONNEES_LIVRAISON"

# Departments to re-ingest (01-10, missing from BigQuery)
MISSING_DEPT_FILES = [f"dvf_plus_d{i:02d}.sql" for i in range(1, 11)]
INIT_FILE = "dvf_plus_init.sql"
ANNEXE_FILE = "dvf_plus_annexe.sql"

# Upload as chunk index 0 -> chunk_000.csv in GCS (no collision with 001-009)
TARGET_CHUNK_INDEX = -1  # will produce chunk_000


def _resolve_files() -> tuple[list[Path], Path, Path]:
    """Find init, annexe, and department SQL files on disk."""
    init = LIVRAISON_DIR / INIT_FILE
    annexe = LIVRAISON_DIR / ANNEXE_FILE
    dept_files = [LIVRAISON_DIR / f for f in MISSING_DEPT_FILES]

    for f in [init, annexe] + dept_files:
        if not f.exists():
            msg = f"Required file not found: {f}"
            raise FileNotFoundError(msg)

    return dept_files, init, annexe


def _restore_files(dept_files: list[Path], init: Path, annexe: Path) -> None:
    """Restore init + annexe + department files into PostgreSQL."""
    all_files = [init, annexe] + dept_files

    conn = get_pg_connection()
    try:
        _ensure_postgis(conn)
        data_schema = _create_data_tables(conn, all_files)
    finally:
        conn.close()

    for sql_file in all_files:
        logger.info("Restoring %s ...", sql_file.name)
        _run_psql_file(sql_file)

    if data_schema:
        conn = get_pg_connection()
        try:
            _create_compatibility_views(conn, data_schema)
        finally:
            conn.close()


def _export_and_upload() -> None:
    """Export PostgreSQL tables to CSV and upload as chunk_000."""
    export_tables()

    chunk_dir = DATA_EXPORT_DIR / "chunk_000"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    for csv_file in DATA_EXPORT_DIR.glob("*.csv"):
        shutil.move(str(csv_file), str(chunk_dir / csv_file.name))

    count = upload_chunk_to_gcs(chunk_dir, TARGET_CHUNK_INDEX)
    logger.info("Uploaded %d files as chunk_000.", count)

    shutil.rmtree(chunk_dir)


def main() -> None:
    """Re-ingest departments 01-10."""
    setup_logging()
    logger.info("=== Re-ingesting departments 01-10 ===")

    dept_files, init, annexe = _resolve_files()
    logger.info("Found %d department files + init + annexe.", len(dept_files))

    _restore_files(dept_files, init, annexe)
    _export_and_upload()

    logger.info("=== Done. Now run: make bq-load && make dbt-build ===")


if __name__ == "__main__":
    main()
