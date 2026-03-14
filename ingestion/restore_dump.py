"""Restore DVF+ SQL dump into the ephemeral PostgreSQL container.

Finds .sql files in data/, executes them via ``psql -f``, then verifies
that the expected tables were created and contain data. In demo mode,
rows outside the configured departments are deleted to reduce volume.
"""

import logging
import subprocess
import sys
from pathlib import Path

import psycopg2

from ingestion.config import (
    DATA_DIR,
    DVF_DEMO_DEPARTMENTS,
    DVF_MODE,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# Principal and secondary tables expected after restore (DVF+ open-data).
EXPECTED_PRINCIPAL_TABLES: list[str] = [
    "mutation",
    "disposition",
    "disposition_parcelle",
    "local",
    "parcelle",
    "adresse",
]

EXPECTED_SECONDARY_TABLES: list[str] = [
    "suf",
    "lot",
    "volume",
    "mutation_article_cgi",
    "adresse_dispoparc",
    "adresse_local",
]

EXPECTED_ANNEXE_TABLES: list[str] = [
    "ann_nature_mutation",
    "ann_type_local",
    "ann_cgi",
    "ann_nature_culture",
    "ann_nature_culture_speciale",
]

ALL_EXPECTED_TABLES: list[str] = (
    EXPECTED_PRINCIPAL_TABLES + EXPECTED_SECONDARY_TABLES + EXPECTED_ANNEXE_TABLES
)

# Tables that have a coddep column (for demo-mode filtering).
TABLES_WITH_CODDEP: list[str] = [
    "mutation",
    "disposition",
    "disposition_parcelle",
    "local",
    "parcelle",
    "adresse",
    "suf",
    "lot",
    "volume",
    "mutation_article_cgi",
    "adresse_dispoparc",
    "adresse_local",
]

# psql command timeout (10 minutes for large restores).
PSQL_TIMEOUT_SECONDS: int = 600


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _find_sql_files(directory: Path) -> list[Path]:
    """Return .sql files in *directory*, sorted by name."""
    if not directory.exists():
        return []
    return sorted(directory.glob("*.sql"))


def _build_psql_env() -> dict[str, str]:
    """Build environment dict for psql subprocess (with PGPASSWORD)."""
    import os

    env = os.environ.copy()
    env["PGPASSWORD"] = POSTGRES_PASSWORD
    return env


def _run_psql_file(sql_file: Path) -> int:
    """Execute a .sql file via ``psql -f`` and return the exit code."""
    cmd = [
        "psql",
        "-h", POSTGRES_HOST,
        "-p", str(POSTGRES_PORT),
        "-U", POSTGRES_USER,
        "-d", POSTGRES_DB,
        "-f", str(sql_file),
        "--set", "ON_ERROR_STOP=off",
    ]
    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        env=_build_psql_env(),
        capture_output=True,
        text=True,
        timeout=PSQL_TIMEOUT_SECONDS,
    )
    if result.returncode != 0:
        logger.warning(
            "psql exited with code %d for %s.\nstderr (last 2000 chars):\n%s",
            result.returncode,
            sql_file.name,
            result.stderr[-2000:] if result.stderr else "(empty)",
        )
    else:
        logger.info("psql completed successfully for %s", sql_file.name)
    return result.returncode


def _get_connection() -> psycopg2.extensions.connection:
    """Open a psycopg2 connection to the DVF database."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )


def _ensure_postgis(conn: psycopg2.extensions.connection) -> None:
    """Ensure the PostGIS extension is enabled."""
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    conn.commit()
    logger.info("PostGIS extension ensured.")


def _list_public_tables(conn: psycopg2.extensions.connection) -> list[str]:
    """Return names of all tables in the public schema."""
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return [row[0] for row in cur.fetchall()]


def _count_rows(conn: psycopg2.extensions.connection, table: str) -> int:
    """Return the row count for *table*."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table};")  # noqa: S608 — table name from constant list
        result = cur.fetchone()
        return result[0] if result else 0


def _table_has_column(
    conn: psycopg2.extensions.connection, table: str, column: str
) -> bool:
    """Check whether *table* has a column named *column*."""
    query = """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (table, column))
        return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# Demo-mode filtering
# ---------------------------------------------------------------------------
def _filter_demo_departments(conn: psycopg2.extensions.connection) -> None:
    """Delete rows outside demo departments from all tables with coddep."""
    departments = DVF_DEMO_DEPARTMENTS
    if not departments:
        logger.warning("DVF_DEMO_DEPARTMENTS is empty — skipping filter.")
        return

    logger.info(
        "Demo mode: keeping only departments %s", departments
    )

    for table in TABLES_WITH_CODDEP:
        if not _table_has_column(conn, table, "coddep"):
            logger.info("Table %s has no coddep column — skipping.", table)
            continue

        # Use parameterised IN clause.
        placeholders = ", ".join(["%s"] * len(departments))
        delete_sql = (
            f"DELETE FROM {table} WHERE coddep NOT IN ({placeholders});"  # noqa: S608
        )
        with conn.cursor() as cur:
            cur.execute(delete_sql, departments)
            deleted = cur.rowcount
        conn.commit()
        logger.info("Table %s: deleted %s rows outside demo departments.", table, f"{deleted:,}")


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def _verify_restore(conn: psycopg2.extensions.connection) -> bool:
    """Verify the restore produced the expected tables and data.

    Returns True if verification passes, False otherwise.
    """
    tables = _list_public_tables(conn)
    logger.info("Tables found in public schema (%d): %s", len(tables), tables)

    missing_principal = [
        t for t in EXPECTED_PRINCIPAL_TABLES if t not in tables
    ]
    if missing_principal:
        logger.error("Missing principal tables: %s", missing_principal)
        return False

    missing_annexe = [t for t in EXPECTED_ANNEXE_TABLES if t not in tables]
    if missing_annexe:
        logger.warning("Missing annexe tables: %s", missing_annexe)

    # Row counts for key tables.
    for table in EXPECTED_PRINCIPAL_TABLES:
        if table in tables:
            count = _count_rows(conn, table)
            logger.info("Table %-25s: %s rows", table, f"{count:,}")

    mutation_count = _count_rows(conn, "mutation") if "mutation" in tables else 0
    if mutation_count == 0:
        logger.error("mutation table has 0 rows — restore may have failed.")
        return False

    logger.info("Verification passed: %d tables, %s mutations.", len(tables), f"{mutation_count:,}")
    return True


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------
def restore_dump() -> None:
    """Find .sql files in data/ and restore them into PostgreSQL."""
    sql_files = _find_sql_files(DATA_DIR)
    if not sql_files:
        logger.error(
            "No .sql files found in %s. Run download_dvf.py first.", DATA_DIR
        )
        sys.exit(1)

    logger.info("Found %d SQL file(s) to restore: %s", len(sql_files), [f.name for f in sql_files])

    # Ensure PostGIS is available before restoring.
    conn = _get_connection()
    try:
        _ensure_postgis(conn)
    finally:
        conn.close()

    # Execute each SQL file via psql.
    exit_codes: list[int] = []
    for sql_file in sql_files:
        code = _run_psql_file(sql_file)
        exit_codes.append(code)

    # Some warnings are expected (e.g. "relation already exists").
    # We only fail hard if ALL files had non-zero exit codes.
    all_failed = all(c != 0 for c in exit_codes)
    if all_failed and len(exit_codes) > 0:
        logger.error("All SQL restores returned non-zero exit codes.")
        sys.exit(1)

    # Demo-mode filtering.
    conn = _get_connection()
    try:
        if DVF_MODE == "demo":
            _filter_demo_departments(conn)

        # Verify the restore.
        success = _verify_restore(conn)
    finally:
        conn.close()

    if not success:
        logger.error("Restore verification failed.")
        sys.exit(1)

    logger.info("Restore complete.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    restore_dump()


if __name__ == "__main__":
    main()
