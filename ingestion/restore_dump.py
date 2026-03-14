"""Restore DVF+ SQL dump into the ephemeral PostgreSQL container.

Finds .sql files in data/ (or data/1_DONNEES_LIVRAISON/ for the national
format), executes them via ``psql -f``, then verifies that the expected
tables were created and contain data. In demo mode, rows outside the
configured departments are deleted to reduce volume.

Supports two dump formats:
  - Regional (demo): schemas ``dvf`` + ``dvf_annexe``, tables named directly
    (e.g. ``mutation``, ``disposition``).
  - National (full): schemas ``dvf_plus_<year>_<semester>`` +
    ``dvf_plus_annexe``, tables prefixed with ``dvf_plus_`` (e.g.
    ``dvf_plus_mutation``). Compatibility views in ``dvf`` / ``dvf_annexe``
    are created automatically so downstream code works unchanged.
"""

import logging
import re
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
    TABLES_WITH_CODDEP,
    get_pg_connection,
    setup_logging,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# Principal and secondary tables expected after restore (DVF+ open-data).
# These are the *view* names used by export_tables.py (demo naming).
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

# psql command timeout (60 minutes for full national restores).
PSQL_TIMEOUT_SECONDS: int = 3600

# Mapping from demo table names to national ``dvf_plus_`` table names.
_NATIONAL_DATA_TABLES: dict[str, str] = {
    "mutation": "dvf_plus_mutation",
    "disposition": "dvf_plus_disposition",
    "disposition_parcelle": "dvf_plus_disposition_parcelle",
    "local": "dvf_plus_local",
    "parcelle": "dvf_plus_parcelle",
    "adresse": "dvf_plus_adresse",
    "suf": "dvf_plus_suf",
    "lot": "dvf_plus_lot",
    "volume": "dvf_plus_volume",
    "mutation_article_cgi": "dvf_plus_mutation_article_cgi",
    "adresse_dispoparc": "dvf_plus_adresse_dispoparc",
    "adresse_local": "dvf_plus_adresse_local",
}

# Pattern to detect COPY commands referencing dvf_plus_<year>_<sem> schemas.
_NATIONAL_COPY_PATTERN: re.Pattern[str] = re.compile(
    r"^COPY\s+(dvf_plus_\d{4}_\d+)\.(dvf_plus_\w+)\s+\(([^)]+)\)",
)

# Columns known to hold PostGIS geometry data.
_GEOMETRY_COLUMNS: set[str] = {
    "geomlocmut",
    "geomparmut",
    "geompar",
    "geomloc",
}

# Columns known to be PostgreSQL arrays (text[]).
_ARRAY_COLUMN_PREFIXES: tuple[str, ...] = ("l_",)


# ---------------------------------------------------------------------------
# SQL file discovery
# ---------------------------------------------------------------------------
def _sort_sql_files(files: list[Path]) -> list[Path]:
    """Sort SQL files so that schema/init files run before department data.

    Handles both the demo format (``dvf_initial.sql`` first) and the national
    format (``dvf_plus_annexe.sql`` first, then department files in order).
    """

    def priority(path: Path) -> tuple[int, str]:
        name = path.name.lower()
        if "initial" in name or "init" in name:
            return (0, name)
        if "annexe" in name:
            return (0, name)
        return (1, name)

    return sorted(files, key=priority)


def _find_sql_files(directory: Path) -> list[Path]:
    """Return .sql files in *directory* and its livraison subdirectory.

    The national DVF+ dump extracts into ``1_DONNEES_LIVRAISON/``, so we
    search there as well as the top-level directory.
    """
    if not directory.exists():
        return []
    sql_files = list(directory.glob("*.sql"))
    livraison_dir = directory / "1_DONNEES_LIVRAISON"
    if livraison_dir.exists():
        sql_files.extend(livraison_dir.glob("*.sql"))
    if not sql_files:
        return []
    return _sort_sql_files(sql_files)


# ---------------------------------------------------------------------------
# National format detection and DDL generation
# ---------------------------------------------------------------------------
def _detect_national_schema(sql_files: list[Path]) -> str | None:
    """Detect the national data schema name from COPY commands in SQL files.

    Scans department files for COPY commands like
    ``COPY dvf_plus_2025_2.dvf_plus_mutation (...) FROM stdin;``
    and returns the schema name (e.g. ``dvf_plus_2025_2``), or None if
    no national-format COPY commands are found (demo format).
    """
    for sql_file in sql_files:
        if "annexe" in sql_file.name.lower():
            continue
        with open(sql_file, "r", encoding="utf-8") as fh:
            for line in fh:
                match = _NATIONAL_COPY_PATTERN.match(line)
                if match:
                    schema_name = match.group(1)
                    logger.info(
                        "Detected national format: data schema = %s",
                        schema_name,
                    )
                    return schema_name
    return None


def _parse_copy_definitions(
    sql_file: Path,
) -> dict[str, list[str]]:
    """Parse COPY commands from a department SQL file.

    Returns a dict mapping fully-qualified table name
    (e.g. ``dvf_plus_2025_2.dvf_plus_mutation``) to its column list.
    """
    tables: dict[str, list[str]] = {}
    with open(sql_file, "r", encoding="utf-8") as fh:
        for line in fh:
            match = _NATIONAL_COPY_PATTERN.match(line)
            if match:
                schema = match.group(1)
                table = match.group(2)
                columns = [c.strip() for c in match.group(3).split(",")]
                qualified = f"{schema}.{table}"
                if qualified not in tables:
                    tables[qualified] = columns
    return tables


def _column_type(column_name: str) -> str:
    """Infer a PostgreSQL column type from the column name.

    Uses geometry type for known PostGIS columns and text[] for known
    array columns. Defaults to text for everything else -- this is safe
    because COPY accepts text into text columns, and downstream export
    queries handle type coercion.
    """
    if column_name in _GEOMETRY_COLUMNS:
        return "geometry"
    if any(column_name.startswith(prefix) for prefix in _ARRAY_COLUMN_PREFIXES):
        return "text[]"
    return "text"


def _generate_create_table_ddl(
    tables: dict[str, list[str]],
) -> str:
    """Generate CREATE TABLE statements for all national-format tables.

    Returns a single SQL string with CREATE SCHEMA IF NOT EXISTS and
    CREATE TABLE IF NOT EXISTS for each table discovered from COPY commands.
    """
    if not tables:
        return ""

    schemas: set[str] = set()
    ddl_parts: list[str] = []

    for qualified_name, columns in tables.items():
        schema, _table = qualified_name.split(".", 1)
        schemas.add(schema)

        col_defs = []
        for col in columns:
            col_type = _column_type(col)
            col_defs.append(f"    {col} {col_type}")

        ddl_parts.append(
            f"CREATE TABLE IF NOT EXISTS {qualified_name} (\n"
            + ",\n".join(col_defs)
            + "\n);"
        )

    schema_ddl = "\n".join(f"CREATE SCHEMA IF NOT EXISTS {s};" for s in sorted(schemas))
    table_ddl = "\n\n".join(ddl_parts)
    return f"{schema_ddl}\n\n{table_ddl}\n"


def _has_init_file(sql_files: list[Path]) -> bool:
    """Check whether the SQL file list includes an init/schema DDL file."""
    return any(
        "init" in f.name.lower() or "initial" in f.name.lower() for f in sql_files
    )


def _create_data_tables(
    conn: psycopg2.extensions.connection,
    sql_files: list[Path],
) -> str | None:
    """Detect national format and create data tables if needed.

    The national DVF+ dump normally includes an init file
    (``dvf_plus_init.sql``) that creates the data schema and tables. If
    that file is present, this function only detects and returns the
    schema name.  If no init file is found, it parses COPY commands from
    a department file and generates CREATE TABLE DDL as a fallback.

    Returns the detected data schema name, or None for demo format.
    """
    data_schema = _detect_national_schema(sql_files)
    if data_schema is None:
        return None

    if _has_init_file(sql_files):
        logger.info(
            "Init file found -- schema %s tables will be created by it.",
            data_schema,
        )
        return data_schema

    # Fallback: no init file, generate DDL from COPY commands.
    dept_files = [f for f in sql_files if "annexe" not in f.name.lower()]
    if not dept_files:
        logger.warning("National format detected but no department files found.")
        return data_schema

    tables = _parse_copy_definitions(dept_files[0])
    if not tables:
        logger.warning("No COPY commands found in %s.", dept_files[0].name)
        return data_schema

    ddl = _generate_create_table_ddl(tables)
    logger.info(
        "Creating %d data tables in schema %s (no init file found) ...",
        len(tables),
        data_schema,
    )
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    logger.info("Data tables created successfully.")
    return data_schema


# ---------------------------------------------------------------------------
# Compatibility views
# ---------------------------------------------------------------------------
def _create_compatibility_views(
    conn: psycopg2.extensions.connection,
    data_schema: str,
) -> None:
    """Create views in ``dvf`` and ``dvf_annexe`` that map demo-style names.

    After restoring the national dump, tables live in schemas like
    ``dvf_plus_2025_2`` and ``dvf_plus_annexe`` with ``dvf_plus_`` prefixed
    names. This function creates views so that downstream code using
    ``search_path TO dvf, dvf_annexe, public`` finds the expected table
    names (e.g. ``dvf.mutation`` points to ``dvf_plus_2025_2.dvf_plus_mutation``).
    """
    ddl_parts: list[str] = [
        "CREATE SCHEMA IF NOT EXISTS dvf;",
        "CREATE SCHEMA IF NOT EXISTS dvf_annexe;",
    ]

    # Data table views: dvf.<name> -> <data_schema>.dvf_plus_<name>
    for demo_name, national_name in _NATIONAL_DATA_TABLES.items():
        ddl_parts.append(
            f"CREATE OR REPLACE VIEW dvf.{demo_name} AS "
            f"SELECT * FROM {data_schema}.{national_name};"
        )

    # Annexe table views: dvf_annexe.<name> -> dvf_plus_annexe.<name>
    for table in EXPECTED_ANNEXE_TABLES:
        ddl_parts.append(
            f"CREATE OR REPLACE VIEW dvf_annexe.{table} AS "
            f"SELECT * FROM dvf_plus_annexe.{table};"
        )

    # Also create a view for ann_typologie (new in national format).
    ddl_parts.append(
        "CREATE OR REPLACE VIEW dvf_annexe.ann_typologie AS "
        "SELECT * FROM dvf_plus_annexe.ann_typologie;"
    )

    full_ddl = "\n".join(ddl_parts)
    with conn.cursor() as cur:
        cur.execute(full_ddl)
    conn.commit()
    logger.info("Compatibility views created in dvf/dvf_annexe for national format.")


# ---------------------------------------------------------------------------
# psql execution helpers
# ---------------------------------------------------------------------------
def _build_psql_env() -> dict[str, str]:
    """Build environment dict for psql subprocess (with PGPASSWORD)."""
    import os

    env = os.environ.copy()
    env["PGPASSWORD"] = POSTGRES_PASSWORD
    return env


def _build_psql_command(sql_file: Path) -> list[str]:
    """Build the psql command list for executing a SQL file."""
    return [
        "psql",
        "-h",
        POSTGRES_HOST,
        "-p",
        str(POSTGRES_PORT),
        "-U",
        POSTGRES_USER,
        "-d",
        POSTGRES_DB,
        "-f",
        str(sql_file),
        "--set",
        "ON_ERROR_STOP=off",
    ]


def _run_psql_file(sql_file: Path) -> int:
    """Execute a .sql file via ``psql -f`` and return the exit code."""
    cmd = _build_psql_command(sql_file)
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


def _ensure_postgis(conn: psycopg2.extensions.connection) -> None:
    """Ensure the PostGIS extension is enabled."""
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    conn.commit()
    logger.info("PostGIS extension ensured.")


# ---------------------------------------------------------------------------
# Schema listing and table introspection
# ---------------------------------------------------------------------------
DVF_SCHEMAS: list[str] = ["public", "dvf", "dvf_annexe", "dvf_plus_annexe"]


def _get_all_dvf_schemas(conn: psycopg2.extensions.connection) -> list[str]:
    """Return all DVF-relevant schemas that exist in the database.

    Includes the static schemas plus any dynamically-created
    ``dvf_plus_<year>_<sem>`` data schemas.
    """
    query = """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name LIKE 'dvf%%'
           OR schema_name = 'public'
        ORDER BY schema_name;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return [row[0] for row in cur.fetchall()]


def _list_tables(conn: psycopg2.extensions.connection) -> list[str]:
    """Return names of all tables and views in DVF-relevant schemas."""
    schemas = _get_all_dvf_schemas(conn)
    if not schemas:
        return []
    placeholders = ", ".join(["%s"] * len(schemas))
    query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema IN ({placeholders})
          AND table_type IN ('BASE TABLE', 'VIEW')
        ORDER BY table_name;
    """
    with conn.cursor() as cur:
        cur.execute(query, schemas)
        return [row[0] for row in cur.fetchall()]


def _resolve_schema(conn: psycopg2.extensions.connection, table: str) -> str | None:
    """Find the schema containing *table*, searching DVF-relevant schemas."""
    schemas = _get_all_dvf_schemas(conn)
    if not schemas:
        return None
    placeholders = ", ".join(["%s"] * len(schemas))
    query = f"""
        SELECT table_schema
        FROM information_schema.tables
        WHERE table_name = %s
          AND table_schema IN ({placeholders})
        LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(query, [table, *schemas])
        row = cur.fetchone()
        return row[0] if row else None


def _resolve_base_table(
    conn: psycopg2.extensions.connection, table: str
) -> tuple[str, str]:
    """Resolve *table* to its base table name, preferring real tables over views.

    For the national format, demo names (e.g. ``mutation``) exist as views
    in ``dvf``, while the actual base tables are named ``dvf_plus_mutation``
    in ``dvf_plus_*`` schemas. This function returns the (schema, table_name)
    pair for the base table so that DML operations like DELETE work.

    Falls back to the view if no base table is found.
    """
    schemas = _get_all_dvf_schemas(conn)
    if not schemas:
        return ("public", table)

    # First try: find a BASE TABLE with this exact name.
    placeholders = ", ".join(["%s"] * len(schemas))
    query = f"""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_name = %s
          AND table_schema IN ({placeholders})
          AND table_type = 'BASE TABLE'
        LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(query, [table, *schemas])
        row = cur.fetchone()
        if row:
            return (row[0], row[1])

    # Second try: if it's a demo name, look for the dvf_plus_ prefixed base table.
    national_name = _NATIONAL_DATA_TABLES.get(table)
    if national_name:
        query2 = f"""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_name = %s
              AND table_schema IN ({placeholders})
              AND table_type = 'BASE TABLE'
            LIMIT 1;
        """
        with conn.cursor() as cur:
            cur.execute(query2, [national_name, *schemas])
            row = cur.fetchone()
            if row:
                return (row[0], row[1])

    # Fallback: return whatever we can find (including views).
    schema = _resolve_schema(conn, table)
    return (schema or "public", table)


def _count_rows(conn: psycopg2.extensions.connection, table: str) -> int:
    """Return the row count for *table*."""
    schema = _resolve_schema(conn, table)
    qualified = f"{schema}.{table}" if schema else table
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {qualified};")  # noqa: S608
        result = cur.fetchone()
        return result[0] if result else 0


def _table_has_column(
    conn: psycopg2.extensions.connection, table: str, column: str
) -> bool:
    """Check whether *table* has a column named *column*."""
    schemas = _get_all_dvf_schemas(conn)
    if not schemas:
        return False
    placeholders = ", ".join(["%s"] * len(schemas))
    query = f"""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema IN ({placeholders})
          AND table_name = %s
          AND column_name = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, [*schemas, table, column])
        return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# Demo-mode filtering
# ---------------------------------------------------------------------------
def _delete_outside_departments(
    conn: psycopg2.extensions.connection,
    table: str,
    departments: list[str],
) -> None:
    """Delete rows from *table* where coddep is not in *departments*.

    Resolves to the base table (not a view) so DELETE works correctly
    with both demo and national dump formats.
    """
    schema, resolved_name = _resolve_base_table(conn, table)
    qualified = f"{schema}.{resolved_name}"
    placeholders = ", ".join(["%s"] * len(departments))
    delete_sql = f"DELETE FROM {qualified} WHERE coddep NOT IN ({placeholders});"  # noqa: S608
    with conn.cursor() as cur:
        cur.execute(delete_sql, departments)
        deleted = cur.rowcount
    conn.commit()
    logger.info(
        "Table %s: deleted %s rows outside demo departments.",
        table,
        f"{deleted:,}",
    )


def _filter_demo_departments(conn: psycopg2.extensions.connection) -> None:
    """Delete rows outside demo departments from all tables with coddep."""
    departments = DVF_DEMO_DEPARTMENTS
    if not departments:
        logger.warning("DVF_DEMO_DEPARTMENTS is empty -- skipping filter.")
        return

    logger.info("Demo mode: keeping only departments %s", departments)

    for table in TABLES_WITH_CODDEP:
        if not _table_has_column(conn, table, "coddep"):
            logger.info("Table %s has no coddep column -- skipping.", table)
            continue
        _delete_outside_departments(conn, table, departments)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def _check_principal_tables(tables: list[str]) -> bool:
    """Check that all principal tables exist. Return False if any missing."""
    missing = [t for t in EXPECTED_PRINCIPAL_TABLES if t not in tables]
    if missing:
        logger.error("Missing principal tables: %s", missing)
        return False
    return True


def _log_missing_annexe_tables(tables: list[str]) -> None:
    """Log a warning if any annexe tables are missing."""
    missing = [t for t in EXPECTED_ANNEXE_TABLES if t not in tables]
    if missing:
        logger.warning("Missing annexe tables: %s", missing)


def _log_table_row_counts(
    conn: psycopg2.extensions.connection, tables: list[str]
) -> None:
    """Log row counts for each principal table that exists."""
    for table in EXPECTED_PRINCIPAL_TABLES:
        if table in tables:
            count = _count_rows(conn, table)
            logger.info("Table %-25s: %s rows", table, f"{count:,}")


def _verify_restore(conn: psycopg2.extensions.connection) -> bool:
    """Verify the restore produced the expected tables and data.

    Returns True if verification passes, False otherwise.
    """
    tables = _list_tables(conn)
    logger.info("Tables found (%d): %s", len(tables), tables)

    if not _check_principal_tables(tables):
        return False

    _log_missing_annexe_tables(tables)
    _log_table_row_counts(conn, tables)

    mutation_count = _count_rows(conn, "mutation") if "mutation" in tables else 0
    if mutation_count == 0:
        logger.error("mutation table has 0 rows -- restore may have failed.")
        return False

    logger.info(
        "Verification passed: %d tables, %s mutations.",
        len(tables),
        f"{mutation_count:,}",
    )
    return True


# ---------------------------------------------------------------------------
# Main workflow helpers
# ---------------------------------------------------------------------------
def _execute_sql_files(sql_files: list[Path]) -> list[int]:
    """Execute each SQL file via psql and return exit codes."""
    exit_codes: list[int] = []
    for sql_file in sql_files:
        code = _run_psql_file(sql_file)
        exit_codes.append(code)
    return exit_codes


def _check_all_failed(exit_codes: list[int]) -> bool:
    """Return True if all SQL restores returned non-zero exit codes."""
    return len(exit_codes) > 0 and all(c != 0 for c in exit_codes)


def _post_restore_processing(conn: psycopg2.extensions.connection) -> bool:
    """Run demo filtering and verification. Return True if successful."""
    if DVF_MODE == "demo":
        _filter_demo_departments(conn)
    return _verify_restore(conn)


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------
def _get_sql_files_or_exit() -> list[Path]:
    """Return SQL files from DATA_DIR, or exit if none found."""
    sql_files = _find_sql_files(DATA_DIR)
    if not sql_files:
        logger.error("No .sql files found in %s. Run download_dvf.py first.", DATA_DIR)
        sys.exit(1)
    logger.info(
        "Found %d SQL file(s) to restore: %s",
        len(sql_files),
        [f.name for f in sql_files],
    )
    return sql_files


def _prepare_database(sql_files: list[Path]) -> str | None:
    """Ensure PostGIS is available and create national data tables if needed.

    Returns the national data schema name, or None for demo format.
    """
    conn = get_pg_connection()
    try:
        _ensure_postgis(conn)
        data_schema = _create_data_tables(conn, sql_files)
    finally:
        conn.close()
    return data_schema


def _restore_and_verify(
    sql_files: list[Path],
    data_schema: str | None,
) -> None:
    """Execute SQL files, create compat views, apply filtering, and verify."""
    exit_codes = _execute_sql_files(sql_files)
    if _check_all_failed(exit_codes):
        logger.error("All SQL restores returned non-zero exit codes.")
        sys.exit(1)

    conn = get_pg_connection()
    try:
        if data_schema is not None:
            _create_compatibility_views(conn, data_schema)

        with conn.cursor() as cur:
            cur.execute("SET search_path TO dvf, dvf_annexe, public;")
        conn.commit()
        success = _post_restore_processing(conn)
    finally:
        conn.close()

    if not success:
        logger.error("Restore verification failed.")
        sys.exit(1)


def restore_dump() -> None:
    """Find .sql files in data/ and restore them into PostgreSQL."""
    sql_files = _get_sql_files_or_exit()
    data_schema = _prepare_database(sql_files)
    _restore_and_verify(sql_files, data_schema)
    logger.info("Restore complete.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    """CLI entry point."""
    setup_logging()
    restore_dump()


if __name__ == "__main__":
    main()
