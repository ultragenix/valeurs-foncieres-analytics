"""Export DVF+ tables from PostgreSQL to CSV files.

Handles PostGIS geometry extraction (point centroids to lat/lon floats),
PostgreSQL array columns (to first-element scalars), and drops heavy
polygon geometry columns not needed downstream in BigQuery.

Inputs:
    - Populated PostgreSQL tables in ``dvf`` / ``dvf_annexe`` schemas.
Outputs:
    - CSV files in ``data/export/`` (one per table).

Dependencies:
    psycopg2 -- for database queries and ``COPY ... TO STDOUT``.
    tqdm     -- for export progress bar.
"""

import csv
import logging
import re
import sys
from pathlib import Path

import psycopg2
from tqdm import tqdm

from ingestion.config import (
    DATA_EXPORT_DIR,
    DVF_DEMO_DEPARTMENTS,
    DVF_MODE,
    TABLES_WITH_CODDEP,
    get_pg_connection,
    setup_logging,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# Tables exported with simple COPY (no geometry, no array transforms).
SIMPLE_TABLES: list[str] = [
    "disposition",
    "local",
    "adresse",
    "ann_nature_mutation",
    "ann_type_local",
    "ann_cgi",
    "ann_nature_culture",
    "ann_nature_culture_speciale",
]

# Geometry columns to exclude from parcelle and disposition_parcelle exports.
PARCELLE_EXCLUDE_COLUMNS: list[str] = ["geompar", "geomparmut"]
DISPOSITION_PARCELLE_EXCLUDE_COLUMNS: list[str] = ["geomloc", "geompar"]

# Regex pattern for valid French department codes (e.g., "01", "75", "2A", "974").
DEPARTMENT_CODE_PATTERN: re.Pattern[str] = re.compile(r"^[0-9]{2,3}[A-B]?$")

# All tables in export order.
ALL_EXPORT_TABLES: list[str] = [
    "mutation",
    "parcelle",
    "disposition_parcelle",
    *SIMPLE_TABLES,
]


# ---------------------------------------------------------------------------
# Column introspection
# ---------------------------------------------------------------------------
DVF_SCHEMAS: list[str] = ["public", "dvf", "dvf_annexe", "dvf_plus_annexe"]


def _set_search_path(conn: psycopg2.extensions.connection) -> None:
    """Set ``search_path`` to include all DVF-relevant schemas.

    Args:
        conn: Open psycopg2 connection.
    """
    with conn.cursor() as cur:
        cur.execute("SET search_path TO dvf, dvf_annexe, public;")
    conn.commit()


def _get_table_columns(
    conn: psycopg2.extensions.connection, table_name: str
) -> list[str]:
    """Return the ordered list of column names for *table_name*.

    Searches across all schemas listed in ``DVF_SCHEMAS``.

    Args:
        conn: Open psycopg2 connection.
        table_name: Unqualified table name.

    Returns:
        List of column names in ordinal position order.
    """
    placeholders = ", ".join(["%s"] * len(DVF_SCHEMAS))
    query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema IN ({placeholders})
          AND table_name = %s
        ORDER BY ordinal_position;
    """
    with conn.cursor() as cur:
        cur.execute(query, [*DVF_SCHEMAS, table_name])
        return [row[0] for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Query builders
# ---------------------------------------------------------------------------
def _validate_department_codes(departments: list[str]) -> None:
    """Validate that all department codes match the expected French pattern.

    Valid codes: ``01``-``95``, ``2A``, ``2B``, ``971``-``976``.

    Args:
        departments: List of department code strings to validate.

    Raises:
        ValueError: If any code does not match ``DEPARTMENT_CODE_PATTERN``.
    """
    for code in departments:
        if not DEPARTMENT_CODE_PATTERN.match(code):
            msg = f"Invalid department code: {code!r}"
            raise ValueError(msg)


def _build_where_clause(table_name: str, departments: list[str] | None) -> str:
    """Build a WHERE clause filtering by department codes, or empty string.

    Validates department codes against a strict regex before embedding
    them in SQL to prevent injection.

    Args:
        table_name: Table to filter (only tables in ``TABLES_WITH_CODDEP``
            are filtered).
        departments: List of department codes, or ``None`` for no filter.

    Returns:
        SQL WHERE clause string (e.g. `` WHERE coddep IN ('75','13')``),
        or empty string if no filter applies.
    """
    if not departments:
        return ""
    if table_name not in TABLES_WITH_CODDEP:
        return ""
    _validate_department_codes(departments)
    placeholders = ", ".join(f"'{d}'" for d in departments)
    return f" WHERE coddep IN ({placeholders})"


# Array columns replaced by their first element (PG arrays -> scalar).
# Maps original column name to the SQL expression with alias.
_MUTATION_ARRAY_COLS: dict[str, str] = {
    "l_codinsee": "l_codinsee[1] AS codinsee",
    "l_section": "l_section[1] AS section",
    "l_par": "l_par[1] AS par",
    "l_artcgi": "l_artcgi[1] AS artcgi",
}

# Geometry columns transformed to WGS84 lat/lon via PostGIS.
# ST_Centroid handles multi-point geometries; ST_Transform reprojects
# from the source CRS to EPSG:4326 (standard lat/lon).
_MUTATION_GEOM_COLS: dict[str, str] = {
    "geomlocmut": (
        "ST_Y(ST_Transform(ST_Centroid(geomlocmut), 4326)) AS latitude,"
        " ST_X(ST_Transform(ST_Centroid(geomlocmut), 4326)) AS longitude"
    ),
}

# Columns to skip entirely in mutation export.  Includes array columns
# (handled above), geometry columns (handled above or too heavy), and
# columns not needed in BigQuery (internal identifiers, document refs).
_MUTATION_SKIP_COLS: set[str] = {
    *_MUTATION_ARRAY_COLS.keys(),
    *_MUTATION_GEOM_COLS.keys(),
    "geomparmut",      # Heavy polygon geometry (parcel mutation boundary)
    "geompar",         # Heavy polygon geometry (parcel boundary)
    "codservch",       # Internal service code (not needed downstream)
    "refdoc",          # Document reference (not needed downstream)
    "idmutinvar",      # Invariant mutation ID (superseded by idmutation)
    "l_dcnt",          # Array of land use counts (too granular)
    "l_idpar",         # Array of parcel IDs (joined via parcelle table)
    "l_idparmut",      # Array of mutation parcel IDs
    "l_idlocmut",      # Array of mutation local IDs
}


def _build_mutation_query(
    conn: psycopg2.extensions.connection,
    departments: list[str] | None,
) -> str:
    """Build the SELECT query for the ``mutation`` table.

    Dynamically adapts to the columns present in the actual database:
    - PostGIS geometry columns are transformed to ``latitude``/``longitude``
      floats via ``ST_Centroid`` + ``ST_Transform`` to EPSG:4326.
    - Array columns (``l_*``) are reduced to their first element.
    - Heavy/unused columns are dropped entirely.

    Args:
        conn: Open psycopg2 connection (for column introspection).
        departments: Department codes for WHERE clause, or ``None``.

    Returns:
        A complete SQL SELECT query string.
    """
    all_cols = _get_table_columns(conn, "mutation")
    select_parts: list[str] = []

    for col in all_cols:
        if col in _MUTATION_SKIP_COLS:
            if col in _MUTATION_ARRAY_COLS:
                select_parts.append(_MUTATION_ARRAY_COLS[col])
            elif col in _MUTATION_GEOM_COLS:
                select_parts.append(_MUTATION_GEOM_COLS[col])
            continue
        select_parts.append(col)

    where = _build_where_clause("mutation", departments)
    return f"SELECT {', '.join(select_parts)} FROM mutation{where}"


def _build_parcelle_query(
    conn: psycopg2.extensions.connection,
    departments: list[str] | None,
) -> str:
    """Build a SELECT for ``parcelle``, excluding heavy geometry columns.

    Args:
        conn: Open psycopg2 connection (for column introspection).
        departments: Department codes for WHERE clause, or ``None``.

    Returns:
        A complete SQL SELECT query string.
    """
    all_cols = _get_table_columns(conn, "parcelle")
    keep = [c for c in all_cols if c not in PARCELLE_EXCLUDE_COLUMNS]
    cols_sql = ", ".join(keep)
    where = _build_where_clause("parcelle", departments)
    return f"SELECT {cols_sql} FROM parcelle{where}"


def _build_disposition_parcelle_query(
    conn: psycopg2.extensions.connection,
    departments: list[str] | None,
) -> str:
    """Build a SELECT for ``disposition_parcelle``, excluding geometry columns.

    Args:
        conn: Open psycopg2 connection (for column introspection).
        departments: Department codes for WHERE clause, or ``None``.

    Returns:
        A complete SQL SELECT query string.
    """
    all_cols = _get_table_columns(conn, "disposition_parcelle")
    keep = [c for c in all_cols if c not in DISPOSITION_PARCELLE_EXCLUDE_COLUMNS]
    cols_sql = ", ".join(keep)
    where = _build_where_clause("disposition_parcelle", departments)
    return f"SELECT {cols_sql} FROM disposition_parcelle{where}"


def _build_simple_query(
    table_name: str,
    departments: list[str] | None,
) -> str:
    """Build a simple ``SELECT *`` query, optionally filtered by department.

    Args:
        table_name: Unqualified table name.
        departments: Department codes for WHERE clause, or ``None``.

    Returns:
        A complete SQL SELECT query string.
    """
    where = _build_where_clause(table_name, departments)
    return f"SELECT * FROM {table_name}{where}"


# ---------------------------------------------------------------------------
# Export engine
# ---------------------------------------------------------------------------
def _export_table(
    conn: psycopg2.extensions.connection,
    table_name: str,
    query: str,
    output_path: Path,
) -> int:
    """Export a query result to CSV via ``COPY (...) TO STDOUT WITH CSV HEADER``.

    Args:
        conn: Open psycopg2 connection.
        table_name: Table name (used for logging only).
        query: SQL SELECT query whose results will be exported.
        output_path: Destination CSV file path (parent dirs created).

    Returns:
        Number of data rows written (excluding the header).
    """
    copy_sql = f"COPY ({query}) TO STDOUT WITH CSV HEADER"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as fh:
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, fh)

    row_count = _count_csv_rows(output_path)
    logger.info(
        "Exported %-30s -> %s (%s rows)",
        table_name,
        output_path.name,
        f"{row_count:,}",
    )
    return row_count


def _count_csv_rows(csv_path: Path) -> int:
    """Count data rows in a CSV file using ``csv.reader``.

    Handles multiline quoted fields correctly by counting actual CSV
    records rather than raw lines. Subtracts 1 for the header row.

    Args:
        csv_path: Path to the CSV file.

    Returns:
        Number of data rows (total records minus header), minimum 0.
    """
    csv.field_size_limit(10_000_000)  # 10 MB for large geometry WKT fields
    with open(csv_path, "r", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        row_count = sum(1 for _ in reader)
    return max(row_count - 1, 0)


# ---------------------------------------------------------------------------
# Department filter resolution
# ---------------------------------------------------------------------------
def _resolve_departments() -> list[str] | None:
    """Return demo department codes if in demo mode, else ``None``.

    Returns:
        List of department codes from ``DVF_DEMO_DEPARTMENTS`` when
        ``DVF_MODE`` is ``"demo"``, otherwise ``None`` (no filtering).
    """
    if DVF_MODE == "demo":
        logger.info(
            "Demo mode: filtering exports to departments %s", DVF_DEMO_DEPARTMENTS
        )
        return DVF_DEMO_DEPARTMENTS
    return None


# ---------------------------------------------------------------------------
# Table query dispatch
# ---------------------------------------------------------------------------
def _build_query_for_table(
    conn: psycopg2.extensions.connection,
    table_name: str,
    departments: list[str] | None,
) -> str:
    """Return the appropriate SQL query for exporting *table_name*.

    Dispatches to table-specific query builders for ``mutation``,
    ``parcelle``, and ``disposition_parcelle``; falls back to a simple
    ``SELECT *`` for all other tables.

    Args:
        conn: Open psycopg2 connection.
        table_name: Unqualified table name.
        departments: Department codes for filtering, or ``None``.

    Returns:
        A complete SQL SELECT query string.
    """
    if table_name == "mutation":
        return _build_mutation_query(conn, departments)
    if table_name == "parcelle":
        return _build_parcelle_query(conn, departments)
    if table_name == "disposition_parcelle":
        return _build_disposition_parcelle_query(conn, departments)
    return _build_simple_query(table_name, departments)


def _table_exists(conn: psycopg2.extensions.connection, table_name: str) -> bool:
    """Check whether *table_name* exists in any DVF-relevant schema.

    Args:
        conn: Open psycopg2 connection.
        table_name: Unqualified table name.

    Returns:
        True if the table exists in any of ``DVF_SCHEMAS``.
    """
    placeholders = ", ".join(["%s"] * len(DVF_SCHEMAS))
    query = f"""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema IN ({placeholders})
          AND table_name = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, [*DVF_SCHEMAS, table_name])
        return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def export_tables() -> None:
    """Export all DVF+ tables to CSV files in DATA_EXPORT_DIR."""
    departments = _resolve_departments()
    DATA_EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    conn = get_pg_connection()
    try:
        _set_search_path(conn)
        _export_all_tables(conn, departments)
    finally:
        conn.close()

    logger.info("All exports complete. Output directory: %s", DATA_EXPORT_DIR)


def _export_all_tables(
    conn: psycopg2.extensions.connection,
    departments: list[str] | None,
) -> None:
    """Iterate over all tables and export each one to CSV.

    Args:
        conn: Open psycopg2 connection.
        departments: Department codes for filtering, or ``None``.
    """
    total_rows = 0
    progress = tqdm(ALL_EXPORT_TABLES, desc="Exporting tables", unit="table")

    for table_name in progress:
        progress.set_postfix_str(table_name)
        total_rows += _export_single_table(conn, table_name, departments)

    logger.info("Total rows exported across all tables: %s", f"{total_rows:,}")


def _export_single_table(
    conn: psycopg2.extensions.connection,
    table_name: str,
    departments: list[str] | None,
) -> int:
    """Export a single table to CSV, skipping if it does not exist.

    Args:
        conn: Open psycopg2 connection.
        table_name: Unqualified table name.
        departments: Department codes for filtering, or ``None``.

    Returns:
        Number of data rows exported, or 0 if the table was skipped.
    """
    if not _table_exists(conn, table_name):
        logger.warning("Table %s does not exist -- skipping.", table_name)
        return 0

    query = _build_query_for_table(conn, table_name, departments)
    output_path = DATA_EXPORT_DIR / f"{table_name}.csv"
    return _export_table(conn, table_name, query, output_path)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    """CLI entry point."""
    setup_logging()
    try:
        export_tables()
    except psycopg2.OperationalError as exc:
        logger.error("Cannot connect to PostgreSQL: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
