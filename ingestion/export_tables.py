"""Export DVF+ tables from PostgreSQL to CSV files.

Handles PostGIS geometry extraction (point to lat/lon floats),
PostgreSQL array columns (to first-element or comma-separated strings),
and drops heavy polygon geometry columns not needed for BigQuery.
"""

import logging
import sys
from pathlib import Path

import psycopg2
from tqdm import tqdm

from ingestion.config import (
    DATA_EXPORT_DIR,
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

# Tables exported with simple COPY (no geometry, no array transforms).
SIMPLE_TABLES: list[str] = [
    "disposition",
    "local",
    "disposition_parcelle",
    "adresse",
    "ann_nature_mutation",
    "ann_type_local",
    "ann_cgi",
    "ann_nature_culture",
    "ann_nature_culture_speciale",
]

# Tables that have a coddep column (for demo-mode WHERE clause).
TABLES_WITH_CODDEP: list[str] = [
    "mutation",
    "disposition",
    "local",
    "disposition_parcelle",
    "parcelle",
    "adresse",
]

# Geometry columns to exclude from parcelle export.
PARCELLE_EXCLUDE_COLUMNS: list[str] = ["geompar", "geomparmut"]

# All tables in export order.
ALL_EXPORT_TABLES: list[str] = [
    "mutation",
    "parcelle",
    *SIMPLE_TABLES,
]


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
def _get_connection() -> psycopg2.extensions.connection:
    """Open a psycopg2 connection to the DVF database."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )


# ---------------------------------------------------------------------------
# Column introspection
# ---------------------------------------------------------------------------
def _get_table_columns(
    conn: psycopg2.extensions.connection, table_name: str
) -> list[str]:
    """Return the ordered list of column names for *table_name*."""
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        ORDER BY ordinal_position;
    """
    with conn.cursor() as cur:
        cur.execute(query, (table_name,))
        return [row[0] for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Query builders
# ---------------------------------------------------------------------------
def _build_where_clause(
    table_name: str, departments: list[str] | None
) -> str:
    """Build a WHERE clause filtering by department codes, or empty string."""
    if not departments:
        return ""
    if table_name not in TABLES_WITH_CODDEP:
        return ""
    placeholders = ", ".join(f"'{d}'" for d in departments)
    return f" WHERE coddep IN ({placeholders})"


def _build_mutation_query(departments: list[str] | None) -> str:
    """Build the SELECT query for the mutation table.

    Extracts geometry as lat/lon floats and array columns as scalars.
    """
    where = _build_where_clause("mutation", departments)
    return (
        "SELECT idmutation, idopendata, datemut, anneemut, moismut,"
        " idnatmut, libnatmut, vefa, coddep, codcomm, codcommune,"
        " l_codinsee[1] AS codinsee, valeurfonc, nbdispo, nblot,"
        " nbcomm, nbsection, l_section[1] AS section,"
        " nbpar, l_par[1] AS par, nbparmut, nbartcgi,"
        " l_artcgi[1] AS artcgi, nblocmut, nblocmai, nblocapt,"
        " nblocact, nblocdep, nbloccom,"
        " codtypbien, libtypbien, sbati, sbatmai, sbatapt, sbatact,"
        " sterr, nbsuf, nbvolmut,"
        " ST_Y(ST_Transform(geomlocmut, 4326)) AS latitude,"
        " ST_X(ST_Transform(geomlocmut, 4326)) AS longitude"
        f" FROM mutation{where}"
    )


def _build_parcelle_query(
    conn: psycopg2.extensions.connection,
    departments: list[str] | None,
) -> str:
    """Build a SELECT for parcelle, excluding heavy geometry columns."""
    all_cols = _get_table_columns(conn, "parcelle")
    keep = [c for c in all_cols if c not in PARCELLE_EXCLUDE_COLUMNS]
    cols_sql = ", ".join(keep)
    where = _build_where_clause("parcelle", departments)
    return f"SELECT {cols_sql} FROM parcelle{where}"


def _build_simple_query(
    table_name: str,
    departments: list[str] | None,
) -> str:
    """Build a simple SELECT * query, optionally filtered by department."""
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
    """Export a query result to CSV via COPY ... TO STDOUT.

    Returns the number of lines written (excluding the header).
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
    """Count data rows in a CSV file (total lines minus header)."""
    with open(csv_path, "r", encoding="utf-8") as fh:
        line_count = sum(1 for _ in fh)
    return max(line_count - 1, 0)


# ---------------------------------------------------------------------------
# Department filter resolution
# ---------------------------------------------------------------------------
def _resolve_departments() -> list[str] | None:
    """Return demo department codes if in demo mode, else None."""
    if DVF_MODE == "demo":
        logger.info("Demo mode: filtering exports to departments %s", DVF_DEMO_DEPARTMENTS)
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
    """Return the appropriate SQL query for exporting *table_name*."""
    if table_name == "mutation":
        return _build_mutation_query(departments)
    if table_name == "parcelle":
        return _build_parcelle_query(conn, departments)
    return _build_simple_query(table_name, departments)


def _table_exists(
    conn: psycopg2.extensions.connection, table_name: str
) -> bool:
    """Check whether *table_name* exists in the public schema."""
    query = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (table_name,))
        return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def export_tables() -> None:
    """Export all DVF+ tables to CSV files in DATA_EXPORT_DIR."""
    departments = _resolve_departments()
    DATA_EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    conn = _get_connection()
    try:
        _export_all_tables(conn, departments)
    finally:
        conn.close()

    logger.info("All exports complete. Output directory: %s", DATA_EXPORT_DIR)


def _export_all_tables(
    conn: psycopg2.extensions.connection,
    departments: list[str] | None,
) -> None:
    """Iterate over all tables and export each one."""
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
    """Export a single table, skipping if it does not exist. Returns row count."""
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s -- %(message)s",
    )
    try:
        export_tables()
    except psycopg2.OperationalError as exc:
        logger.error("Cannot connect to PostgreSQL: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
