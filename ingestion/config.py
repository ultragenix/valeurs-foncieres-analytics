"""Shared configuration for ingestion scripts.

Loads environment variables from .env and exposes typed constants
and shared utility functions.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load .env from project root
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Pipeline mode
# ---------------------------------------------------------------------------
DVF_MODE: str = os.getenv("DVF_MODE", "demo")
DVF_DEMO_DEPARTMENTS: list[str] = [
    d.strip()
    for d in os.getenv("DVF_DEMO_DEPARTMENTS", "75,13").split(",")
    if d.strip()
]

# ---------------------------------------------------------------------------
# PostgreSQL (ephemeral container)
# ---------------------------------------------------------------------------
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "dvf")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "dvf_local_only")
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "dvf")
POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))

POSTGRES_CONNECTION_STRING: str = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ---------------------------------------------------------------------------
# GCP
# ---------------------------------------------------------------------------
GCP_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "")
GCP_REGION: str = os.getenv("GCP_REGION", "europe-west9")
GCS_BUCKET_NAME: str = os.getenv("GCS_BUCKET_NAME", "")

# ---------------------------------------------------------------------------
# BigQuery datasets
# ---------------------------------------------------------------------------
BQ_DATASET_RAW: str = os.getenv("BQ_DATASET_RAW", "dvf_raw")
BQ_DATASET_STAGING: str = os.getenv("BQ_DATASET_STAGING", "dvf_staging")
BQ_DATASET_ANALYTICS: str = os.getenv("BQ_DATASET_ANALYTICS", "dvf_analytics")

# ---------------------------------------------------------------------------
# HTTP download settings
# ---------------------------------------------------------------------------
HTTP_CONNECT_TIMEOUT: int = 30
DOWNLOAD_CHUNK_SIZE: int = 1_048_576  # 1 MB

# ---------------------------------------------------------------------------
# GCS path prefixes
# ---------------------------------------------------------------------------
GCS_DVF_PREFIX: str = "raw/dvf"
GCS_GEOJSON_PREFIX: str = "raw/geojson"

# ---------------------------------------------------------------------------
# Data directories
# ---------------------------------------------------------------------------
DATA_DIR: Path = PROJECT_ROOT / "data"
DATA_EXPORT_DIR: Path = DATA_DIR / "export"
DATA_GEOJSON_DIR: Path = DATA_DIR / "geojson"

# ---------------------------------------------------------------------------
# Chunked ingestion settings (full mode)
# ---------------------------------------------------------------------------
DVF_CHUNK_SIZE: int = int(os.getenv("DVF_CHUNK_SIZE", "10"))
DVF_PROGRESS_FILE: Path = DATA_DIR / "chunked_progress.json"


# ---------------------------------------------------------------------------
# DVF table metadata
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_FORMAT: str = "%(asctime)s [%(levelname)s] %(name)s -- %(message)s"


def setup_logging(level: int = logging.INFO) -> None:
    """Configure logging with a consistent format across all modules."""
    logging.basicConfig(level=level, format=LOG_FORMAT)


# ---------------------------------------------------------------------------
# Shared connection helpers
# ---------------------------------------------------------------------------
def get_pg_connection() -> Any:
    """Open a psycopg2 connection to the DVF PostgreSQL database.

    Returns a ``psycopg2.extensions.connection`` instance (typed as Any to
    avoid import-time dependency on psycopg2 for modules that don't need it).
    """
    import psycopg2  # noqa: WPS433 -- lazy import

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )


def get_gcs_client() -> Any:
    """Create and return a GCS storage client.

    Returns a ``google.cloud.storage.Client`` instance (typed as Any to
    avoid import-time dependency on google-cloud-storage).
    """
    from google.cloud import storage  # noqa: WPS433 -- lazy import

    return storage.Client()
