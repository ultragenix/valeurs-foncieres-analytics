"""Shared configuration for ingestion scripts.

Loads environment variables from .env and exposes typed constants.
"""

import os
from pathlib import Path

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
# Data directories
# ---------------------------------------------------------------------------
DATA_DIR: Path = PROJECT_ROOT / "data"
DATA_EXPORT_DIR: Path = DATA_DIR / "export"
DATA_GEOJSON_DIR: Path = DATA_DIR / "geojson"
