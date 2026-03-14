"""Shared HTTP download utilities for ingestion scripts.

Provides reusable streaming download and file-writing helpers
used by download_dvf and download_geojson modules.
"""

from __future__ import annotations

import logging
from pathlib import Path

import requests
from tqdm import tqdm

from ingestion.config import DOWNLOAD_CHUNK_SIZE, HTTP_CONNECT_TIMEOUT

logger = logging.getLogger(__name__)

# Default HTTP read timeout in seconds (per chunk).
HTTP_READ_TIMEOUT_DEFAULT: int = 120


def stream_download(
    url: str,
    destination: Path,
    *,
    read_timeout: int = HTTP_READ_TIMEOUT_DEFAULT,
) -> bool:
    """Stream-download a URL to *destination* with a tqdm progress bar.

    Returns True on success, False on failure.
    """
    response = _initiate_request(url, read_timeout=read_timeout)
    if response is None:
        return False
    _write_response_to_file(response, destination)
    return True


def _initiate_request(
    url: str, *, read_timeout: int = HTTP_READ_TIMEOUT_DEFAULT
) -> requests.Response | None:
    """Open a streaming HTTP connection and return the response.

    Returns None if the request fails.
    """
    try:
        response = requests.get(
            url,
            stream=True,
            timeout=(HTTP_CONNECT_TIMEOUT, read_timeout),
            allow_redirects=True,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Download from %s failed: %s", url, exc)
        return None
    return response


def _write_response_to_file(
    response: requests.Response, destination: Path
) -> None:
    """Write streaming response content to *destination* with progress."""
    total_size = int(response.headers.get("content-length", 0))
    logger.info(
        "Downloading (%s bytes) ...",
        f"{total_size:,}" if total_size else "unknown size",
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    with (
        open(destination, "wb") as fh,
        tqdm(
            total=total_size, unit="B", unit_scale=True, desc=destination.name
        ) as bar,
    ):
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
            fh.write(chunk)
            bar.update(len(chunk))
    logger.info(
        "Saved %s (%s bytes)",
        destination.name,
        f"{destination.stat().st_size:,}",
    )
