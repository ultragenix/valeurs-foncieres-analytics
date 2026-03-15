"""Shared HTTP download utilities for ingestion scripts.

Provides reusable streaming download and file-writing helpers used by
``download_dvf`` and ``download_geojson`` modules. Downloads are
streamed in fixed-size chunks to avoid loading large files into memory,
and progress is displayed via ``tqdm``.

Dependencies:
    requests -- HTTP client library.
    tqdm     -- progress bar for streaming downloads.
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

    Args:
        url: The HTTP(S) URL to download.
        destination: Local file path where the content will be saved.
            Parent directories are created automatically.
        read_timeout: Per-chunk read timeout in seconds (keyword-only).

    Returns:
        True on success, False if the request fails for any reason.
    """
    response = _initiate_request(url, read_timeout=read_timeout)
    if response is None:
        return False
    _write_response_to_file(response, destination)
    return True


def _initiate_request(
    url: str, *, read_timeout: int = HTTP_READ_TIMEOUT_DEFAULT
) -> requests.Response | None:
    """Open a streaming HTTP GET connection and return the response.

    Uses ``HTTP_CONNECT_TIMEOUT`` for the connection phase and
    *read_timeout* for the per-chunk read phase. Follows redirects.

    Args:
        url: The HTTP(S) URL to request.
        read_timeout: Per-chunk read timeout in seconds (keyword-only).

    Returns:
        The open ``requests.Response`` on success, or ``None`` if the
        request raises any ``RequestException``.
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
    """Write streaming response content to *destination* with progress.

    Reads the response body in ``DOWNLOAD_CHUNK_SIZE`` byte chunks and
    writes each to disk, updating a ``tqdm`` progress bar. The total
    size is derived from the ``Content-Length`` header when available.

    Args:
        response: An open streaming ``requests.Response``.
        destination: Local file path to write to.
    """
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
