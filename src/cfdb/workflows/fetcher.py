"""Download source files into a workflow's scratch directory.

Used by preprocessors that need the source bytes on local disk before
invoking tools. Wraps ``services.drs`` for DRS URI resolution and HTTPS
streaming, keeping URL-type branching off the processor hot path.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from cfdb.services import drs
from cfdb.workflows.urlsafe import validate_outbound_url

logger = logging.getLogger(__name__)


async def download_source(file_meta: dict[str, Any], dest: Path) -> Path:
    """Fetch the source file referenced by ``file_meta`` and write it to ``dest``.

    Supports direct HTTPS ``access_url`` values and GA4GH DRS URIs.
    Streams to ``dest.with_suffix(dest.suffix + ".part")`` and atomically
    renames into place on success; on failure the partial file is
    unlinked so the next retry never sees a half-downloaded source
    masquerading as complete bytes. Raises ``ValueError`` if
    ``access_url`` is missing; propagates DRS service exceptions
    unchanged after annotating with the resolved URL and
    (dcc, local_id) identity for triage.
    """
    access_url = file_meta.get("access_url")
    if not access_url:
        raise ValueError("file_meta has no access_url — cannot fetch source")

    dest.parent.mkdir(parents=True, exist_ok=True)
    download_url = await _resolve_download_url(access_url)

    part = dest.with_suffix(dest.suffix + ".part")
    bytes_written = 0
    try:
        with part.open("wb") as out:
            async for chunk in drs.stream_from_url(download_url, range_header=None):
                out.write(chunk)
                bytes_written += len(chunk)
        # Atomic publish: the cache/processor layer never sees a
        # half-written file at the canonical path.
        await asyncio.to_thread(part.replace, dest)
    except BaseException as exc:
        # Best-effort cleanup of the partial. ``missing_ok=True``
        # handles the case where the file was never opened (e.g.,
        # immediate failure inside stream_from_url).
        await asyncio.to_thread(part.unlink, True)
        identity = (file_meta.get("dcc"), file_meta.get("local_id"))
        logger.warning(
            "download_source failed for %s after %d bytes (identity=%r, url=%s)",
            type(exc).__name__,
            bytes_written,
            identity,
            download_url,
        )
        raise

    logger.info("Downloaded %s to %s (%d bytes)", download_url, dest, bytes_written)
    return dest


async def _resolve_download_url(access_url: str) -> str:
    """Return a direct HTTPS URL for ``access_url``.

    Direct ``https://`` URLs pass through. ``drs://`` URIs are resolved
    via the DRS service to obtain an HTTPS access method. Both the input
    URI and the resolved HTTPS URL go through ``validate_outbound_url``
    so a poisoned ``access_url`` or a DRS object pointing at an internal
    host gets rejected before any worker-side fetch.
    """
    validate_outbound_url(access_url)
    if access_url.startswith("drs://"):
        obj = await drs.fetch_drs_object(access_url)
        resolved = await drs.get_https_download_url(obj.access_methods)
        validate_outbound_url(resolved)
        return resolved
    return access_url
