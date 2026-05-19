"""Shared cache-streaming helper for the /data and /index routers.

Both routers stream bytes from a ``CacheBackend`` back to the client with
HTTP ``Range`` support. The logic — parse-range, pick 200 vs 206, honor
HEAD — is identical across both endpoints, and the workflow-dispatch
sequence (look up the processor, derive the cache key, hit the cache
or claim a workflow) is also identical, so both live here and are
imported by ``routers.data`` and ``routers.index``.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse, Response, StreamingResponse

from cfdb import api
from cfdb.services import drs
from cfdb.workflows import keys as key_utils
from cfdb.workflows.cache import CacheBackend
from cfdb.workflows.executor import ExecutorDraining, WorkflowNotApplicable
from cfdb.workflows.keys import extract_identity
from cfdb.workflows.models import ArtifactKind

logger = logging.getLogger(__name__)


def stream_cache_entry(
    cache: CacheBackend,
    cache_key: str,
    file_size: int,
    request: Request,
    range_header: Optional[str],
    *,
    media_type: str = "application/octet-stream",
):
    """Return a Response streaming ``cache_key`` bytes, honoring Range.

    Args:
        cache: The backend to pull bytes from.
        cache_key: The key under which the artifact is stored.
        file_size: Size of the cached artifact in bytes.
        request: FastAPI request — used to detect HEAD so we can return
            headers only.
        range_header: Raw value of the incoming ``Range`` header, if any.
        media_type: Content-Type to return. Defaults to
            ``application/octet-stream`` since workflow artifacts are
            genuinely binary (BAM, bgzipped text, BAI, TBI).

    Returns:
        ``Response`` for HEAD or ``StreamingResponse`` for GET, with
        ``Content-Length`` and ``Content-Range`` populated as appropriate.

    Raises:
        HTTPException(416): Range is syntactically valid but exceeds the
            file bounds.
        HTTPException(400): Range header has invalid syntax.
    """
    response_headers = {"Accept-Ranges": "bytes"}

    byte_range: Optional[tuple[int, int]] = None
    status_code = status.HTTP_200_OK

    if range_header:
        try:
            start, end, content_length = drs.parse_range_header(range_header, file_size)
            byte_range = (start, end)
            status_code = status.HTTP_206_PARTIAL_CONTENT
            response_headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
            response_headers["Content-Length"] = str(content_length)
        except drs.RangeNotSatisfiableError as e:
            raise HTTPException(
                status_code=status.HTTP_416_RANGE_NOT_SATISFIABLE,
                headers={
                    "Content-Range": f"bytes */{e.file_size}",
                    "Accept-Ranges": "bytes",
                },
            )
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid Range header: {str(e)}",
            )
    else:
        response_headers["Content-Length"] = str(file_size)

    if request.method == "HEAD":
        return Response(
            status_code=status_code,
            media_type=media_type,
            headers=response_headers,
        )

    return StreamingResponse(
        cache.get(cache_key, byte_range),
        status_code=status_code,
        media_type=media_type,
        headers=response_headers,
    )


async def serve_workflow_artifact_or_dispatch(
    file_doc: dict[str, Any],
    artifact_kind: ArtifactKind,
    request: Request,
    range_header: Optional[str],
    *,
    head_404_detail: str,
) -> Optional[Response]:
    """Serve a cached workflow artifact, dispatch on cache miss, or fall through.

    Centralizes the dispatch sequence shared by ``/data`` and ``/index``:
    workflow-subsystem-wired check → processor lookup → identity
    extraction → cache key derivation → cache hit (stream) or miss
    (HEAD 404, GET 202).

    Args:
        file_doc: Source file metadata document.
        artifact_kind: Which artifact kind to look up / produce.
        request: FastAPI request — used to detect HEAD vs GET.
        range_header: Raw value of the incoming ``Range`` header.
        head_404_detail: Detail string for the HEAD-cache-miss 404
            (caller-supplied because data and index phrase it
            differently).

    Returns:
        - A streaming Response on cache hit.
        - A 202 ``JSONResponse`` with ``Location`` and ``Retry-After`` on
          a successful claim.
        - ``None`` when the workflow subsystem is unwired, the file
          isn't workflow-applicable, the processor doesn't produce
          ``artifact_kind``, or ``file_doc`` is incomplete — caller
          falls through to its own path.

    Raises:
        HTTPException(404): HEAD request found no cached artifact. HEAD
            never dispatches so monitoring probes and prefetch tooling
            cannot trigger preprocessing side-effects.
    """
    if api.processor_registry is None or api.cache is None or api.executor is None:
        return None

    processor = api.processor_registry.lookup_for(file_doc)
    if processor is None or not processor.needs_processing(file_doc):
        return None

    if artifact_kind not in processor.artifact_kinds_produced(file_doc):
        return None

    try:
        dcc, local_id, md5 = extract_identity(file_doc)
    except ValueError as exc:
        # Treat incomplete file_meta as "workflow not applicable" so
        # the caller falls through. The DCC ingest contract guarantees
        # md5 is populated; a missing field here is almost always a
        # stale or hand-edited document, not a request to 500 on.
        logger.warning(f"Cannot dispatch workflow — file_meta incomplete: {exc}")
        return None

    cache_key = key_utils.cache_key(
        dcc=dcc,
        local_id=local_id,
        artifact_kind=artifact_kind,
        md5=md5,
        processor_version=processor.processor_version,
    )

    entry = await api.cache.head(cache_key)
    if entry is not None:
        return stream_cache_entry(
            api.cache, cache_key, entry.size, request, range_header
        )

    # Cache miss. HEAD reports "not here" without side-effects so
    # probes cannot force dispatch; GET claims or attaches.
    if request.method == "HEAD":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=head_404_detail
        )

    try:
        record, _ = await api.executor.ensure_workflow(file_doc)
    except ExecutorDraining:
        # Lifespan teardown is in progress; surface a clean 503 with
        # Retry-After rather than the generic 500 a router-level catchall
        # would otherwise produce. Caught BEFORE WorkflowNotApplicable
        # so the subclass-specific status code wins.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service is shutting down; please retry",
            headers={"Retry-After": "30"},
        )
    except WorkflowNotApplicable:
        # Race: processor accepted this file, but executor disagreed.
        # Fall through to the caller's own path rather than 500.
        return None

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"job_id": record.job_id, "status": record.status.value},
        headers={
            "Location": f"/jobs/{record.job_id}",
            "Retry-After": "5",
        },
    )
