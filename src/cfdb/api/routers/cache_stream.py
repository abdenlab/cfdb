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
from cfdb.workflows.cache import CacheBackend
from cfdb.workflows.executor import (
    AdmissionRejected,
    ExecutorDraining,
    WorkflowNotApplicable,
)
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


def stream_upstream_url(
    download_url: str,
    file_size: Optional[int],
    filename: str,
    request: Request,
    range_header: Optional[str],
    *,
    media_type: str = "application/octet-stream",
):
    """Return a Response streaming an upstream URL, honoring Range.

    The upstream-URL twin of :func:`stream_cache_entry`: same parse-range
    / 200-vs-206 / HEAD ritual, but the bytes come from ``download_url``
    via :func:`drs.stream_from_url` instead of a ``CacheBackend``. The
    DRS/HTTPS, ENCODE, and 4DN-sidecar paths all delegate here so the
    range contract stays in one place.

    Args:
        download_url: Absolute URL to stream from (already allowlist-validated
            by the caller where applicable).
        file_size: Total size in bytes, or ``None`` when unknown. A range
            request against an unknown size yields ``416`` per RFC 9110
            §15.5.17 (not ``500``).
        filename: Used for the ``Content-Disposition`` header.
        request: FastAPI request — used to detect HEAD vs GET.
        range_header: Raw value of the incoming ``Range`` header, if any.
        media_type: Content-Type to return.

    Raises:
        HTTPException(416): size unknown on a range request, or the range
            is valid but out of bounds.
        HTTPException(400): Range header has invalid syntax.
    """
    response_headers = {
        "Content-Disposition": f'attachment; filename="{filename or "file"}"',
        "Accept-Ranges": "bytes",
    }
    status_code = status.HTTP_200_OK
    range_to_send: Optional[str] = None

    if range_header:
        if not file_size:
            # RFC 9110 §15.5.17: a range request the server cannot satisfy
            # because the total size is unknown produces 416 with
            # ``Content-Range: bytes */*`` so the client can fall back to a
            # plain GET. (The 4DN sidecar path previously mis-signalled 500.)
            raise HTTPException(
                status_code=status.HTTP_416_RANGE_NOT_SATISFIABLE,
                detail="File size unknown; range cannot be satisfied",
                headers={"Content-Range": "bytes */*"},
            )
        try:
            start, end, content_length = drs.parse_range_header(
                range_header, file_size
            )
            range_to_send = range_header
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
    elif file_size:
        response_headers["Content-Length"] = str(file_size)

    if request.method == "HEAD":
        return Response(
            status_code=status_code,
            media_type=media_type,
            headers=response_headers,
        )

    return StreamingResponse(
        drs.stream_from_url(download_url, range_to_send),
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
        # Single cache-key authority: the processor derives the key it
        # writes under; the router probes the same key here. Re-deriving
        # the formula independently is what let producer/consumer drift.
        cache_key = processor.cache_key_for(file_doc, artifact_kind)
    except ValueError as exc:
        # Treat incomplete file_meta as "workflow not applicable" so
        # the caller falls through. The DCC ingest contract guarantees
        # md5 is populated; a missing field here is almost always a
        # stale or hand-edited document, not a request to 500 on.
        logger.warning(f"Cannot dispatch workflow — file_meta incomplete: {exc}")
        return None

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
    except AdmissionRejected as exc:
        # The active-workflow ceiling is hit — shed this request with a
        # 429 so the client backs off rather than queuing unbounded work.
        # NOT a WorkflowNotApplicable subclass, so it must be caught
        # explicitly (before that handler) to avoid falling through to
        # direct upstream streaming, which would bypass the bounded
        # pipeline entirely.
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many active preprocessing jobs; please retry shortly",
            headers={"Retry-After": str(exc.retry_after_seconds)},
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


async def probe_workflow_readiness(
    file_doc: dict[str, Any],
    artifact_kind: ArtifactKind,
) -> Optional[bool]:
    """Report whether a GET would stream a workflow artifact immediately.

    Mirrors the applicability checks in
    ``serve_workflow_artifact_or_dispatch`` exactly — workflow-subsystem
    wired → processor lookup → ``needs_processing`` →
    ``artifact_kinds_produced`` → cache-key derivation — but **never
    dispatches**: it only reads cache state. This is the side-effect-free
    counterpart used by the ``/status`` probes.

    Args:
        file_doc: Source file metadata document.
        artifact_kind: Which artifact kind to probe for.

    Returns:
        - ``True`` — the artifact is cached; a GET would stream it now.
        - ``False`` — the file is workflow-applicable for ``artifact_kind``
          but the artifact is not cached; a GET would dispatch (202).
        - ``None`` — the workflow subsystem is unwired, the file isn't
          workflow-applicable, the processor doesn't produce
          ``artifact_kind``, or ``file_doc`` is incomplete. The caller
          decides what "ready" means for its own non-workflow path.
    """
    if api.processor_registry is None or api.cache is None or api.executor is None:
        return None

    processor = api.processor_registry.lookup_for(file_doc)
    if processor is None or not processor.needs_processing(file_doc):
        return None

    if artifact_kind not in processor.artifact_kinds_produced(file_doc):
        return None

    try:
        # Single cache-key authority: the processor derives the key it
        # writes under; the probe reads the same key here, exactly as
        # ``serve_workflow_artifact_or_dispatch`` does.
        cache_key = processor.cache_key_for(file_doc, artifact_kind)
    except ValueError as exc:
        # Mirror the dispatch helper: treat an incomplete file_meta as
        # "workflow not applicable" so the caller falls through to its own
        # path rather than 500ing on a stale/hand-edited document.
        logger.warning(
            f"Cannot probe workflow readiness — file_meta incomplete: {exc}"
        )
        return None

    entry = await api.cache.head(cache_key)
    return entry is not None
