"""REST API router for streaming index files associated with DCC data files.

Serves index artifacts via three cascading strategies:

1. **4DN sidecar fast path**: 4DN's C2M2 enrichment sometimes publishes
   pre-built index files under ``extra.fourdn.extra_files`` (notably
   ``beddb`` and ``tbi`` for BED) or ``extra.extra_files``. When one is
   present, the router streams it directly — preserves existing behavior
   for the 218 BED→beddb and 4 BED→tbi sidecars.

2. **Workflow cache path**: for files produced by a workflow that emits
   an index artifact (BAM→BAI, text intervals→TBI, etc.), the router
   streams the cached ``.bai``/``.tbi`` from the workflow cache.

3. **Workflow dispatch**: on cache miss, the router dispatches a workflow
   (shared with any in-flight ``/data`` request for the same source file)
   and returns ``202 Accepted`` with a ``Location: /jobs/{id}`` header.

Formats that produce no index (CSV, TSV) return ``404 Not Found``.
"""

from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import urljoin

from fastapi import APIRouter, Header, HTTPException, Path, Request, status
from fastapi.responses import Response, StreamingResponse

from cfdb import api
from cfdb.api.routers._helpers import enforce_hubmap_access, lookup_file_doc
from cfdb.api.routers.cache_stream import serve_workflow_artifact_or_dispatch
from cfdb.dcc_registry import get_all_dcc_names, get_dcc_config, normalize_dcc_name
from cfdb.services import drs, locks
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.passthrough import PassthroughProcessor
from cfdb.workflows.processors.tools import format_name
from cfdb.workflows.urlsafe import UnsafeOutboundURL, validate_outbound_url

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/index", tags=["index"])

#: Mirrors the constraint applied in ``routers/data.py`` so both endpoints
#: reject the same shape of path-param input. ENCODE/4DN/HuBMAP accessions
#: all live under ``[A-Za-z0-9._-]``.
_PATH_PARAM_PATTERN = r"^[A-Za-z0-9._-]+$"
_PATH_PARAM_MAX_LEN = 256

#: Index artifact kind → preferred sidecar ``file_format`` tokens used in
#: 4DN's ``extra.extra_files`` / ``extra.fourdn.extra_files`` arrays. The
#: lookup is best-effort: if no entry matches we fall back to the first
#: sidecar entry (legacy behavior) so existing 4DN BED→beddb / BED→tbi
#: cases keep working.
_SIDECAR_INDEX_FORMATS = ("bai", "tbi", "beddb", "csi")


@router.head("/{dcc}/{local_id}")
@router.get("/{dcc}/{local_id}")
async def stream_index_file(
    dcc: str = Path(
        ..., max_length=_PATH_PARAM_MAX_LEN, pattern=_PATH_PARAM_PATTERN
    ),
    local_id: str = Path(
        ..., max_length=_PATH_PARAM_MAX_LEN, pattern=_PATH_PARAM_PATTERN
    ),
    request: Request = None,  # type: ignore[assignment]  # injected by FastAPI
    range: Optional[str] = Header(None),
    raw: bool = False,
):
    """Stream an index artifact for a file, dispatching a workflow on miss.

    When ``raw`` is False (the default), the router prefers any upstream
    sidecar (e.g., a 4DN ``extra_files`` entry) and otherwise serves or
    builds the index via the workflow pipeline. Passing ``raw=true``
    restricts the response to upstream sidecars only — files without a
    sidecar return 404 rather than entering the pipeline.
    """
    await locks.wait_for_cutover()

    try:
        normalized_dcc = normalize_dcc_name(dcc)
        valid_dccs = get_all_dcc_names()

        if normalized_dcc not in valid_dccs:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown DCC '{dcc}'. Valid DCCs: {', '.join(valid_dccs)}",
            )

        if api.db is None:
            logger.error("Database not initialized")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database not available",
            )

        # Look up the file document via the shared helper so /data and
        # /index converge on the same record for any given
        # (normalized_dcc, local_id) pair. FILE_DOC_PROJECTION strips _id
        # so bson.ObjectId never crosses into the workflow subsystem.
        file_doc = await lookup_file_doc(api.db, normalized_dcc, local_id)

        if not file_doc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="File not found"
            )

        # Defense-in-depth: reject any non-public HuBMAP file that survived
        # pruning before either the sidecar fast path or the workflow path
        # can stream upstream bytes or cache them.
        enforce_hubmap_access(normalized_dcc, file_doc)

        # 1. Upstream sidecar (e.g., 4DN's extra_files) always takes
        #    precedence when present — the upstream DCC pre-built the
        #    index for us. ``raw=True`` terminates here either way.
        sidecar_response = await _try_serve_fourdn_sidecar(
            file_doc, normalized_dcc, request, range
        )
        if sidecar_response is not None:
            return sidecar_response

        if raw:
            # Client asked for the raw upstream index but none exists.
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No upstream index sidecar available",
            )

        # 2 + 3. Workflow cache path or dispatch-on-miss for GET; HEAD
        #        reports existence without side-effects.
        workflow_response = await _try_serve_workflow_index(file_doc, request, range)
        if workflow_response is not None:
            return workflow_response

        # No sidecar, no workflow handler. Two terminal cases:
        #
        # 1. The format has no index in any state of the world (CSV,
        #    TSV, bigWig — passthrough formats with no companion
        #    index file). Return 404 unconditionally; the issue spec
        #    promises a clean "no index" signal regardless of
        #    subsystem state.
        # 2. The format could be processed into an index but the
        #    workflow subsystem is disabled — return 503 so operators
        #    can distinguish degraded mode from an inherent no-index
        #    format.
        fmt = format_name(file_doc)
        if fmt is not None and fmt in PassthroughProcessor.supported_formats:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No index file available for this file format",
            )
        if api.processor_registry is None or api.cache is None or api.executor is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    "Workflow subsystem disabled — set SYNC_DATA_DIR and "
                    "start a wool worker pool to serve preprocessed "
                    "indexes."
                ),
                headers={"Retry-After": "30"},
            )

        # Reached only if no processor produces an index for this format
        # and no sidecar exists.
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No index file available for this file",
        )

    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected error in stream_index_file")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        )


async def _try_serve_fourdn_sidecar(
    file_doc: dict,
    normalized_dcc: str,
    request: Request,
    range_header: Optional[str],
):
    """Return a streaming response if the file carries a legacy sidecar.

    Looks first at ``extra.extra_files`` (the path used by pre-materialized
    documents) and then at ``extra.fourdn.extra_files`` (the DCC-specific
    subdocument).
    """
    extra = file_doc.get("extra") or {}
    extra_files = extra.get("extra_files") or []
    if not extra_files:
        fourdn = extra.get("fourdn") or {}
        extra_files = fourdn.get("extra_files") or []
    if not extra_files:
        return None

    # Prefer a sidecar entry whose ``file_format`` is one of the
    # canonical index types we care about (bai/tbi/beddb/csi); fall
    # back to the first entry to preserve legacy behavior for files
    # whose sidecar omits ``file_format``. Iterating defends against
    # future 4DN docs that publish multiple sidecar artifacts where
    # the data file (not the index) happens to be first.
    index_entry: Optional[dict] = None
    for candidate in extra_files:
        if not isinstance(candidate, dict):
            continue
        fmt = (candidate.get("file_format") or "").lower()
        if fmt in _SIDECAR_INDEX_FORMATS:
            index_entry = candidate
            break
    if index_entry is None:
        first = extra_files[0]
        if not isinstance(first, dict):
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Upstream sidecar entry is not a valid object",
            )
        index_entry = first

    href = index_entry.get("href")
    if not href:
        # Sidecar entry is malformed — surface a clear 502 rather than
        # silently falling through to the workflow path. A workflow
        # artifact (md5-cached) would not match the bytes a client
        # expected from the upstream sidecar.
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream sidecar entry is missing required `href` field",
        )

    try:
        config = get_dcc_config(normalized_dcc)
    except Exception:
        return None
    # ``urljoin`` resolves an absolute ``href`` correctly (and prevents the
    # string-concat open-redirect where a malicious href like
    # "https://attacker/x" would otherwise be appended to the api_base);
    # validate the result against the outbound allowlist before we
    # actually fetch the URL.
    download_url = urljoin(config["api_base"], href)
    try:
        validate_outbound_url(download_url)
    except UnsafeOutboundURL as e:
        logger.warning("Refusing 4DN sidecar fetch: %s", e)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream sidecar URL failed allowlist validation",
        )

    filename = href.rsplit("/", 1)[-1] if "/" in href else href
    file_size = index_entry.get("file_size")

    logger.info(f"Streaming 4DN sidecar: {filename} from {download_url}")

    response_headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Accept-Ranges": "bytes",
    }

    status_code = status.HTTP_200_OK
    range_to_send: Optional[str] = None
    if range_header:
        if not file_size:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Cannot process range request: index file size unavailable",
            )
        try:
            start, end, content_length = drs.parse_range_header(range_header, file_size)
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

    if not range_header and file_size:
        response_headers["Content-Length"] = str(file_size)

    if request.method == "HEAD":
        return Response(
            status_code=status_code,
            media_type="application/octet-stream",
            headers=response_headers,
        )

    chunk_gen = drs.stream_from_url(download_url, range_to_send)
    return StreamingResponse(
        chunk_gen,
        status_code=status_code,
        media_type="application/octet-stream",
        headers=response_headers,
    )


async def _try_serve_workflow_index(
    file_doc: dict, request: Request, range_header: Optional[str]
):
    """Serve a cached workflow index or dispatch a workflow on miss.

    Thin wrapper over ``serve_workflow_artifact_or_dispatch`` pinning
    the artifact kind to INDEX. See the helper docstring for full
    behavior.
    """
    return await serve_workflow_artifact_or_dispatch(
        file_doc,
        ArtifactKind.INDEX,
        request,
        range_header,
        head_404_detail=(
            "Processed index not yet available. Issue a GET to "
            "trigger preprocessing, or add ?raw=true for the upstream "
            "sidecar."
        ),
    )
