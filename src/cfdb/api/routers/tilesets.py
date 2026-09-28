"""Preparation channel for matrix tile serving.

The HiGlass endpoints in ``cfdb.api.routers.tiles`` are pure readers: a
heatmap track cannot be handed a job id, so they 404 rather than 202 when
a contact map's artifact has not been built. This router is where that
artifact gets built.

- ``GET /tilesets/{dcc}/{local_id}/status`` — side-effect-free, mirroring
  the ``/data`` and ``/index`` readiness probes.
- ``POST /tilesets/{dcc}/{local_id}`` — dispatches, and hands back a job
  to poll on the existing ``/jobs/{job_id}`` endpoint.

A UI therefore probes, POSTs if needed, polls, and only then mounts the
Gosling spec — instead of mounting a track that fails until it doesn't.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Path, Response, status
from fastapi.responses import JSONResponse

from cfdb import api
from cfdb.api.routers._helpers import (
    PATH_PARAM_MAX_LEN,
    PATH_PARAM_PATTERN,
    resolve_file_doc,
)
from cfdb.api.routers.cache_stream import probe_workflow_readiness
from cfdb.services import locks
from cfdb.tilesets import backend
from cfdb.tilesets.formats import MatrixSource, matrix_source_kind
from cfdb.workflows.executor import (
    AdmissionRejected,
    ExecutorDraining,
    WorkflowNotApplicable,
)
from cfdb.workflows.models import ArtifactKind

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tilesets", tags=["tilesets"])


async def _resolve_contact_map(dcc: str, local_id: str) -> tuple[dict, MatrixSource]:
    """Resolve a document that is a contact map, or explain why it isn't.

    Raises:
        HTTPException: 400/403/404/500 from the shared preamble, or 404
            when the file is not a contact map at all.
    """
    _, file_doc = await resolve_file_doc(dcc, local_id)

    kind = matrix_source_kind(file_doc)
    if kind is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                "This file is not a contact map; only .mcool, .cool, and "
                ".hic files can be served as HiGlass tilesets."
            ),
        )
    return file_doc, kind


def _assert_subsystem_wired() -> None:
    """Fail loudly when nothing could build or read an artifact.

    Raises:
        HTTPException: 501 when the build carries no tile backend, 503
            when the workflow subsystem is disabled.

    Note:
        Checked explicitly, ahead of ``probe_workflow_readiness``, because
        that helper collapses three distinct conditions into ``None``.
        Ruling two of them out here is what lets the third be handled
        unambiguously.
    """
    if not backend.is_available():
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail=(
                "Matrix tile serving is not available in this build — it "
                "requires the clodius tile backend."
            ),
        )
    if api.processor_registry is None or api.cache is None or api.executor is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Workflow subsystem disabled — set SYNC_DATA_DIR and start a "
                "wool worker pool to prepare contact maps for tile serving."
            ),
            headers={"Retry-After": "30"},
        )


def _assert_identity_complete(file_doc: dict[str, Any]) -> None:
    """Reject a document with no md5 to build a cache key from.

    Raises:
        HTTPException: 409 — a stale or hand-edited record, not something
            a retry will fix, and not a server fault either.
    """
    if not file_doc.get("md5"):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "File metadata is incomplete (no md5), so no tileset "
                "artifact can be addressed for it."
            ),
        )


def _assert_hic_is_servable(kind: MatrixSource) -> None:
    """Refuse a ``.hic``, which this build cannot tile.

    Raises:
        HTTPException: 501 for every ``.hic``.

    Note:
        Reported here and not only on the tile routes, because the whole
        point of this channel is to tell a UI whether mounting a Gosling
        spec will work. Answering ``ready: true`` — which is what an
        in-place-read design would have meant — would send the client
        straight into a 501 from ``/tileset_info``. A readiness probe that
        promises something the tile endpoint then refuses is worse than no
        probe at all.
    """
    if kind is MatrixSource.HIC:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail=(
                "ENCODE .hic contact maps cannot be tiled yet. The tile "
                "backend reads local files only, and cfdb does not "
                "materialize .hic artifacts — those files run to hundreds "
                "of GB. 4DN .mcool contact maps are unaffected."
            ),
        )


@router.get("/{dcc}/{local_id}/status")
async def tileset_status(
    dcc: str = Path(..., max_length=PATH_PARAM_MAX_LEN, pattern=PATH_PARAM_PATTERN),
    local_id: str = Path(
        ..., max_length=PATH_PARAM_MAX_LEN, pattern=PATH_PARAM_PATTERN
    ),
) -> dict[str, bool]:
    """Report whether ``GET /tileset_info`` would serve this file now.

    Side-effect-free: it reads cache state and dispatches nothing, exactly
    like the ``/data`` and ``/index`` probes it mirrors.

    ``ready: true`` means no preparation is required — the artifact is
    already cached and ``GET /tileset_info`` will serve it.

    ``.hic`` answers 501 rather than a readiness value, matching the tile
    routes. It is a contact map, but not one this build can serve.
    """
    await locks.wait_for_cutover()

    try:
        file_doc, kind = await _resolve_contact_map(dcc, local_id)
        _assert_subsystem_wired()
        _assert_hic_is_servable(kind)

        _assert_identity_complete(file_doc)
        readiness = await probe_workflow_readiness(file_doc, ArtifactKind.TILESET)
        # Both prior conditions that make this None are ruled out above,
        # so None here can only mean the processor declined the file —
        # which _resolve_contact_map has already excluded.
        return {"ready": bool(readiness)}

    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected error in tileset_status")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        )


@router.post("/{dcc}/{local_id}")
async def prepare_tileset(
    dcc: str = Path(..., max_length=PATH_PARAM_MAX_LEN, pattern=PATH_PARAM_PATTERN),
    local_id: str = Path(
        ..., max_length=PATH_PARAM_MAX_LEN, pattern=PATH_PARAM_PATTERN
    ),
) -> Response:
    """Build the tileset artifact for a contact map, if it needs one.

    Returns:
        ``200 {"ready": true}`` when the artifact is already cached, so
        there is nothing to do. ``202`` with a ``Location`` header
        pointing at ``/jobs/{id}`` when a workflow was claimed or
        attached to.
    """
    await locks.wait_for_cutover()

    try:
        file_doc, kind = await _resolve_contact_map(dcc, local_id)
        _assert_subsystem_wired()
        _assert_hic_is_servable(kind)

        _assert_identity_complete(file_doc)

        if await probe_workflow_readiness(file_doc, ArtifactKind.TILESET):
            # Idempotent: a second POST for a prepared file is not a job.
            return JSONResponse(status_code=status.HTTP_200_OK, content={"ready": True})

        return await _dispatch(file_doc)

    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected error in prepare_tileset")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        )


async def _dispatch(file_doc: dict[str, Any]) -> Response:
    """Claim or attach to the workflow that builds this file's artifact.

    The exception ladder mirrors ``serve_workflow_artifact_or_dispatch``,
    and its order is load-bearing: ``ExecutorDraining`` subclasses
    ``WorkflowNotApplicable``, so it has to be caught first or a shutdown
    would be reported as an inapplicable file.
    """
    try:
        record, _ = await api.executor.ensure_workflow(file_doc)
    except ExecutorDraining:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service is shutting down; please retry",
            headers={"Retry-After": "30"},
        )
    except AdmissionRejected as exc:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many active preprocessing jobs; please retry shortly",
            headers={"Retry-After": str(exc.retry_after_seconds)},
        )
    except WorkflowNotApplicable as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"No tileset can be prepared for this file: {exc}",
        )

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"job_id": record.job_id, "status": record.status.value},
        headers={"Location": f"/jobs/{record.job_id}", "Retry-After": "5"},
    )
