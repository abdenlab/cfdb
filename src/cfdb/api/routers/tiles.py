"""HiGlass tile-serving endpoints for Hi-C contact maps.

Two routes, both strictly read-only:

- ``GET /tileset_info/?d=<uid>``
- ``GET /tiles/?d=<uid>.<z>.<x>.<y>``

They are mounted at the root, and take repeatable ``d`` query parameters,
because that is the wire protocol a HiGlass client constructs — Gosling's
``MatrixData.url`` is conventionally the full ``.../tileset_info/?d=<uid>``
URL, which it splits into a server and a tileset uid.

**These endpoints never dispatch a workflow.** A heatmap track issues
thousands of tile requests and has nowhere to put a job id, so there is no
useful 202 to return: a dataset whose artifact has not been built is a
404. Building it is the separate preparation channel's job — see
``cfdb.api.routers.tilesets``.
"""

from __future__ import annotations

import logging
import re
from collections import OrderedDict
from typing import Any

from fastapi import APIRouter, HTTPException, Query, status

from cfdb import api
from cfdb.api.routers._helpers import (
    PATH_PARAM_MAX_LEN,
    PATH_PARAM_PATTERN,
    resolve_file_doc,
)
from cfdb.services import locks
from cfdb.tilesets import backend
from cfdb.tilesets.errors import (
    TileBackendUnavailable,
    TilesetError,
    TilesetHydrationTimeout,
    TilesetTooLarge,
)
from cfdb.tilesets.wire import error_payload

logger = logging.getLogger(__name__)

router = APIRouter(tags=["tiles"])

#: Ceiling on the number of ``d`` parameters one request may carry. A
#: HiGlass client batches on the order of dozens; anything far beyond that
#: is a way to turn one unauthenticated request into arbitrarily many h5py
#: reads.
MAX_TILE_IDS = 64

_UID_PATTERN = re.compile(PATH_PARAM_PATTERN)


def _split_uid(uid: str) -> tuple[str, str]:
    """Split ``{dcc}/{local_id}`` into its two halves.

    Raises:
        HTTPException: 400 when the uid is not the expected shape.

    Note:
        A ``.`` in either half is rejected. clodius parses a tile id by
        splitting on ``.`` and taking the first field as the uid, so a
        dotted identifier would silently be read as a uid plus a zoom
        level. No file in the corpus has one — 4DN uses UUIDs, ENCODE uses
        ``ENCFF`` accessions — so this is a guard rather than a
        restriction, but it is the kind of thing that would otherwise be
        found by a user with a very confusing bug report.
    """
    dcc, separator, local_id = uid.partition("/")
    if not separator or not dcc or not local_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Malformed tileset uid '{uid}'; expected '<dcc>/<local_id>'",
        )
    for part in (dcc, local_id):
        if (
            len(part) > PATH_PARAM_MAX_LEN
            or "." in part
            or not _UID_PATTERN.fullmatch(part)
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Malformed tileset uid '{uid}'; each part must match "
                    f"{PATH_PARAM_PATTERN} and contain no '.'"
                ),
            )
    return dcc, local_id


def _require_service():
    """Return the tile service, or explain why there isn't one.

    Raises:
        HTTPException: 501 when the build carries no tile backend, 503
            when the workflow subsystem (and hence the artifact cache) is
            not wired.
    """
    service = api.tileset_service
    if service is not None:
        return service
    if not backend.is_available():
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail=(
                "Matrix tile serving is not available in this build — it "
                "requires the clodius tile backend."
            ),
        )
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=(
            "Tile serving is unavailable — set SYNC_DATA_DIR to enable the "
            "artifact cache the tile server reads from."
        ),
        headers={"Retry-After": "30"},
    )


def _validate_ids(values: list[str], parameter: str) -> list[str]:
    """Reject an empty or oversized ``d`` list."""
    if not values:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"At least one '{parameter}' parameter is required",
        )
    if len(values) > MAX_TILE_IDS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Too many '{parameter}' parameters ({len(values)}); at most "
                f"{MAX_TILE_IDS} may be requested at once"
            ),
        )
    return values


def _reraise_infrastructure(exc: TilesetError) -> None:
    """Translate the tileset failures that are not per-dataset facts.

    A missing backend and a disk-budget or hydration problem are
    conditions of the *server*, so they surface as status codes on the
    whole request rather than as an error object beside a sibling uid that
    happened to work.
    """
    if isinstance(exc, TileBackendUnavailable):
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=str(exc)
        )
    if isinstance(exc, (TilesetTooLarge, TilesetHydrationTimeout)):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
            headers={"Retry-After": "30"},
        )


def _render_dataset_failure(
    uid: str, exc: TilesetError, *, solo: bool
) -> dict[str, Any]:
    """Render a per-dataset failure as a 404 or an error object.

    ``solo`` decides which. A request naming one uid — what a Gosling
    matrix track sends — gets the promised 404. A batch naming several
    gets an error object beside its siblings, so one unbuilt dataset does
    not discard the tiles that were available.
    """
    _reraise_infrastructure(exc)
    if solo:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
        )
    logger.info(f"Tileset {uid} unavailable: {exc}")
    return error_payload(str(exc))


@router.get("/tileset_info/")
@router.get("/tileset_info")
async def tileset_info(
    d: list[str] = Query(default_factory=list),  # noqa: B008
) -> dict[str, Any]:
    """Return HiGlass tileset metadata for each requested uid.

    Never dispatches: a contact map whose artifact has not been built is
    reported as absent, not as pending.
    """
    await locks.wait_for_cutover()

    uids = _validate_ids(d, "d")
    service = _require_service()
    solo = len(uids) == 1

    response: dict[str, Any] = {}
    for uid in uids:
        dcc, local_id = _split_uid(uid)
        _, file_doc = await resolve_file_doc(dcc, local_id)
        try:
            response[uid] = await service.tileset_info(uid, file_doc)
        except TilesetError as exc:
            response[uid] = _render_dataset_failure(uid, exc, solo=solo)
    return response


@router.get("/tiles/")
@router.get("/tiles")
async def tiles(
    d: list[str] = Query(default_factory=list),  # noqa: B008
) -> dict[str, Any]:
    """Return tile payloads keyed by the exact tile id that was requested.

    The response carries exactly one entry per requested id — a response
    may safely be built by zipping the request against the result. A tile
    position the resolution ladder does not hold, a tile id that fails to
    parse, or one that names a balancing column the file does not carry
    all get an ``{"error": ...}`` object in place of data rather than being
    omitted or failing the request. One malformed id in a batch of sixty
    must not blank the track.
    """
    await locks.wait_for_cutover()

    raw_ids = _validate_ids(d, "d")
    service = _require_service()

    # Group by uid, preserving first-seen order, so one dataset is opened
    # once per request however many of its tiles were asked for.
    grouped: OrderedDict[str, list[str]] = OrderedDict()
    for raw in raw_ids:
        uid, _, _ = raw.partition(".")
        grouped.setdefault(uid, []).append(raw)
    solo = len(grouped) == 1

    response: dict[str, Any] = {}
    for uid, uid_tile_ids in grouped.items():
        dcc, local_id = _split_uid(uid)
        _, file_doc = await resolve_file_doc(dcc, local_id)
        try:
            response.update(await service.tiles(uid, file_doc, uid_tile_ids))
        except TilesetError as exc:
            failure = _render_dataset_failure(uid, exc, solo=solo)
            for raw in uid_tile_ids:
                response[raw] = failure
    return response
