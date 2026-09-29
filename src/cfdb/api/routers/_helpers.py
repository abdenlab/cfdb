"""Shared primitives for the /data, /index, and tileset routers.

Provides:

- ``FILE_DOC_PROJECTION``: the Mongo projection every router uses when reading
  a file document. Strips ``_id`` (so ``bson.ObjectId`` never crosses into the
  workflow subsystem) and limits the returned dict to the fields any router
  or workflow consumer reads.
- ``lookup_file_doc``: single canonical Mongo query so the routers hit
  the same record for a given ``(dcc, local_id)`` pair — preserves the
  "exactly one workflow under contention" invariant.
- ``enforce_hubmap_access``: defense-in-depth guard rejecting non-public
  HuBMAP files before any workflow dispatch or upstream stream.
- ``resolve_file_doc``: the whole per-request preamble the routers share —
  DCC validation, database check, lookup, and access control — in the one
  ordering all of them agree on.
"""

from __future__ import annotations

import logging
from typing import Any, Final

from fastapi import HTTPException, status

from cfdb import api
from cfdb.dcc_registry import get_all_dcc_names, normalize_dcc_name

logger = logging.getLogger(__name__)

#: Tight path-param constraint shared by every router that takes a
#: ``(dcc, local_id)`` pair. DCC accessions across ENCODE / 4DN / HuBMAP are
#: all subsets of ``[A-Za-z0-9._-]``; the length cap defends Mongo and log
#: lines from unbounded input.
PATH_PARAM_PATTERN: Final[str] = r"^[A-Za-z0-9._-]+$"
PATH_PARAM_MAX_LEN: Final[int] = 256

FILE_DOC_PROJECTION: Final[dict[str, int]] = {
    "_id": 0,
    "local_id": 1,
    "md5": 1,
    "submission": 1,
    "dcc.dcc_abbreviation": 1,
    "file_format.name": 1,
    "access_url": 1,
    "filename": 1,
    "data_access_level": 1,
    "size_in_bytes": 1,
    # Promoted to the top level by every DCC's sync: ENCODE via
    # ``_add_extra(doc, "genome_assembly", ...)``, 4DN and HuBMAP via the
    # ``top_level_map`` / direct ``update``. The ``extra.*.genome_assembly``
    # fields declared on the enriched models are never written, so this is
    # the only path that carries an assembly. Read by the tileset routers to
    # derive HiGlass's ``coordSystem``.
    "genome_assembly": 1,
    "extra.extra_files": 1,
    "extra.fourdn.extra_files": 1,
}


async def lookup_file_doc(
    db, normalized_dcc: str, local_id: str
) -> dict[str, Any] | None:
    """Look up a file document for ``(normalized_dcc, local_id)``.

    Tries the materialized ``files`` collection first, then falls back to the
    raw ``file`` collection. Both queries use ``submission`` so /data and
    /index converge on the same record and the workflow_key mutex actually
    serializes concurrent requests for the same source file.

    Returns the projected document or ``None`` if not found.
    """
    file_doc = await db.files.find_one(
        {"submission": normalized_dcc, "local_id": local_id},
        projection=FILE_DOC_PROJECTION,
    )
    if file_doc is None:
        file_doc = await db.file.find_one(
            {"submission": normalized_dcc, "local_id": local_id},
            projection=FILE_DOC_PROJECTION,
        )
    return file_doc


def enforce_hubmap_access(normalized_dcc: str, file_doc: dict[str, Any]) -> None:
    """Reject non-public HuBMAP files with HTTP 403.

    Idempotent — safe to call on any route handler that handles HuBMAP
    requests, including ones that route to upstream streaming, the workflow
    cache, or the 4DN sidecar fast path (HuBMAP shouldn't have one but the
    guard keeps the policy uniform).
    """
    if normalized_dcc != "hubmap":
        return
    data_access_level = file_doc.get("data_access_level")
    # Fail-closed: only an explicit ``"public"`` value permits access.
    # None / empty string / missing field / protected / consortium all
    # block. A HuBMAP document missing the access-level field is a data
    # quality problem; defense-in-depth refuses to assume public.
    if data_access_level != "public":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"This file requires {data_access_level or 'unspecified'} "
                "access and is not available through this API. This API "
                "only serves publicly accessible files. For access to "
                "HuBMAP data, please use the HuBMAP Portal at "
                "https://portal.hubmapconsortium.org/"
            ),
        )


async def resolve_file_doc(
    dcc: str, local_id: str, *, require_access_url: bool = False
) -> tuple[str, dict[str, Any]]:
    """Run the per-request preamble every file-serving router shares.

    In order: normalize and validate the DCC (400), check the database is
    wired (500), look the document up (404), optionally require an access
    method (501), and enforce the HuBMAP access guard (403).

    Args:
        dcc: Raw DCC path parameter, normalized here.
        local_id: The file's identifier within the DCC.
        require_access_url: When True, reject a record with no
            ``access_url`` with 501 *before* the HuBMAP guard runs. This
            reproduces ``/data``'s ordering; ``/index`` and the tileset
            routers do not check for an access URL at all and leave it
            False.

    Returns:
        ``(normalized_dcc, file_doc)``.

    Raises:
        HTTPException: 400, 500, 404, 501, or 403 as described above.

    Note:
        The caller keeps ``await locks.wait_for_cutover()``. It stays in
        the handlers because that is where every route test patches it,
        and because a cutover wait is about request admission rather than
        about resolving a document.
    """
    normalized_dcc = normalize_dcc_name(dcc)
    valid_dccs = get_all_dcc_names()

    if normalized_dcc not in valid_dccs:
        logger.warning(f"Invalid DCC requested: {dcc}")
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

    logger.info(f"Looking up file: submission={normalized_dcc}, local_id={local_id}")
    file_doc = await lookup_file_doc(api.db, normalized_dcc, local_id)

    if not file_doc:
        logger.warning(f"File not found: {normalized_dcc}/{local_id}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="File not found"
        )

    # Checked before the HuBMAP guard, matching ``/data``'s existing
    # ordering. Guarding here is what lets the caller treat
    # ``file_doc["access_url"]`` as a plain str rather than an Optional
    # that the call ordering happens to have narrowed.
    if require_access_url and not file_doc.get("access_url"):
        logger.warning(f"File has no access_url: {normalized_dcc}/{local_id}")
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="File has no access URL",
        )

    # Defense-in-depth: reject any non-public HuBMAP file that survived
    # pruning, before any caller streams upstream bytes, caches them, or
    # dispatches a workflow.
    #
    # Runs after the access-url check so signed/private URLs for
    # non-public HuBMAP files are never logged ahead of the 403.
    enforce_hubmap_access(normalized_dcc, file_doc)

    return normalized_dcc, file_doc
