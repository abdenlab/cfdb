"""Shared primitives for /data and /index routers.

Provides:

- ``FILE_DOC_PROJECTION``: the Mongo projection both routers use when reading
  a file document. Strips ``_id`` (so ``bson.ObjectId`` never crosses into the
  workflow subsystem) and limits the returned dict to the fields any router
  or workflow consumer reads.
- ``lookup_file_doc``: single canonical Mongo query so /data and /index hit
  the same record for a given ``(dcc, local_id)`` pair — preserves the
  "exactly one workflow under contention" invariant.
- ``enforce_hubmap_access``: defense-in-depth guard rejecting non-public
  HuBMAP files before any workflow dispatch or upstream stream.
"""

from __future__ import annotations

from typing import Any, Final

from fastapi import HTTPException, status

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
