"""Failure vocabulary for the tile subsystem.

These are cfdb's own, distinct from clodius's ``TileError`` hierarchy. The
split is deliberate: clodius's errors describe things wrong with a *tile
request* against a tileset that exists, and are translated into per-tile
error payloads; these describe reasons cfdb cannot produce a tileset at
all, and become HTTP status codes.
"""

from __future__ import annotations


class TilesetError(Exception):
    """Base for every reason cfdb cannot serve tiles for a file."""


class TilesetUnsupported(TilesetError):
    """The file is not a contact map, so it has no tiles in any state.

    Rendered as 404 — permanently, not "not yet".
    """


class TilesetNotReady(TilesetError):
    """The file is a contact map but its artifact has not been built.

    Rendered as 404 by the tile endpoints, which never dispatch. Callers
    who want it built ask the preparation channel
    (``POST /tilesets/{dcc}/{local_id}``); a heatmap track cannot be handed
    a job id, so there is nothing useful to say here beyond "not here".
    """


class TilesetIdentityIncomplete(TilesetError):
    """The document lacks the md5 the cache key is derived from.

    A stale or hand-edited record rather than a request to 500 on.
    """


class TileBackendUnavailable(TilesetError):
    """The installed build carries no usable tile backend.

    Rendered as 501. Two distinct causes share it: clodius is absent
    entirely (a broken or incomplete install — it is an ordinary
    dependency, see ``pyproject.toml``), or the request is for a ``.hic``,
    which cfdb deliberately does not serve because the backend reads
    local files only and those files are too large to cache.
    """


class TilesetTooLarge(TilesetError):
    """The artifact will not fit in the local disk budget.

    Rendered as 503 rather than 404: the file is genuinely tileable, and
    a larger ``CFDB_TILESET_DISK_CACHE_BYTES`` would serve it. Raised
    instead of evicting the entire cache for one oversized file, which
    would thrash without ever succeeding.
    """


class TilesetHydrationTimeout(TilesetError):
    """Pulling the artifact onto local disk exceeded its budget.

    Rendered as 503 with ``Retry-After``: the download may well finish,
    and a retry will find it.
    """


class TilesetSourceUnavailable(TilesetError):
    """Resolving or reading a remote-source tileset's upstream URL failed.

    Only raised by formats served without local materialization (today,
    bigInteract — see
    :meth:`~cfdb.tilesets.service.TilesetService._open_bbi_interaction`).
    Distinct from :class:`TilesetNotReady`: there is no "not yet built"
    state here, no preparation channel to ask — the failure is in
    reaching the DCC's own upstream file. ``status_code`` is not one
    fixed value, and its two sources differ in how closely they track
    ``/data`` (``cfdb.api.routers.data``): a DRS-resolution failure
    (object not found, access denied, upstream timeout or error) carries
    the same 404/403/504/502 that failure maps to on ``/data``, while a
    malformed or disallowed URL (the SSRF allowlist, a malformed DRS
    URI, or no usable access method) carries 400 as cfdb's own choice
    for the tileset path specifically — ``/data`` has no 400 case for
    any of these, so that one status is not a claim of parity. A batch
    entry still renders as a plain ``{"error": ...}`` object regardless
    of which, like every other per-dataset ``TilesetError``.
    """

    def __init__(self, message: str, *, status_code: int = 502) -> None:
        super().__init__(message)
        self.status_code = status_code
