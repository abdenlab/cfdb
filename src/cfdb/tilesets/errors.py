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
    entirely (the images do not yet carry it — see the ``tiles`` extra in
    ``pyproject.toml``), or the request is for a ``.hic``, which cfdb
    deliberately does not serve because the backend reads local files only
    and those files are too large to cache.
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
