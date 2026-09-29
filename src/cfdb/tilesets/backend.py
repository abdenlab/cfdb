"""The single import gate for the clodius tile backend.

Every import of clodius in cfdb goes through here, so the whole subsystem
has exactly one on/off seam. That matters because clodius is currently an
optional extra installed from a sibling checkout (the fork is private, so
the Dockerfiles cannot fetch it) — which means production images do not
carry it. Concentrating the import lets that degrade to a clear 501 on the
tile routes instead of an ImportError at application startup or, worse, a
subsystem that half-works.

The gate also resolves ``clodius.tiles_v2.hic`` separately from the cooler
tileset. The module is not part of the pinned build — cfdb does not serve
``.hic`` regardless (see :func:`load_hic_tileset` for why), so the two
capabilities are kept distinct rather than collapsed into one availability
flag.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cfdb.tilesets.errors import TileBackendUnavailable


@dataclass(frozen=True)
class ClodiusBackend:
    """The clodius names cfdb uses, resolved once.

    Held as data rather than imported at module scope so that a build
    without clodius can still import every cfdb module.
    """

    #: The mcool tileset implementation.
    cooler_tileset: type
    #: Base of clodius's whole deliberate-error hierarchy (``TilesetError``,
    #: not the narrower ``TileError``). Anything deriving from this is a
    #: problem cfdb can render per-tile-id — a malformed id, an unusable
    #: transform, an out-of-ladder position — rather than a bug; anything
    #: else becomes a 500. Named ``tile_error`` for the call sites that
    #: consult it, which all classify at tile-id granularity even though
    #: some of what it now admits (e.g. ``MalformedTileId``) is raised
    #: before a tile is ever read.
    tile_error: type[Exception]
    #: Raised for a tile position the ladder does not hold. ``CoolerTileset``
    #: catches it internally and renders it as that tile's payload — see
    #: :meth:`~cfdb.tilesets.service.TilesetService._read_tiles` — but named
    #: here so the contract is visible at the boundary.
    tile_out_of_bounds: type[Exception]


def load_backend() -> ClodiusBackend:
    """Resolve the clodius tile backend.

    Raises:
        TileBackendUnavailable: when clodius is not installed.
    """
    try:
        from clodius.core.errors import TileOutOfBounds, TilesetError
        from clodius.tiles_v2.cooler import CoolerTileset
    except ImportError as exc:  # pragma: no cover - exercised via monkeypatch
        raise TileBackendUnavailable(
            "Matrix tile serving requires the clodius tile backend, which "
            "this build does not carry. Install the 'tiles' extra "
            "(`uv sync --extra tiles`, which resolves clodius from the "
            "sibling checkout) to enable it."
        ) from exc

    return ClodiusBackend(
        cooler_tileset=CoolerTileset,
        tile_error=TilesetError,
        tile_out_of_bounds=TileOutOfBounds,
    )


def is_available() -> bool:
    """True when tiles can be served at all.

    Used by the lifespan to decide whether to construct the tile service,
    and by the preparation channel to answer 503 rather than promising a
    readiness it cannot deliver.
    """
    try:
        load_backend()
    except TileBackendUnavailable:
        return False
    return True


def load_hic_tileset() -> type:
    """Resolve the ``.hic`` tileset implementation.

    Resolving it is not the same as being able to use it. clodius's
    ``.hic`` tileset is built on ``hictkpy``, whose entry points take
    ``str | os.PathLike`` and nothing else — there is no URL, HTTP, or S3
    surface anywhere in the library. So a ``.hic`` can only be served from
    a local copy, and cfdb does not make one: across the ENCODE corpus
    those files total ~78 TB, with a median of 10 GB and a largest single
    file of 315 GB, which exceeds the 200 GiB ceiling on Fargate ephemeral
    storage and so could not be cached at any budget.

    That is a deliberate deferral rather than a missing import, which is
    why the refusal lives at the call site in
    :meth:`~cfdb.tilesets.service.TilesetService._open_hic` and not here.
    This function still exists so a clodius that *loses* the module
    reports the more accurate error.

    Raises:
        TileBackendUnavailable: when clodius carries no ``.hic`` tileset.
    """
    try:
        from clodius.tiles_v2.hic import HicTileset
    except ImportError as exc:
        raise TileBackendUnavailable(
            "Serving tiles from .hic files requires clodius.tiles_v2.hic, "
            "which the installed clodius does not provide. 4DN .mcool "
            "files are unaffected."
        ) from exc
    return HicTileset


def is_tile_error(backend: ClodiusBackend, exc: BaseException) -> bool:
    """True when ``exc`` is a clodius per-tile error rather than a bug.

    Kept here so that ``except`` clauses elsewhere never need a
    module-scope clodius import of their own.
    """
    return isinstance(exc, backend.tile_error)


def datatype_of(tileset: Any) -> str:
    """The HiGlass ``datatype`` a tileset serves.

    Read off the class rather than hardcoded to ``"matrix"``: it is
    declared on the clodius ``Tileset`` protocol, and reading it is what
    keeps every layer above ``resolve_tileset`` free of knowledge about
    which concrete tileset it is holding.
    """
    return type(tileset).datatype
