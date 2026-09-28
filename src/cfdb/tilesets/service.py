"""Open tilesets, read tiles, and cache both.

Owns the three things a tile request needs that a single clodius call does
not provide: a local file to open, an open handle worth reusing across
thousands of requests, and somewhere to put the result so panning back
over the same region does not re-read it.

Concurrency shape, since it is the part that is easy to get wrong:

- clodius reads are synchronous h5py + numpy, so they run in a dedicated
  thread pool. Not the default executor — the cache and processor helpers
  already share that for byte streaming, and a multi-second tile read
  there would stall ``/data``.
- An open tileset may be evicted while a thread is mid-read. Closing an
  h5py handle under a live numpy read is a segfault, not an exception, so
  handles are reference-counted and eviction defers the close to the last
  reader.
"""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from cfdb.tilesets.backend import load_backend, load_hic_tileset
from cfdb.tilesets.errors import (
    TileBackendUnavailable,
    TilesetHydrationTimeout,
    TilesetIdentityIncomplete,
    TilesetNotReady,
    TilesetUnsupported,
)
from cfdb.tilesets.formats import MatrixSource, matrix_source_kind
from cfdb.tilesets.store import LocalTilesetStore
from cfdb.tilesets.wire import error_payload, tileset_info_payload
from cfdb.workflows.cache import CacheBackend
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.matrix import MatrixTilesetProcessor

logger = logging.getLogger(__name__)


@dataclass
class _OpenTileset:
    """An open tileset handle and the readers currently using it."""

    tileset: Any
    #: Readers inside :meth:`TilesetService.checkout`. A handle with
    #: readers is never closed, however urgently it is being evicted.
    in_flight: int = 0
    #: Set when the entry has been evicted. The last reader out closes it.
    closing: bool = False
    #: Byte size of the tiles cached against this tileset, for accounting.
    tile_bytes: int = field(default=0)


class TilesetService:
    """Serves HiGlass tileset info and tiles from cached artifacts."""

    def __init__(
        self,
        store: LocalTilesetStore,
        *,
        open_max: int,
        tile_cache_bytes: int,
        threads: int,
        hydrate_timeout_s: float,
        cache_provider: Callable[[], CacheBackend | None],
    ) -> None:
        self._store = store
        self._open_max = open_max
        self._tile_cache_bytes = tile_cache_bytes
        self._hydrate_timeout_s = hydrate_timeout_s
        self._cache_provider = cache_provider
        self._backend = load_backend()
        self._processor = MatrixTilesetProcessor()
        self._pool = ThreadPoolExecutor(
            max_workers=threads, thread_name_prefix="cfdb-tile"
        )
        self._open: OrderedDict[str, _OpenTileset] = OrderedDict()
        self._open_lock = asyncio.Lock()
        #: ``(cache_key, tile_id.raw)`` to payload, least-recently-used
        #: first. Keyed on the cache key rather than the uid because the
        #: key embeds ``md5-v{processor_version}``, so an upstream byte
        #: change or a processor bump invalidates every tile for free.
        self._tiles: OrderedDict[tuple[str, str], dict[str, Any]] = OrderedDict()
        self._tile_bytes = 0
        store.set_release_hook(self._release_for_eviction)

    # --- public API ---------------------------------------------------------

    async def tileset_info(self, uid: str, file_doc: dict[str, Any]) -> dict:
        """Return the HiGlass tileset_info document for ``uid``."""
        async with self.checkout(uid, file_doc) as tileset:
            return await self._run(
                lambda: tileset_info_payload(uid, tileset, file_doc)
            )

    async def tiles(
        self, uid: str, file_doc: dict[str, Any], raw_tile_ids: Sequence[str]
    ) -> dict[str, dict[str, Any]]:
        """Return tile payloads for ``raw_tile_ids``, keyed by request string.

        ``CoolerTileset.tiles`` returns exactly one entry per requested
        position: a position the resolution ladder does not hold, an id that
        fails to parse, or a modifier naming a balancing column the file
        does not carry all render as a per-tile error object in that
        position's slot rather than raising or being omitted — one bad id in
        a batch must not blank the whole track.
        """
        cache_key = self._cache_key(file_doc)

        payloads: dict[str, dict[str, Any]] = {}
        pending: list[str] = []
        for raw in raw_tile_ids:
            cached = self._tile_cache_get(cache_key, raw)
            if cached is not None:
                payloads[raw] = cached
            else:
                pending.append(raw)

        if not pending:
            return payloads

        async with self.checkout(uid, file_doc) as tileset:
            parsed = []
            for raw in pending:
                try:
                    parsed.append(tileset.parse_tile_id(raw))
                except Exception as exc:
                    payloads[raw] = self._translate_tile_error(raw, exc)

            if parsed:
                payloads.update(
                    await self._read_tiles(tileset, cache_key, parsed)
                )

        return payloads

    @asynccontextmanager
    async def checkout(
        self, uid: str, file_doc: dict[str, Any]
    ) -> AsyncIterator[Any]:
        """Borrow an open tileset for the duration of the block.

        The handle is reference-counted for as long as the block runs, so
        an eviction that lands mid-read defers its ``close()`` until the
        last reader leaves.
        """
        entry = await self._acquire(uid, file_doc)
        try:
            yield entry.tileset
        finally:
            entry.in_flight -= 1
            if entry.closing and entry.in_flight == 0:
                await self._close(entry)

    async def aclose(self) -> None:
        """Close every open handle and shut the thread pool down."""
        async with self._open_lock:
            entries = list(self._open.values())
            self._open.clear()
        for entry in entries:
            entry.closing = True
            if entry.in_flight == 0:
                await self._close(entry)
        self._tiles.clear()
        self._tile_bytes = 0
        await self._store.aclose()
        self._pool.shutdown(wait=False, cancel_futures=True)

    # --- opening ------------------------------------------------------------

    async def _acquire(self, uid: str, file_doc: dict[str, Any]) -> _OpenTileset:
        """Return an open tileset for ``uid``, opening one if needed."""
        cache_key = self._cache_key(file_doc)

        async with self._open_lock:
            entry = self._open.get(cache_key)
            if entry is not None and not entry.closing:
                self._open.move_to_end(cache_key)
                entry.in_flight += 1
                return entry

        tileset = await self._open_tileset(uid, file_doc, cache_key)

        async with self._open_lock:
            # Another request may have opened the same tileset while this
            # one was hydrating. Keep the winner and close the loser rather
            # than leaking a second h5py handle onto the same file.
            existing = self._open.get(cache_key)
            if existing is not None and not existing.closing:
                await self._run(tileset.close)
                self._open.move_to_end(cache_key)
                existing.in_flight += 1
                return existing

            entry = _OpenTileset(tileset=tileset, in_flight=1)
            self._open[cache_key] = entry
            evictable = self._select_evictions()

        for victim in evictable:
            await self._close(victim)
        return entry

    def _select_evictions(self) -> list[_OpenTileset]:
        """Drop over-cap entries from the table, returning ones safe to close.

        Called with ``_open_lock`` held. An entry with readers is marked
        ``closing`` and removed from the table — so no new reader can find
        it — but its ``close()`` is left to the last reader out.
        """
        victims: list[_OpenTileset] = []
        while len(self._open) > self._open_max:
            key, entry = self._open.popitem(last=False)
            entry.closing = True
            self._evict_tiles_for(key)
            if entry.in_flight == 0:
                victims.append(entry)
        return victims

    async def _open_tileset(
        self, uid: str, file_doc: dict[str, Any], cache_key: str
    ) -> Any:
        """Construct a tileset for ``file_doc``, without dispatching anything."""
        kind = matrix_source_kind(file_doc)
        if kind is None:
            raise TilesetUnsupported(f"{uid} is not a contact map")
        if kind is MatrixSource.HIC:
            return await self._open_hic(uid, file_doc)
        return await self._open_cooler(uid, cache_key)

    async def _open_cooler(self, uid: str, cache_key: str) -> Any:
        """Open the cached mcool artifact for ``uid``."""
        cache = self._cache_provider()
        if cache is None:
            raise TilesetNotReady(f"{uid} has no cache to read an artifact from")

        entry = await cache.head(cache_key)
        if entry is None:
            # Never dispatch from here. The tile endpoints are pure
            # readers; building the artifact is the preparation channel's
            # job, because a heatmap track cannot poll a job id.
            raise TilesetNotReady(f"{uid} has no cached tileset artifact")

        try:
            path = await asyncio.wait_for(
                self._store.path_for(cache, cache_key, expected_size=entry.size),
                timeout=self._hydrate_timeout_s,
            )
        except asyncio.TimeoutError as exc:
            raise TilesetHydrationTimeout(
                f"pulling the tileset artifact for {uid} onto local disk "
                f"exceeded {self._hydrate_timeout_s}s"
            ) from exc

        return await self._run(lambda: self._backend.cooler_tileset(str(path)))

    async def _open_hic(self, uid: str, file_doc: dict[str, Any]) -> Any:
        """Refuse a ``.hic`` — cfdb has no local artifact to open one from.

        The pinned clodius does not carry ``clodius.tiles_v2.hic`` at all
        (see :func:`~cfdb.tilesets.backend.load_hic_tileset`), which makes
        this refusal unconditional today. It would stay unconditional even
        if a future clodius did carry the module: it is built on
        ``hictkpy``, whose ``MultiResFile`` takes ``str | os.PathLike`` and
        nothing else — there is no way to read one *remotely*, no URL
        handling in it at all.

        That invalidates the assumption this endpoint was designed
        around. ``.hic`` was to be read in place over HTTP range requests
        precisely so the files would never have to be copied — and the
        measurement behind that plan was taken with ``hicstraw``, a
        different library, which does support range reads over HTTP.
        Against ``hictkpy`` the only way to serve a ``.hic`` is to
        materialize it first, and the corpus makes that a decision rather
        than a detail: 3,903 files, 78 TB in total, a median of 10 GB, and
        a largest single file of 315 GB — which exceeds the 200 GiB
        ceiling on Fargate ephemeral storage, so it could not be cached on
        an API task at any budget.

        Raising here keeps that decision visible instead of half-answering
        it. 4DN ``.mcool`` files are unaffected.
        """
        # Resolved for its side effect: surfaces the more specific
        # "module missing" error when that's the case, even though this
        # function refuses .hic regardless of whether the module is present.
        load_hic_tileset()
        raise TileBackendUnavailable(
            f"{uid} is a .hic, which cfdb cannot serve tiles from yet. The "
            "clodius .hic tileset reads local files only (hictkpy has no "
            "remote support), and cfdb does not materialize .hic artifacts. "
            "4DN .mcool contact maps are unaffected."
        )

    async def _close(self, entry: _OpenTileset) -> None:
        """Close a handle off the event loop."""
        try:
            await self._run(entry.tileset.close)
        except Exception:
            logger.exception("Failed to close a tileset handle")

    async def _release_for_eviction(self, cache_key: str) -> bool:
        """Store hook: may this artifact's file be unlinked?

        Closes the tileset holding it open when nothing is reading, and
        refuses otherwise. This is the coupling that keeps the disk store
        from pulling a file out from under a live h5py handle.
        """
        async with self._open_lock:
            entry = self._open.get(cache_key)
            if entry is None:
                return True
            if entry.in_flight:
                return False
            del self._open[cache_key]
            entry.closing = True
            self._evict_tiles_for(cache_key)
        await self._close(entry)
        return True

    # --- reading ------------------------------------------------------------

    async def _read_tiles(
        self, tileset: Any, cache_key: str, parsed: list[Any]
    ) -> dict[str, dict[str, Any]]:
        """Read tiles off the event loop and cache what comes back."""
        try:
            returned = await self._run(lambda: tileset.tiles(parsed))
        except Exception as exc:
            if not isinstance(exc, self._backend.tile_error):
                raise
            # A batch-level clodius error would apply to every id in the
            # batch. In practice `CoolerTileset.tiles` no longer raises for
            # this — it catches its own per-id and per-batch `TileError`s
            # internally and renders them inline in `returned` below — so
            # this branch guards a bug in the backend rather than an
            # expected path.
            message = str(exc)
            return {tid.raw: error_payload(message) for tid in parsed}

        payloads: dict[str, dict[str, Any]] = {}
        for tile_id, payload in returned:
            # clodius renders its own per-tile failures as
            # `{"error": ..., "error_type": ...}` (`TileError.to_dict`).
            # Normalize through `error_payload` so every error on the wire
            # — clodius's or cfdb's own parse-time translation below — has
            # the same `{"error": str}` shape.
            if "error" in payload and "dense" not in payload:
                payload = error_payload(str(payload["error"]))
            payloads[tile_id.raw] = payload
            self._tile_cache_put(cache_key, tile_id.raw, payload)
        return payloads

    def _translate_tile_error(self, raw: str, exc: Exception) -> dict[str, str]:
        """Render a per-tile failure, or re-raise a genuine bug."""
        if isinstance(exc, self._backend.tile_error):
            return error_payload(str(exc))
        raise exc

    # --- tile cache ---------------------------------------------------------

    def _tile_cache_get(self, cache_key: str, raw: str) -> dict | None:
        entry = self._tiles.get((cache_key, raw))
        if entry is not None:
            self._tiles.move_to_end((cache_key, raw))
        return entry

    def _tile_cache_put(
        self, cache_key: str, raw: str, payload: dict[str, Any]
    ) -> None:
        size = _payload_bytes(payload)
        if size > self._tile_cache_bytes:
            return
        key = (cache_key, raw)
        if key in self._tiles:
            self._tile_bytes -= _payload_bytes(self._tiles[key])
        self._tiles[key] = payload
        self._tiles.move_to_end(key)
        self._tile_bytes += size
        while self._tile_bytes > self._tile_cache_bytes:
            _, evicted = self._tiles.popitem(last=False)
            self._tile_bytes -= _payload_bytes(evicted)

    def _evict_tiles_for(self, cache_key: str) -> None:
        """Drop every cached tile belonging to one tileset."""
        for key in [k for k in self._tiles if k[0] == cache_key]:
            self._tile_bytes -= _payload_bytes(self._tiles.pop(key))

    @property
    def tile_cache_bytes_used(self) -> int:
        """Bytes currently held by the tile cache."""
        return self._tile_bytes

    # --- plumbing -----------------------------------------------------------

    def _cache_key(self, file_doc: dict[str, Any]) -> str:
        """The TILESET cache key for ``file_doc``.

        Derived through the processor rather than re-implemented, so the
        writer, the readiness probe, and this reader agree by construction
        rather than by three formulas kept in sync.
        """
        try:
            return self._processor.cache_key_for(file_doc, ArtifactKind.TILESET)
        except ValueError as exc:
            raise TilesetIdentityIncomplete(str(exc)) from exc

    async def _run(self, fn):
        """Run a blocking tile-backend call on the dedicated pool."""
        return await asyncio.get_running_loop().run_in_executor(self._pool, fn)


def _payload_bytes(payload: dict[str, Any]) -> int:
    """Approximate on-heap size of a tile payload.

    The base64 ``dense`` string dominates by orders of magnitude; the
    handful of floats beside it are not worth measuring.
    """
    dense = payload.get("dense")
    return len(dense) if isinstance(dense, (str, bytes)) else 0


def build_service(
    *,
    root: Path,
    cache_provider: Callable[[], CacheBackend | None],
    disk_cache_bytes: int,
    open_max: int,
    tile_cache_bytes: int,
    threads: int,
    hydrate_timeout_s: float,
) -> TilesetService:
    """Construct a service and the disk store behind it."""
    store = LocalTilesetStore(root, max_bytes=disk_cache_bytes)
    return TilesetService(
        store,
        open_max=open_max,
        tile_cache_bytes=tile_cache_bytes,
        threads=threads,
        hydrate_timeout_s=hydrate_timeout_s,
        cache_provider=cache_provider,
    )
