"""Cached tileset artifact to local filesystem path.

The tile backend opens a file: ``CoolerTileset`` hands its path to
``h5py.File``, which needs random access to real bytes on a real
filesystem. ``CacheBackend.get`` yields a byte stream, which is the right
shape for ``/data`` and the wrong shape here.

On the local profile that mismatch is free — a ``LocalFsCache`` artifact
already *is* a local file, so it is opened in place. On the S3 profile the
artifact has to be pulled down first, which this module does through a
single-flight, byte-budgeted LRU on the API task's disk.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from cfdb.tilesets.errors import TilesetTooLarge
from cfdb.workflows.cache import CacheBackend, LocalFsCache
from cfdb.workflows.processors.tools import copy_from_cache

logger = logging.getLogger(__name__)

#: Asked before a hydrated file is unlinked. Returns True when the file is
#: no longer in use and may be removed. The tile service supplies one that
#: closes the corresponding open tileset first — unlinking a file whose
#: h5py handle is live is not an error you get to catch.
ReleaseHook = Callable[[str], Awaitable[bool]]


async def _always_releasable(_key: str) -> bool:
    """Default release hook: nothing is holding anything open."""
    return True


@dataclass
class _Resident:
    """A hydrated artifact on local disk."""

    path: Path
    size: int


class LocalTilesetStore:
    """Gives the tile server a local path for a cached artifact.

    Never reaches upstream. A cache miss means the artifact has not been
    built, which is the preparation channel's problem, not this one's.
    """

    def __init__(
        self,
        root: Path,
        *,
        max_bytes: int,
        release_hook: ReleaseHook | None = None,
    ) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._max_bytes = max_bytes
        self._release = release_hook or _always_releasable
        #: Hydrated files, least-recently-used first.
        self._resident: OrderedDict[str, _Resident] = OrderedDict()
        #: One lock per key, so N concurrent tile requests for a cold
        #: dataset produce one download rather than N.
        self._inflight: dict[str, asyncio.Lock] = {}

    def set_release_hook(self, hook: ReleaseHook) -> None:
        """Attach the hook eviction consults before unlinking.

        Set after construction because the service and the store hold
        references to each other: the service asks the store for a path,
        and the store asks the service whether a file may go.
        """
        self._release = hook

    @property
    def resident_bytes(self) -> int:
        """Total size of the hydrated files currently on disk."""
        return sum(entry.size for entry in self._resident.values())

    async def path_for(
        self, cache: CacheBackend, cache_key: str, *, expected_size: int
    ) -> Path:
        """Return a local path holding the artifact at ``cache_key``.

        Args:
            cache: The configured cache backend.
            cache_key: Key the artifact was committed under.
            expected_size: The artifact's size, from ``cache.head``. Used
                to make room before downloading rather than after.

        Raises:
            TilesetTooLarge: when the artifact alone exceeds the whole
                disk budget.
        """
        if isinstance(cache, LocalFsCache):
            # Already a local file. Opening it in place is not just an
            # optimization: it is what lets the entire tile subsystem be
            # exercised in tests without an S3 stand-in.
            return cache.path_for(cache_key)

        if expected_size > self._max_bytes:
            raise TilesetTooLarge(
                f"artifact {cache_key} is {expected_size} bytes, which "
                f"exceeds the whole {self._max_bytes}-byte local tileset "
                "budget (CFDB_TILESET_DISK_CACHE_BYTES)"
            )

        lock = self._inflight.setdefault(cache_key, asyncio.Lock())
        async with lock:
            resident = self._resident.get(cache_key)
            if resident is not None and resident.path.exists():
                self._resident.move_to_end(cache_key)
                return resident.path
            return await self._hydrate(cache, cache_key, expected_size)

    async def _hydrate(
        self, cache: CacheBackend, cache_key: str, expected_size: int
    ) -> Path:
        """Download the artifact onto local disk, evicting to make room."""
        await self._make_room(expected_size)

        dest = self._local_path(cache_key)
        partial = dest.with_suffix(dest.suffix + ".part")
        logger.info(
            f"Hydrating tileset artifact {cache_key} ({expected_size} bytes)"
        )
        try:
            await copy_from_cache(cache, cache_key, partial)
            # Rename rather than write in place, so a download interrupted
            # by cancellation or a timeout can never be mistaken for a
            # complete artifact and handed to h5py.
            await asyncio.to_thread(os.replace, str(partial), str(dest))
        except BaseException:
            partial.unlink(missing_ok=True)
            raise

        self._resident[cache_key] = _Resident(path=dest, size=expected_size)
        self._resident.move_to_end(cache_key)
        return dest

    async def _make_room(self, incoming: int) -> None:
        """Evict least-recently-used artifacts until ``incoming`` fits."""
        for key in list(self._resident):
            if self.resident_bytes + incoming <= self._max_bytes:
                return
            if not await self._release(key):
                # Something is reading it. Skip rather than unlink: closing
                # an h5py handle out from under a numpy read in a worker
                # thread is a segfault, not an exception.
                logger.debug(f"Skipping eviction of in-use tileset {key}")
                continue
            self._evict(key)

        # Falling through means every remaining resident is pinned. Let the
        # hydration proceed and overshoot the budget rather than failing a
        # request; the overshoot is bounded by the open-tileset cap, and
        # the next eviction pass reclaims it.
        if self.resident_bytes + incoming > self._max_bytes:
            logger.warning(
                "Local tileset budget exceeded: %d resident bytes plus %d "
                "incoming against a %d-byte budget, and every resident "
                "artifact is in use",
                self.resident_bytes,
                incoming,
                self._max_bytes,
            )

    def _evict(self, key: str) -> None:
        """Drop one artifact from disk and from the residency table."""
        entry = self._resident.pop(key, None)
        if entry is None:
            return
        entry.path.unlink(missing_ok=True)
        self._inflight.pop(key, None)
        logger.info(f"Evicted local tileset artifact {key}")

    def _local_path(self, cache_key: str) -> Path:
        """Flat, collision-free filename for a cache key.

        Hashed rather than mirroring the key's directory structure: the
        key is already validated against traversal by the cache layer, and
        a flat namespace keeps eviction a single ``unlink`` with no empty
        directories left behind.
        """
        digest = hashlib.sha256(cache_key.encode()).hexdigest()
        return self._root / f"{digest}.tileset"

    async def aclose(self) -> None:
        """Drop every hydrated artifact.

        Called on lifespan teardown. The files are a cache of a cache;
        leaving them behind would leak the API task's disk across
        restarts, since the hashed names carry no expiry of their own.
        """
        for key in list(self._resident):
            self._evict(key)
