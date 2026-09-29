"""A shared non-local cache stand-in for the tile-serving tests.

Hoisted out of ``tests/test_tilesets/test_store.py`` so the service-layer
hydration tests and the router-level 503 tests can use the same fake
without importing from a test module.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

from cfdb.workflows.cache import CacheBackend, CacheEntry


class FakeRemoteCache(CacheBackend):
    """A cache that is not a LocalFsCache, so it must be hydrated.

    Stands in for ``S3Cache`` without needing moto: the store branches on
    the backend type, and everything past that branch is ``head``/``get``.
    """

    def __init__(self, blobs: dict[str, bytes]) -> None:
        self.blobs = blobs
        self.get_calls: list[str] = []
        self.gate: asyncio.Event | None = None

    async def head(self, key: str):
        blob = self.blobs.get(key)
        return None if blob is None else CacheEntry(key=key, size=len(blob))

    def get(self, key: str, byte_range=None) -> AsyncIterator[bytes]:
        self.get_calls.append(key)
        blobs, gate = self.blobs, self.gate

        async def _stream():
            if gate is not None:
                await gate.wait()
            yield blobs[key]

        return _stream()

    async def put(self, key: str, source_path: Path) -> CacheEntry:
        data = source_path.read_bytes()
        self.blobs[key] = data
        return CacheEntry(key=key, size=len(data))

    async def delete(self, key: str) -> bool:
        return self.blobs.pop(key, None) is not None
