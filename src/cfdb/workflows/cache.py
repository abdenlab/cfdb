"""Pluggable cache backend for workflow artifacts.

The cache is a byte-range-aware content store keyed by strings produced by
``workflows.keys.cache_key``. ``LocalFsCache`` is the concrete backend used
in local development and for the initial CVH rollout; an S3-backed
implementation with the same interface is planned for production.

Range-aware reads matter because Gosling's client-side fetchers (BAM/tabix
families, bbi) issue ``Range: bytes=…`` requests against the artifact URL.
The router parses the HTTP header and hands the resolved ``(start, end)``
tuple to the backend.
"""

from __future__ import annotations

import asyncio
import os
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class CacheEntry:
    """Metadata for a cached artifact."""

    key: str
    size: int


class CacheBackend(ABC):
    """Abstract cache backend."""

    @abstractmethod
    async def head(self, key: str) -> Optional[CacheEntry]:
        """Return cache metadata for ``key``, or None if absent."""

    @abstractmethod
    def get(
        self, key: str, byte_range: Optional[tuple[int, int]] = None
    ) -> AsyncIterator[bytes]:
        """Stream cached bytes.

        This is a regular function (not a coroutine); it returns the async
        iterator directly so callers can ``async for`` on the result
        without an extra ``await``.

        Args:
            key: Cache key.
            byte_range: Optional inclusive ``(start, end)`` byte range. When
                omitted, the full artifact is streamed.

        Returns:
            An async iterator yielding chunks of bytes. The iterator is
            empty if the key is absent.
        """

    @abstractmethod
    async def put(self, key: str, source_path: Path) -> CacheEntry:
        """Move/copy ``source_path`` into the cache under ``key``.

        The caller guarantees ``source_path`` contains the final artifact
        bytes. The backend writes atomically: partially-written entries
        MUST never be observable via ``head`` or ``get``.
        """

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Delete the cache entry. Return True if something was deleted."""


_CHUNK_SIZE = 1 << 16  # 64 KiB


def _safe_key_path(root: Path, key: str) -> Path:
    """Resolve a cache key to an absolute path under ``root``.

    Rejects empty keys and keys containing path-traversal segments so that
    malformed input cannot escape the cache root or collapse onto the
    root directory itself.
    """
    if not key or not key.strip("/"):
        raise ValueError(f"Cache key must be non-empty: {key!r}")
    if ".." in key.split("/"):
        raise ValueError(f"Invalid cache key: {key!r}")
    path = (root / key).resolve()
    root_resolved = root.resolve()
    if root_resolved not in path.parents:
        raise ValueError(f"Cache key escapes cache root: {key!r}")
    return path


class LocalFsCache(CacheBackend):
    """Local-filesystem backend. Keys map directly to relative paths.

    ``put`` writes via a temp sibling + atomic rename. ``get`` supports
    byte-range reads and chunks by 64 KiB to keep streaming memory-bounded.
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    async def head(self, key: str) -> Optional[CacheEntry]:
        """Return size metadata for a key, or None if absent."""
        path = _safe_key_path(self.root, key)
        try:
            stat = await asyncio.to_thread(path.stat)
        except FileNotFoundError:
            return None
        return CacheEntry(key=key, size=stat.st_size)

    def get(
        self, key: str, byte_range: Optional[tuple[int, int]] = None
    ) -> AsyncIterator[bytes]:
        """Stream cached bytes, optionally restricted to a byte range."""
        path = _safe_key_path(self.root, key)
        return _stream_file(path, byte_range)

    async def put(self, key: str, source_path: Path) -> CacheEntry:
        """Move ``source_path`` into the cache atomically.

        ``os.replace`` provides all-or-nothing semantics on POSIX when the
        source and destination are on the same filesystem, which they are
        by construction (both the per-job workdir and the cache root live
        under ``SYNC_DATA_DIR``).
        """
        dest = _safe_key_path(self.root, key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(os.replace, str(source_path), str(dest))
        stat = await asyncio.to_thread(dest.stat)
        return CacheEntry(key=key, size=stat.st_size)

    async def delete(self, key: str) -> bool:
        """Delete a cache entry. Idempotent."""
        path = _safe_key_path(self.root, key)
        try:
            await asyncio.to_thread(path.unlink)
            return True
        except FileNotFoundError:
            return False


async def _stream_file(
    path: Path, byte_range: Optional[tuple[int, int]]
) -> AsyncIterator[bytes]:
    """Async generator yielding chunks from a file, optionally range-bounded.

    Defensive contract: ``byte_range`` (when present) MUST be a closed
    inclusive ``(start, end)`` interval with ``0 <= start <= end``. Callers
    today produce these via ``drs.parse_range_header`` which already
    enforces RFC 7233 bounds, but the assert here keeps the cache
    backend's invariant explicit.
    """
    if byte_range is not None:
        start, end = byte_range
        if start < 0 or end < start:
            raise ValueError(
                f"byte_range must satisfy 0 <= start <= end; got {byte_range!r}"
            )

    def _open_and_seek():
        try:
            fh = path.open("rb")
        except FileNotFoundError:
            return None, 0
        if byte_range is None:
            return fh, -1
        start, end = byte_range
        fh.seek(start)
        remaining = end - start + 1
        return fh, remaining

    fh, remaining = await asyncio.to_thread(_open_and_seek)
    if fh is None:
        return
    try:
        while True:
            if remaining == 0:
                break
            read_size = _CHUNK_SIZE if remaining < 0 else min(_CHUNK_SIZE, remaining)
            chunk = await asyncio.to_thread(fh.read, read_size)
            if not chunk:
                break
            if remaining > 0:
                remaining -= len(chunk)
            yield chunk
    finally:
        await asyncio.to_thread(fh.close)
