"""Pluggable cache backend for workflow artifacts.

The cache is a byte-range-aware content store keyed by strings produced by
``workflows.keys.cache_key``. Two concrete backends ship: ``LocalFsCache``
for development and unit tests that don't need a network round-trip, and
``S3Cache`` for production (and for LocalStack-backed dev that mirrors
production end-to-end via boto3).

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
from typing import Any, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError


@dataclass(frozen=True)
class CacheEntry:
    """Metadata for a cached artifact."""

    key: str
    size: int


class CacheBackend(ABC):
    """Abstract cache backend.

    A pure ``head`` / ``get`` / ``put`` / ``delete`` keyed store. The
    backend is handed across the Wool boundary to the worker so
    processors persist artifacts through it directly; nothing in the
    interface assumes a filesystem (``LocalFsCache`` keeps a private
    root; ``S3Cache`` has none).
    """

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


def _validate_cache_key(key: str) -> None:
    """Reject path-traversal segments in a cache key.

    Rejects empty keys and keys containing path-traversal segments so that
    malformed input cannot escape the cache root or collapse onto the
    root directory itself. Shared by both backends so the rule (and its
    error message) stays in one place.
    """
    if not key or not key.strip("/"):
        raise ValueError(f"Cache key must be non-empty: {key!r}")
    if ".." in key.split("/"):
        raise ValueError(f"Invalid cache key: {key!r}")
    if key.startswith("/"):
        raise ValueError(f"Cache key must not start with '/': {key!r}")


def _safe_key_path(root: Path, key: str) -> Path:
    """Resolve a cache key to an absolute path under ``root``.

    Validates the key shape and verifies the resolved path stays under
    ``root`` so a malformed key cannot escape the cache root.
    """
    _validate_cache_key(key)
    path = (root / key).resolve()
    root_resolved = root.resolve()
    if root_resolved not in path.parents:
        raise ValueError(f"Cache key escapes cache root: {key!r}")
    return path


class LocalFsCache(CacheBackend):
    """Local-filesystem backend. Keys map directly to relative paths.

    ``put`` atomically renames the caller's source file into place via
    ``os.replace``. ``get`` supports byte-range reads and chunks by 64
    KiB to keep streaming memory-bounded.
    """

    def __init__(self, root: Path) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)

    @property
    def root(self) -> Path:
        return self._root

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


class S3Cache(CacheBackend):
    """Boto3-backed cache for production (and LocalStack-backed dev).

    The same code targets real S3 and LocalStack — the only difference
    is the ``endpoint_url`` passed to ``boto3.client("s3")``. Keys are
    stored as object keys (optionally under a configurable prefix);
    range reads use S3's ``Range`` header verbatim, so the fetcher
    semantics are identical to ``LocalFsCache``.

    Args:
        bucket: Bucket name. Must already exist (LocalStack and prod
            both treat bucket creation as an out-of-band concern).
        prefix: Optional key prefix; useful for sharing a single bucket
            across multiple environments. Empty string by default.
        client: Optional pre-built boto3 ``s3`` client. When omitted,
            one is constructed via :func:`_build_s3_client` with the
            ``endpoint_url`` / ``region_name`` kwargs threaded through.
            Tests inject a moto-backed client through this argument.
        endpoint_url: Boto3 ``endpoint_url``. Passed to
            :func:`_build_s3_client` when ``client`` is omitted. The
            lifespan plumbs :data:`cfdb.api.AWS_ENDPOINT_URL` here so
            LocalStack vs production differ only at this seam.
        region_name: Boto3 ``region_name``. Plumbed analogously.
        chunk_size: Streaming chunk size for ``get`` reads. Defaults to
            64 KiB to match the local filesystem backend.
    """

    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "",
        client: Optional[Any] = None,
        endpoint_url: Optional[str] = None,
        region_name: Optional[str] = None,
        chunk_size: int = _CHUNK_SIZE,
    ) -> None:
        if not bucket:
            raise ValueError("S3Cache requires a non-empty bucket name")
        if client is not None and (endpoint_url or region_name):
            raise ValueError(
                "S3Cache: pass either client or endpoint_url/region_name, "
                "not both — the boto kwargs are silently ignored when client is set"
            )
        self._bucket = bucket
        # Normalize so callers can pass either ``"prefix"`` or ``"prefix/"``.
        self._prefix = prefix.strip("/")
        self._endpoint_url = endpoint_url
        self._region_name = region_name
        self._client = (
            client
            if client is not None
            else _build_s3_client(endpoint_url=endpoint_url, region_name=region_name)
        )
        self._chunk_size = chunk_size

    def _object_key(self, key: str) -> str:
        """Apply the configured prefix to a validated cache key."""
        _validate_cache_key(key)
        return f"{self._prefix}/{key}" if self._prefix else key

    def __getstate__(self) -> dict[str, Any]:
        """Strip the boto3 client for pickling.

        ``S3Cache`` is dispatched across the cloudpickle boundary into
        Wool worker processes; botocore's ``BaseClient`` cannot be
        pickled. ``__setstate__`` rebuilds it via ``_build_s3_client``
        during unpickling, threading the originally-supplied
        ``endpoint_url`` / ``region_name`` through so the worker
        targets the same backend as the API process.
        """
        state = self.__dict__.copy()
        state["_client"] = None
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restore state and rebuild the boto3 client on the worker."""
        self.__dict__.update(state)
        if self._client is None:
            self._client = _build_s3_client(
                endpoint_url=self._endpoint_url,
                region_name=self._region_name,
            )

    async def head(self, key: str) -> Optional[CacheEntry]:
        """Return cache metadata for ``key``, or None if the object is absent.

        ``ClientError`` covers HTTP-level S3 errors with structured
        response codes; ``BotoCoreError`` covers transport / cred
        failures with no ``.response``. ``_is_not_found`` returns
        False for the latter family, so transport failures correctly
        re-raise rather than masquerade as cache miss.

        Note: S3 ``HEAD`` responses carry no body, so a missing
        bucket is indistinguishable from a missing object at this
        endpoint — both surface as a bare ``404``. The lifespan
        startup probes the bucket separately (see
        ``check_s3_bucket_or_raise``) so a typo in
        ``WORKFLOW_S3_BUCKET`` fails fast at boot rather than as a
        cascade of "permanent cache miss" symptoms.
        """
        object_key = self._object_key(key)
        try:
            response = await asyncio.to_thread(
                self._client.head_object,
                Bucket=self._bucket,
                Key=object_key,
            )
        except (ClientError, BotoCoreError) as exc:
            if _is_not_found(exc):
                return None
            raise
        return CacheEntry(key=key, size=int(response["ContentLength"]))

    def get(
        self, key: str, byte_range: Optional[tuple[int, int]] = None
    ) -> AsyncIterator[bytes]:
        """Stream cached bytes from S3, optionally restricted to a byte range.

        ``GetObject`` accepts an inclusive ``Range`` header; we forward
        the tuple verbatim. A missing object yields an empty iterator,
        matching ``LocalFsCache`` semantics so router code can treat
        cache misses uniformly across backends.
        """
        return _stream_s3_object(
            self._client,
            self._bucket,
            self._object_key(key),
            byte_range,
            self._chunk_size,
        )

    async def put(self, key: str, source_path: Path) -> CacheEntry:
        """Upload ``source_path`` to S3 under the configured key.

        ``upload_file`` is atomic from the reader's perspective —
        readers either see the prior object (if any) or the new one,
        never a partial write. Boto3 streams the file in a thread so
        the event loop isn't blocked. Size is taken from the source
        file rather than a follow-up ``HEAD`` to avoid the round-trip.

        When the source file has been torn down between upload
        completion and the local ``stat`` (workdir cleanup races
        finalization), we fall back to a ``head_object`` round-trip
        rather than surface ``FileNotFoundError`` for an upload that
        is already committed.
        """
        object_key = self._object_key(key)
        await asyncio.to_thread(
            self._client.upload_file,
            str(source_path),
            self._bucket,
            object_key,
        )
        try:
            size = await asyncio.to_thread(source_path.stat)
        except FileNotFoundError:
            try:
                response = await asyncio.to_thread(
                    self._client.head_object,
                    Bucket=self._bucket,
                    Key=object_key,
                )
            except (ClientError, BotoCoreError) as head_exc:
                raise RuntimeError(
                    f"S3Cache.put: source file vanished post-upload and "
                    f"head_object follow-up failed for {object_key!r}"
                ) from head_exc
            return CacheEntry(key=key, size=int(response["ContentLength"]))
        return CacheEntry(key=key, size=size.st_size)

    async def delete(self, key: str) -> bool:
        """Delete the cache entry. Returns True when the object existed.

        ``DeleteObject`` returns 204 whether or not the key existed, so
        we probe with HEAD first to give callers the existence signal.
        The HEAD/DELETE pair is non-atomic — a concurrent ``put`` between
        them returns ``False`` ("did not exist") yet erases the freshly-
        uploaded object. Callers MUST serialize ``put``/``delete`` for the
        same key via the workflow mutex; the cache backend itself does
        not arbitrate concurrent writers.

        Non-404 ``ClientError`` / ``BotoCoreError`` from the underlying
        ``head_object`` probe propagate; the boolean return is only
        meaningful when the probe succeeds.
        """
        object_key = self._object_key(key)
        existed = await self.head(key) is not None
        await asyncio.to_thread(
            self._client.delete_object,
            Bucket=self._bucket,
            Key=object_key,
        )
        return existed


def _build_s3_client(
    *, endpoint_url: Optional[str] = None, region_name: Optional[str] = None
) -> Any:
    """Construct a boto3 ``s3`` client with explicit endpoint/region.

    The caller (typically :class:`S3Cache` or the API lifespan) is the
    single source of truth for ``endpoint_url`` and ``region_name``;
    this function does not consult :mod:`cfdb.api` for fallback
    values. Leave both ``None`` to let boto3's default session
    resolver chain pick them up from the environment.
    """
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region_name,
    )


async def check_s3_bucket_or_raise(bucket: str, *, client: Optional[Any] = None) -> None:
    """Verify ``bucket`` is reachable; raise on missing/inaccessible.

    Run from the API lifespan so a typo in ``WORKFLOW_S3_BUCKET`` (or
    a missing IAM grant) fails fast at boot rather than masquerading
    as a permanent cache-miss cascade once workflows start. ``HEAD``
    on a missing bucket returns a bare ``404`` that ``S3Cache.head``
    cannot distinguish from a missing object — this probe asks
    ``head_bucket`` directly, which gets a structured response.
    """
    cli = client if client is not None else _build_s3_client()
    try:
        await asyncio.to_thread(cli.head_bucket, Bucket=bucket)
    except (ClientError, BotoCoreError) as exc:
        raise RuntimeError(
            f"S3 bucket {bucket!r} is not reachable: {type(exc).__name__}: {exc}"
        ) from exc


#: Object-level "missing" codes only. ``NoSuchBucket`` is deliberately
#: NOT in this set: a missing bucket is a configuration failure (typo
#: in WORKFLOW_S3_BUCKET, missing IAM, region mismatch) that should
#: surface as an exception rather than silently masquerade as a
#: permanent cache miss.
_NOT_FOUND_CODES = frozenset({"404", "NoSuchKey", "NotFound"})


def _is_not_found(exc: BaseException) -> bool:
    """Return True when a boto3 exception indicates a missing object.

    ``ClientError`` carries a structured ``response`` dict with both an
    ``Error.Code`` (string) and ``ResponseMetadata.HTTPStatusCode``
    (int). We check both: head_object responses use the bare 404 code,
    get_object uses the named ``NoSuchKey``. ``BotoCoreError`` (network
    / credential failures) has no ``response`` and is correctly
    classified as not-a-not-found, so it propagates.
    """
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    code = (response.get("Error") or {}).get("Code")
    if code in _NOT_FOUND_CODES:
        return True
    # S3 returns HTTP 404 with ``Error.Code = "NoSuchBucket"`` for a
    # missing bucket. The status-code fallback below would otherwise undo
    # the allowlist's deliberate exclusion of ``NoSuchBucket`` — keep
    # configuration failures loud.
    if code == "NoSuchBucket":
        return False
    status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
    return status == 404


async def _await_close(close: Any) -> None:
    """Run ``close`` in a worker thread and wait for it under cancellation.

    ``asyncio.shield`` keeps the close-thread's task uncancelled, but a
    cancel of the surrounding await leaves the future unretrieved (logs
    "exception was never retrieved" and "Task was destroyed but it is
    pending" on loop shutdown). Loop until the task finishes, swallowing
    intermediate cancels; re-raise once at the end so the cancel still
    propagates to the caller after the FD/connection release completes.
    """
    close_task = asyncio.ensure_future(asyncio.to_thread(close))
    cancelled = False
    while not close_task.done():
        try:
            await asyncio.shield(close_task)
        except asyncio.CancelledError:
            cancelled = True
    if cancelled:
        raise asyncio.CancelledError


async def _stream_s3_object(
    client: Any,
    bucket: str,
    object_key: str,
    byte_range: Optional[tuple[int, int]],
    chunk_size: int,
) -> AsyncIterator[bytes]:
    """Async generator yielding chunks from an S3 object.

    Diverges from ``_stream_file`` on mid-stream object disappearance:
    a deletion between the ``GetObject`` response and the body read
    raises a ``ClientError`` to the consumer rather than truncating
    silently. Callers serialize put/delete via the workflow mutex
    (see ``S3Cache.delete``), so the divergence is benign in practice.
    """
    kwargs: dict[str, Any] = {"Bucket": bucket, "Key": object_key}
    if byte_range is not None:
        start, end = byte_range
        if start < 0 or end < start:
            raise ValueError(
                f"byte_range must satisfy 0 <= start <= end; got {byte_range!r}"
            )
        kwargs["Range"] = f"bytes={start}-{end}"
    try:
        response = await asyncio.to_thread(client.get_object, **kwargs)
    except (ClientError, BotoCoreError) as exc:
        if _is_not_found(exc):
            return
        raise
    body = response["Body"]
    try:
        while True:
            chunk = await asyncio.to_thread(body.read, chunk_size)
            if not chunk:
                break
            yield chunk
    finally:
        close = getattr(body, "close", None)
        if close is not None:
            await _await_close(close)


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
        await _await_close(fh.close)
