"""Tests for the LocalFsCache implementation."""

from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from cfdb.workflows.cache import CacheEntry, LocalFsCache


def _write(path: Path, data: bytes) -> None:
    """Write bytes to a newly-created file, creating parents as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


async def _collect(stream) -> bytes:
    """Collect an async byte iterator into a single bytes object."""
    chunks: list[bytes] = []
    async for chunk in stream:
        chunks.append(chunk)
    return b"".join(chunks)


class TestLocalFsCache:
    @pytest.mark.asyncio
    async def test_head_should_return_none_when_key_absent(self, tmp_path):
        """Test that head signals a cache miss with None.

        Given:
            An empty LocalFsCache.
        When:
            head is awaited for a key that was never stored.
        Then:
            It should return None so the router can dispatch a workflow.
        """
        # Arrange
        cache = LocalFsCache(tmp_path)

        # Act
        entry = await cache.head("encode/x/data/aa-v0")

        # Assert
        assert entry is None

    @pytest.mark.asyncio
    async def test_head_should_return_entry_with_size_when_key_present(
        self, tmp_path
    ):
        """Test that head reports the stored artifact size.

        Given:
            A LocalFsCache containing one artifact.
        When:
            head is awaited for that key.
        Then:
            It should return a CacheEntry carrying the exact byte size.
        """
        # Arrange
        cache = LocalFsCache(tmp_path)
        source = tmp_path / "src"
        _write(source, b"hello world")
        await cache.put("encode/x/data/aa-v0", source)

        # Act
        entry = await cache.head("encode/x/data/aa-v0")

        # Assert
        assert entry == CacheEntry(key="encode/x/data/aa-v0", size=11)

    @pytest.mark.asyncio
    async def test_put_should_move_source_and_return_size(self, tmp_path):
        """Test that put removes the source and returns the committed size.

        Given:
            A LocalFsCache and a source file with known contents.
        When:
            put is awaited.
        Then:
            It should move the source into the cache (removing the
            original path) and return a CacheEntry with the expected size.
        """
        # Arrange
        cache = LocalFsCache(tmp_path)
        source = tmp_path / "src"
        _write(source, b"payload")

        # Act
        entry = await cache.put("encode/x/data/aa-v0", source)

        # Assert
        assert entry.size == 7
        assert not source.exists()

    @pytest.mark.asyncio
    async def test_get_should_stream_full_file_when_range_omitted(self, tmp_path):
        """Test that get streams the full artifact when no range is given.

        Given:
            A LocalFsCache containing a multi-chunk artifact.
        When:
            get is iterated without a byte_range argument.
        Then:
            It should yield the complete bytes.
        """
        # Arrange
        cache = LocalFsCache(tmp_path)
        source = tmp_path / "src"
        payload = b"0123456789" * 20_000
        _write(source, payload)
        await cache.put("encode/x/data/aa-v0", source)

        # Act
        collected = await _collect(cache.get("encode/x/data/aa-v0"))

        # Assert
        assert collected == payload

    @pytest.mark.asyncio
    async def test_get_should_honor_byte_range_inclusive(self, tmp_path):
        """Test that get respects an inclusive byte range.

        Given:
            A LocalFsCache containing a known artifact.
        When:
            get is iterated with byte_range=(5, 9).
        Then:
            It should yield exactly bytes 5..9 inclusive (5 bytes total).
        """
        # Arrange
        cache = LocalFsCache(tmp_path)
        source = tmp_path / "src"
        _write(source, b"0123456789ABCDEF")
        await cache.put("encode/x/data/aa-v0", source)

        # Act
        collected = await _collect(cache.get("encode/x/data/aa-v0", (5, 9)))

        # Assert
        assert collected == b"56789"

    @pytest.mark.asyncio
    async def test_get_should_honor_byte_range_across_chunk_boundary(self, tmp_path):
        """Test that get returns correct bytes for ranges crossing 64 KiB seams.

        Given:
            A LocalFsCache containing a >128 KiB artifact and a byte
            range that starts in the first 64 KiB chunk and ends in a
            later one.
        When:
            get is iterated with that byte_range.
        Then:
            The concatenated chunks should equal the corresponding slice
            of the original payload — i.e., the cache's per-chunk read
            loop must not lose, duplicate, or reorder bytes at chunk
            seams.
        """
        # Arrange
        cache = LocalFsCache(tmp_path)
        source = tmp_path / "src"
        payload = bytes(b % 256 for b in range(200_000))  # 3+ chunks
        _write(source, payload)
        await cache.put("encode/x/data/aa-v0", source)
        start, end = 60_000, 130_000  # spans chunks 0, 1, and into 2

        # Act
        collected = await _collect(
            cache.get("encode/x/data/aa-v0", (start, end))
        )

        # Assert
        assert collected == payload[start : end + 1]

    @pytest.mark.asyncio
    async def test_get_should_return_empty_iterator_when_key_absent(self, tmp_path):
        """Test that get yields nothing for an absent key.

        Given:
            An empty LocalFsCache.
        When:
            get is iterated for a missing key.
        Then:
            It should yield no chunks rather than raising, so callers can
            uniformly handle cache-miss via a ``head`` check.
        """
        # Arrange
        cache = LocalFsCache(tmp_path)

        # Act
        collected = await _collect(cache.get("encode/x/data/aa-v0"))

        # Assert
        assert collected == b""

    @pytest.mark.asyncio
    async def test_delete_should_return_true_when_key_present(self, tmp_path):
        """Test that delete reports removal of a present key.

        Given:
            A LocalFsCache containing one entry.
        When:
            delete is awaited for that key.
        Then:
            It should return True and a subsequent head should miss.
        """
        # Arrange
        cache = LocalFsCache(tmp_path)
        source = tmp_path / "src"
        _write(source, b"x")
        await cache.put("encode/x/data/aa-v0", source)

        # Act
        deleted = await cache.delete("encode/x/data/aa-v0")

        # Assert
        assert deleted is True
        assert await cache.head("encode/x/data/aa-v0") is None

    @pytest.mark.asyncio
    async def test_delete_should_return_false_when_key_absent(self, tmp_path):
        """Test that delete is idempotent on missing keys.

        Given:
            An empty LocalFsCache.
        When:
            delete is awaited for an unknown key.
        Then:
            It should return False rather than raise.
        """
        # Arrange
        cache = LocalFsCache(tmp_path)

        # Act & assert
        assert await cache.delete("encode/x/data/aa-v0") is False

    @pytest.mark.asyncio
    async def test_put_should_reject_keys_containing_traversal_segments(
        self, tmp_path
    ):
        """Test that put refuses to write outside the cache root.

        Given:
            A LocalFsCache and a malformed key with a ``..`` segment.
        When:
            put is awaited with that key.
        Then:
            It should raise ValueError rather than escape the cache root.
        """
        # Arrange
        cache = LocalFsCache(tmp_path)
        source = tmp_path / "src"
        _write(source, b"x")

        # Act & assert
        with pytest.raises(ValueError, match="cache key"):
            await cache.put("../oops", source)

    @pytest.mark.asyncio
    async def test_get_should_raise_when_byte_range_start_negative(self, tmp_path):
        """Test that a negative range start is rejected by the stream guard.

        Given:
            An empty LocalFsCache.
        When:
            ``get`` is iterated with ``byte_range=(-1, 5)``.
        Then:
            The async iterator's first advance should raise ValueError —
            the defensive contract on ``_stream_file`` requires
            ``0 <= start <= end``.
        """
        # Arrange
        cache = LocalFsCache(tmp_path)

        # Act & assert
        with pytest.raises(ValueError, match="byte_range"):
            stream = cache.get("encode/x/data/aa-v0", (-1, 5))
            await stream.__anext__()

    @pytest.mark.asyncio
    async def test_get_should_raise_when_byte_range_end_before_start(self, tmp_path):
        """Test that ``end < start`` is rejected.

        Given:
            A LocalFsCache containing a 10-byte artifact.
        When:
            ``get`` is iterated with ``byte_range=(5, 4)``.
        Then:
            The iterator should raise ValueError on first advance.
        """
        # Arrange
        cache = LocalFsCache(tmp_path)
        source = tmp_path / "src"
        _write(source, b"0123456789")
        await cache.put("encode/x/data/aa-v0", source)

        # Act & assert
        with pytest.raises(ValueError, match="byte_range"):
            stream = cache.get("encode/x/data/aa-v0", (5, 4))
            await stream.__anext__()

    @pytest.mark.asyncio
    async def test_put_should_raise_when_key_is_empty_string(self, tmp_path):
        """Test that put rejects an empty cache key.

        Given:
            A LocalFsCache and a non-empty source file.
        When:
            ``put("", source)`` is awaited.
        Then:
            It should raise ValueError matching "non-empty".
        """
        # Arrange
        cache = LocalFsCache(tmp_path)
        source = tmp_path / "src"
        _write(source, b"x")

        # Act & assert
        with pytest.raises(ValueError, match="non-empty"):
            await cache.put("", source)

    @pytest.mark.asyncio
    async def test_put_should_raise_when_key_is_only_slashes(self, tmp_path):
        """Test that put rejects a root-only key.

        Given:
            A LocalFsCache and a non-empty source file.
        When:
            ``put("/", source)`` is awaited.
        Then:
            It should raise ValueError so a single slash cannot collapse
            onto the cache root.
        """
        # Arrange
        cache = LocalFsCache(tmp_path)
        source = tmp_path / "src"
        _write(source, b"x")

        # Act & assert
        with pytest.raises(ValueError):
            await cache.put("/", source)

    @pytest.mark.asyncio
    async def test_head_should_handle_directory_at_key_path(self, tmp_path):
        """Test that ``head`` returns reasonable metadata for a key pointing at a dir.

        Given:
            A LocalFsCache rooted at tmp_path with a directory
            pre-created at ``subdir`` (no file).
        When:
            ``head("subdir")`` is awaited.
        Then:
            It should either return a ``CacheEntry`` or not raise — the
            test pins the observed behavior under the current contract,
            which is to ``stat`` the path and report its metadata.
        """
        # Arrange
        cache = LocalFsCache(tmp_path)
        (tmp_path / "subdir").mkdir(exist_ok=True)

        # Act
        entry = await cache.head("subdir")

        # Assert
        # ``Path.stat()`` succeeds on directories; the current implementation
        # returns a CacheEntry carrying the directory's reported st_size.
        # We only assert non-raising behavior + entry shape.
        assert entry is None or isinstance(entry, CacheEntry)

    @pytest.mark.asyncio
    async def test_get_should_yield_all_bytes_for_full_file_inclusive_range(
        self, tmp_path
    ):
        """Test that ``(0, len-1)`` yields exactly the full artifact.

        Given:
            A LocalFsCache containing a 10-byte artifact.
        When:
            ``get`` is iterated with ``byte_range=(0, 9)``.
        Then:
            It should yield exactly all 10 bytes — the inclusive
            boundary is honored correctly.
        """
        # Arrange
        cache = LocalFsCache(tmp_path)
        source = tmp_path / "src"
        _write(source, b"0123456789")
        await cache.put("encode/x/data/aa-v0", source)

        # Act
        collected = await _collect(cache.get("encode/x/data/aa-v0", (0, 9)))

        # Assert
        assert collected == b"0123456789"

    @settings(
        max_examples=25,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        payload=st.binary(min_size=1, max_size=200 * 1024),
        start=st.integers(min_value=0, max_value=200 * 1024),
        length=st.integers(min_value=0, max_value=200 * 1024),
    )
    @pytest.mark.asyncio
    async def test_get_property_byte_range_matches_payload_slice(
        self, tmp_path, payload, start, length
    ):
        """Test (PBT-001) that range reads agree with Python slice semantics.

        Given:
            A Hypothesis-generated payload up to 200 KiB and a
            range ``(start, end)`` clipped within bounds.
        When:
            ``get`` is iterated with that byte range.
        Then:
            The concatenated chunks should equal
            ``payload[start:end+1]`` — the cache's chunked reader must
            agree with Python slice semantics.
        """
        # Arrange
        if start >= len(payload):
            start = len(payload) - 1
        end = min(start + length, len(payload) - 1)
        # Use a per-example subdirectory because hypothesis reuses tmp_path
        # across examples within a single test method.
        sub = tmp_path / f"case-{start}-{end}-{len(payload)}"
        sub.mkdir(parents=True, exist_ok=True)
        cache = LocalFsCache(sub)
        source = sub / "src"
        source.write_bytes(payload)
        await cache.put("encode/x/data/aa-v0", source)

        # Act
        collected = await _collect(cache.get("encode/x/data/aa-v0", (start, end)))

        # Assert
        assert collected == payload[start : end + 1]
