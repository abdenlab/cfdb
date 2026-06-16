"""Tests for the moto-backed S3Cache implementation."""

from __future__ import annotations

from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from cfdb.workflows.cache import CacheEntry, S3Cache


_BUCKET = "cfdb-test-cache"


def _write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


async def _collect(stream) -> bytes:
    chunks: list[bytes] = []
    async for chunk in stream:
        chunks.append(chunk)
    return b"".join(chunks)


@pytest.fixture()
def s3_client():
    """Return a moto-backed boto3 S3 client with one created bucket."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=_BUCKET)
        yield client


@pytest.fixture()
def cache(s3_client, tmp_path) -> S3Cache:
    """Return an S3Cache wired up to the moto-backed client."""
    return S3Cache(bucket=_BUCKET, client=s3_client)


class TestS3Cache:
    def test___init___with_empty_bucket_name(self, s3_client, tmp_path):
        """Test that S3Cache rejects an empty bucket name.

        Given:
            A moto-backed S3 client and an empty bucket name.
        When:
            S3Cache is constructed.
        Then:
            It should raise ValueError so misconfigurations fail fast.
        """
        # Act & assert
        with pytest.raises(ValueError, match="bucket"):
            S3Cache(bucket="", client=s3_client)

    @pytest.mark.asyncio
    async def test_head_with_absent_key(self, cache):
        """Test that head reports a cache miss as None.

        Given:
            An empty S3Cache.
        When:
            head is awaited for an unknown key.
        Then:
            It should return None so the router can dispatch a workflow.
        """
        # Act
        entry = await cache.head("encode/x/data/aa-v0")

        # Assert
        assert entry is None

    @pytest.mark.asyncio
    async def test_put_then_head_with_known_payload(self, cache, tmp_path):
        """Test that put commits the artifact and head reports its size.

        Given:
            An S3Cache and a source file with known contents.
        When:
            put commits the artifact and head is then awaited.
        Then:
            head should return a CacheEntry carrying the exact byte size.
        """
        # Arrange
        source = tmp_path / "src"
        _write(source, b"hello world")

        # Act
        await cache.put("encode/x/data/aa-v0", source)
        entry = await cache.head("encode/x/data/aa-v0")

        # Assert
        assert entry == CacheEntry(key="encode/x/data/aa-v0", size=11)

    @pytest.mark.asyncio
    async def test_get_with_full_object(self, cache, tmp_path):
        """Test that get streams the full artifact without a byte range.

        Given:
            An S3Cache containing a multi-chunk artifact.
        When:
            get is iterated without a byte_range argument.
        Then:
            It should yield the complete bytes.
        """
        # Arrange
        source = tmp_path / "src"
        payload = b"0123456789" * 20_000
        _write(source, payload)
        await cache.put("encode/x/data/aa-v0", source)

        # Act
        collected = await _collect(cache.get("encode/x/data/aa-v0"))

        # Assert
        assert collected == payload

    @pytest.mark.asyncio
    async def test_get_with_inclusive_byte_range(self, cache, tmp_path):
        """Test that get forwards an inclusive byte range to S3.

        Given:
            An S3Cache containing a known artifact.
        When:
            get is iterated with byte_range=(5, 9).
        Then:
            It should yield exactly bytes 5..9 inclusive (5 bytes total).
        """
        # Arrange
        source = tmp_path / "src"
        _write(source, b"0123456789ABCDEF")
        await cache.put("encode/x/data/aa-v0", source)

        # Act
        collected = await _collect(cache.get("encode/x/data/aa-v0", (5, 9)))

        # Assert
        assert collected == b"56789"

    @pytest.mark.asyncio
    async def test_get_with_absent_key(self, cache):
        """Test that get yields nothing for an absent key.

        Given:
            An empty S3Cache.
        When:
            get is iterated for a missing key.
        Then:
            It should yield no chunks rather than raise.
        """
        # Act
        collected = await _collect(cache.get("encode/x/data/aa-v0"))

        # Assert
        assert collected == b""

    @pytest.mark.asyncio
    async def test_delete_with_present_key(self, cache, tmp_path):
        """Test that delete reports True and removes the artifact.

        Given:
            An S3Cache containing one entry.
        When:
            delete is awaited for that key.
        Then:
            It should return True and a subsequent head should miss.
        """
        # Arrange
        source = tmp_path / "src"
        _write(source, b"x")
        await cache.put("encode/x/data/aa-v0", source)

        # Act
        deleted = await cache.delete("encode/x/data/aa-v0")

        # Assert
        assert deleted is True
        assert await cache.head("encode/x/data/aa-v0") is None

    @pytest.mark.asyncio
    async def test_delete_with_absent_key(self, cache):
        """Test that delete on a missing key is idempotent.

        Given:
            An empty S3Cache.
        When:
            delete is awaited for an unknown key.
        Then:
            It should return False rather than raise.
        """
        # Act & assert
        assert await cache.delete("encode/x/data/aa-v0") is False

    @pytest.mark.asyncio
    async def test_put_with_traversal_segment_key(self, cache, tmp_path):
        """Test that put refuses keys containing path-traversal segments.

        Given:
            An S3Cache and a key with a ``..`` segment.
        When:
            put is awaited with that key.
        Then:
            It should raise ValueError rather than write under a parent prefix.
        """
        # Arrange
        source = tmp_path / "src"
        _write(source, b"x")

        # Act & assert
        with pytest.raises(ValueError):
            await cache.put("../oops", source)

    @pytest.mark.asyncio
    async def test_put_then_head_with_configured_prefix(self, s3_client, tmp_path):
        """Test that the configured prefix is applied to every operation.

        Given:
            An S3Cache configured with a non-empty prefix.
        When:
            put writes a key and head reads it back.
        Then:
            The object should land under ``<prefix>/<key>`` and head should
            report its size correctly.
        """
        # Arrange
        cache = S3Cache(bucket=_BUCKET, prefix="env/dev", client=s3_client)
        source = tmp_path / "src"
        _write(source, b"abc")

        # Act
        await cache.put("encode/x/data/aa-v0", source)
        entry = await cache.head("encode/x/data/aa-v0")

        # Assert — moto stores under the prefixed key, not the raw key
        assert entry == CacheEntry(key="encode/x/data/aa-v0", size=3)
        listing = s3_client.list_objects_v2(Bucket=_BUCKET).get("Contents", [])
        assert {item["Key"] for item in listing} == {"env/dev/encode/x/data/aa-v0"}
