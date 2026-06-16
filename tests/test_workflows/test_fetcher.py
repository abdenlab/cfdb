"""Tests for the workflow source-file fetcher."""

from __future__ import annotations

import gzip

import pytest

from cfdb.services import drs
from cfdb.workflows import fetcher
from cfdb.workflows.urlsafe import UnsafeOutboundURL

_HTTPS_URL = "https://encode-public.s3.amazonaws.com/x.sam.gz"


class TestDownloadSource:
    @pytest.mark.asyncio
    async def test_download_source_should_raise_when_access_url_missing(
        self, tmp_path
    ):
        """Test that download_source rejects file_meta without an access_url.

        Given:
            A file_meta dict with no access_url.
        When:
            download_source is awaited.
        Then:
            It should raise ValueError rather than silently produce an
            empty file or hit a generic KeyError later.
        """
        # Arrange
        dest = tmp_path / "out"

        # Act & assert
        with pytest.raises(ValueError, match="access_url"):
            await fetcher.download_source({}, dest)

    @pytest.mark.asyncio
    async def test_download_source_should_stream_https_access_url_to_dest(
        self, tmp_path, mocker
    ):
        """Test that an HTTPS access_url is streamed to dest.

        Given:
            A file_meta with an ``https://`` access_url and a stubbed
            ``drs.stream_from_url`` yielding two chunks.
        When:
            download_source is awaited.
        Then:
            The destination file should contain the concatenated chunk
            bytes, and ``drs.fetch_drs_object`` should NOT be called (the
            URL is already a direct HTTPS resource).
        """
        # Arrange
        dest = tmp_path / "nested" / "out.bin"

        async def fake_stream(url, range_header):
            assert url == "https://encode-public.s3.amazonaws.com/x.bam"
            assert range_header is None
            yield b"hello-"
            yield b"world"

        fetch_drs_object = mocker.patch.object(
            drs, "fetch_drs_object", side_effect=AssertionError(
                "HTTPS URLs must not be resolved via DRS"
            )
        )
        mocker.patch.object(drs, "stream_from_url", fake_stream)

        file_meta = {"access_url": "https://encode-public.s3.amazonaws.com/x.bam"}

        # Act
        result = await fetcher.download_source(file_meta, dest)

        # Assert
        assert result == dest
        assert dest.read_bytes() == b"hello-world"
        assert not fetch_drs_object.called

    @pytest.mark.asyncio
    async def test_download_source_should_resolve_drs_uri_before_streaming(
        self, tmp_path, mocker
    ):
        """Test that a drs:// access_url is resolved to an HTTPS URL first.

        Given:
            A file_meta with a ``drs://`` access_url, a stubbed
            ``fetch_drs_object`` returning a DRS object with an HTTPS
            access method, and a stubbed ``stream_from_url``.
        When:
            download_source is awaited.
        Then:
            ``fetch_drs_object`` should be invoked with the DRS URI and
            ``stream_from_url`` should be invoked with the resolved HTTPS
            URL — not the original DRS URI.
        """
        # Arrange
        dest = tmp_path / "out.bam"

        drs_obj = drs.DRSObject(
            id="abc",
            name="x.bam",
            access_methods=[
                drs.DRSAccessMethod(
                    type="https",
                    access_url="https://encode-public.s3.amazonaws.com/cdn/x.bam",
                )
            ],
        )

        async def fake_fetch(_uri):
            return drs_obj

        async def fake_https_url(_methods):
            return "https://encode-public.s3.amazonaws.com/cdn/x.bam"

        captured_url: dict[str, str] = {}

        async def fake_stream(url, range_header):
            captured_url["url"] = url
            yield b"drs-bytes"

        mocker.patch.object(drs, "fetch_drs_object", fake_fetch)
        mocker.patch.object(drs, "get_https_download_url", fake_https_url)
        mocker.patch.object(drs, "stream_from_url", fake_stream)

        file_meta = {"access_url": "drs://encode-public.s3.amazonaws.com/abc"}

        # Act
        result = await fetcher.download_source(file_meta, dest)

        # Assert
        assert result == dest
        assert dest.read_bytes() == b"drs-bytes"
        assert captured_url["url"] == "https://encode-public.s3.amazonaws.com/cdn/x.bam"

    @pytest.mark.asyncio
    async def test_download_source_should_cleanup_partial_file_on_stream_failure(
        self, tmp_path, mocker
    ):
        """Test that a mid-stream failure removes the .part file.

        Given:
            A ``stream_from_url`` stub that yields 3 bytes then raises.
        When:
            ``download_source`` is awaited.
        Then:
            The ``.part`` file should be removed, ``dest`` should not
            exist, and the exception should propagate to the caller.
        """
        # Arrange
        dest = tmp_path / "out.bin"

        async def fake_stream(_url, range_header=None):
            yield b"abc"
            raise RuntimeError("upstream EOF")

        mocker.patch.object(drs, "stream_from_url", fake_stream)
        file_meta = {"access_url": "https://encode-public.s3.amazonaws.com/x.bam"}

        # Act & assert
        with pytest.raises(RuntimeError, match="upstream EOF"):
            await fetcher.download_source(file_meta, dest)
        part = dest.with_suffix(dest.suffix + ".part")
        assert not part.exists()
        assert not dest.exists()

    @pytest.mark.asyncio
    async def test_download_source_should_atomically_rename_on_successful_completion(
        self, tmp_path, mocker
    ):
        """Test that the .part file is atomically promoted to dest.

        Given:
            A successful 5-byte stream into a non-existent destination
            path.
        When:
            ``download_source`` is awaited.
        Then:
            The destination should contain the streamed bytes and the
            ``.part`` sibling should no longer exist.
        """
        # Arrange
        dest = tmp_path / "out.bin"

        async def fake_stream(_url, range_header=None):
            yield b"hello"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        # Act
        result = await fetcher.download_source(
            {"access_url": "https://encode-public.s3.amazonaws.com/x.bam"}, dest
        )

        # Assert
        assert result == dest
        assert dest.read_bytes() == b"hello"
        part = dest.with_suffix(dest.suffix + ".part")
        assert not part.exists()

    @pytest.mark.asyncio
    async def test_download_source_should_reject_non_allowlisted_https_url(
        self, tmp_path, mocker
    ):
        """Test that the allowlist blocks an attacker-controlled URL.

        Given:
            A file_meta with ``access_url`` pointing at an attacker host.
        When:
            ``download_source`` is awaited.
        Then:
            It should raise ``UnsafeOutboundURL`` BEFORE any stream is
            opened — the spy on ``stream_from_url`` confirms it was
            never called.
        """
        # Arrange
        dest = tmp_path / "out.bin"
        stream_calls: list = []

        async def fake_stream(url, range_header=None):
            stream_calls.append(url)
            yield b"should-never-reach"

        mocker.patch.object(drs, "stream_from_url", fake_stream)
        file_meta = {"access_url": "http://evil.example.com/x"}

        # Act & assert
        with pytest.raises(UnsafeOutboundURL):
            await fetcher.download_source(file_meta, dest)
        assert stream_calls == []

    @pytest.mark.asyncio
    async def test_download_source_should_reject_drs_resolved_to_disallowed_host(
        self, tmp_path, mocker
    ):
        """Test that a DRS resolved to an internal host is rejected.

        Given:
            A ``drs://`` URI whose resolved HTTPS URL points at an
            internal host outside the allowlist.
        When:
            ``download_source`` is awaited.
        Then:
            It should raise ``UnsafeOutboundURL`` on the resolved URL
            (after DRS resolution).
        """
        # Arrange
        dest = tmp_path / "out.bin"

        async def fake_fetch(_uri):
            return drs.DRSObject(
                id="x",
                name="x.bam",
                access_methods=[
                    drs.DRSAccessMethod(
                        type="https",
                        access_url="https://internal.example.com/x",
                    )
                ],
            )

        async def fake_https_url(_methods):
            return "https://internal.example.com/x"

        mocker.patch.object(drs, "fetch_drs_object", fake_fetch)
        mocker.patch.object(drs, "get_https_download_url", fake_https_url)
        file_meta = {"access_url": "drs://drs.hubmapconsortium.org/abc"}

        # Act & assert
        with pytest.raises(UnsafeOutboundURL):
            await fetcher.download_source(file_meta, dest)

    @pytest.mark.asyncio
    async def test_download_source_should_create_missing_parent_dirs(
        self, tmp_path, mocker
    ):
        """Test that download_source creates parent directories on demand.

        Given:
            A destination path whose parent directory does not exist.
        When:
            download_source is awaited.
        Then:
            The directory tree should be created and the file written
            without error.
        """
        # Arrange
        dest = tmp_path / "a" / "b" / "c" / "out.bin"

        async def fake_stream(url, range_header):
            yield b"x"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        # Act
        await fetcher.download_source(
            {"access_url": "https://encode-public.s3.amazonaws.com/y"}, dest
        )

        # Assert
        assert dest.exists()
        assert dest.read_bytes() == b"x"


class TestPeekDecompressedPrefix:
    @pytest.mark.asyncio
    async def test_peek_should_gunzip_only_the_leading_bytes(self, mocker):
        """Test that a gzip source is decompressed and Range-bounded.

        Given:
            A gzip-compressed SAM header streamed in two chunks and a
            small ``max_compressed_bytes`` cap.
        When:
            peek_decompressed_prefix is awaited.
        Then:
            It should return the decompressed header bytes and pass a
            ``bytes=0-`` Range header so only the file prefix is fetched.
        """
        # Arrange
        header = b"@HD\tVN:1.0\n@SQ\tSN:chr1\tLN:1000\n" + b"r1\t0\tchr1\t1\n" * 50
        gz = gzip.compress(header)
        captured = {}

        async def fake_stream(url, range_header=None):
            captured["url"] = url
            captured["range"] = range_header
            yield gz[:8]
            yield gz[8:]

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        # Act
        out = await fetcher.peek_decompressed_prefix({"access_url": _HTTPS_URL})

        # Assert
        assert out.startswith(b"@HD\tVN:1.0\n@SQ\tSN:chr1\tLN:1000")
        assert captured["url"] == _HTTPS_URL
        assert captured["range"].startswith("bytes=0-")

    @pytest.mark.asyncio
    async def test_peek_should_pass_through_uncompressed_source(self, mocker):
        """Test that a non-gzip source is returned verbatim.

        Given:
            A plain-text (non-gzip) source streamed as one chunk.
        When:
            peek_decompressed_prefix is awaited.
        Then:
            It should return the bytes unchanged rather than attempting
            to gunzip them.
        """
        # Arrange
        body = b"@HD\tVN:1.0\n@SQ\tSN:chr1\tLN:10\n"

        async def fake_stream(url, range_header=None):
            yield body

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        # Act
        out = await fetcher.peek_decompressed_prefix({"access_url": _HTTPS_URL})

        # Assert
        assert out == body

    @pytest.mark.asyncio
    async def test_peek_should_stop_after_the_compressed_byte_cap(self, mocker):
        """Test that streaming stops once the compressed cap is reached.

        Given:
            A stream that would yield far more than the cap and records
            how many chunks were consumed.
        When:
            peek_decompressed_prefix is awaited with a tiny cap.
        Then:
            It should stop reading after the first over-cap chunk rather
            than draining the whole (notional) file.
        """
        # Arrange
        chunks_read = 0

        async def fake_stream(url, range_header=None):
            nonlocal chunks_read
            for _ in range(1000):
                chunks_read += 1
                yield b"x" * 64

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        # Act
        out = await fetcher.peek_decompressed_prefix(
            {"access_url": _HTTPS_URL}, max_compressed_bytes=128
        )

        # Assert — stopped early, did not drain all 1000 chunks
        assert chunks_read < 1000
        assert len(out) <= 192

    @pytest.mark.asyncio
    async def test_peek_should_raise_when_access_url_missing(self):
        """Test that peek rejects file_meta without an access_url.

        Given:
            A file_meta dict with no access_url.
        When:
            peek_decompressed_prefix is awaited.
        Then:
            It should raise ValueError rather than hit a later opaque
            failure.
        """
        # Act & assert
        with pytest.raises(ValueError, match="access_url"):
            await fetcher.peek_decompressed_prefix({})
