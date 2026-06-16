"""Tests for the shared cache-streaming helper used by /data and /index."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastapi import HTTPException
from fastapi.responses import JSONResponse, Response
from starlette.responses import StreamingResponse

from cfdb import api
from cfdb.api.routers.cache_stream import (
    serve_workflow_artifact_or_dispatch,
    stream_cache_entry,
)
from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.executor import ExecutorDraining, WorkflowNotApplicable
from cfdb.workflows.models import ArtifactKind, JobRecord, JobStatus
from cfdb.workflows.processors.base import Processor
from cfdb.workflows.processors.registry import ProcessorRegistry
from tests.test_workflows import FIXTURE_MD5, utcnow_aware


class _Request:
    def __init__(self, method: str = "GET") -> None:
        self.method = method


async def _collect(stream) -> bytes:
    body = bytearray()
    async for chunk in stream:
        body.extend(chunk)
    return bytes(body)


@pytest.fixture()
def cached(tmp_path) -> tuple[LocalFsCache, str, int]:
    """Seed a LocalFsCache with a known 100-byte artifact."""
    cache = LocalFsCache(tmp_path / "cache")
    source = tmp_path / "src"
    payload = b"0123456789" * 10
    source.write_bytes(payload)
    asyncio.run(cache.put("dcc/x/data/aa-v0", source))
    return cache, "dcc/x/data/aa-v0", len(payload)


class TestStreamCacheEntry:
    @pytest.mark.asyncio
    async def test_stream_cache_entry_should_return_200_and_full_body_on_get(
        self, cached
    ):
        """Test that a GET without Range streams the full cached artifact.

        Given:
            A cache seeded with a 100-byte artifact and a GET request
            with no Range header.
        When:
            stream_cache_entry is called.
        Then:
            It should return a StreamingResponse whose status is 200
            and whose body matches the cached bytes exactly.
        """
        # Arrange
        cache, key, size = cached

        # Act
        resp = stream_cache_entry(cache, key, size, _Request(), None)

        # Assert
        assert isinstance(resp, StreamingResponse)
        assert resp.status_code == 200
        body = await _collect(resp.body_iterator)
        assert body == b"0123456789" * 10
        assert resp.headers["content-length"] == str(size)

    @pytest.mark.asyncio
    async def test_stream_cache_entry_should_return_200_head_with_no_body(
        self, cached
    ):
        """Test that a HEAD request returns headers without streaming bytes.

        Given:
            A cache seeded with a known artifact and a HEAD request.
        When:
            stream_cache_entry is called.
        Then:
            It should return a bare Response (not StreamingResponse)
            with status 200 and the size in Content-Length.
        """
        # Arrange
        cache, key, size = cached

        # Act
        resp = stream_cache_entry(cache, key, size, _Request("HEAD"), None)

        # Assert
        assert isinstance(resp, Response)
        assert not isinstance(resp, StreamingResponse)
        assert resp.status_code == 200
        assert resp.headers["content-length"] == str(size)

    @pytest.mark.asyncio
    async def test_stream_cache_entry_should_return_206_and_partial_body_on_range(
        self, cached
    ):
        """Test that a valid Range GET returns 206 with exactly the slice.

        Given:
            A 100-byte cached artifact and a Range: bytes=10-19 header.
        When:
            stream_cache_entry is called.
        Then:
            The response should be a 206 StreamingResponse with
            Content-Length=10, Content-Range set, and body containing
            exactly the 10 bytes in the requested window.
        """
        # Arrange
        cache, key, size = cached

        # Act
        resp = stream_cache_entry(cache, key, size, _Request(), "bytes=10-19")

        # Assert
        assert isinstance(resp, StreamingResponse)
        assert resp.status_code == 206
        assert resp.headers["content-range"] == f"bytes 10-19/{size}"
        assert resp.headers["content-length"] == "10"
        body = await _collect(resp.body_iterator)
        assert body == b"0123456789"

    def test_stream_cache_entry_should_raise_416_on_out_of_bounds_range(
        self, cached
    ):
        """Test that an out-of-bounds Range produces 416 with Content-Range.

        Given:
            A Range that starts beyond the end of the cached artifact.
        When:
            stream_cache_entry is called.
        Then:
            It should raise HTTPException(416) carrying the file-size
            hint in its Content-Range header so the client can retry
            with a valid window.
        """
        # Arrange
        cache, key, size = cached

        # Act & assert — start+end both past file_size triggers
        # RangeNotSatisfiableError in parse_range_header, which maps to 416.
        with pytest.raises(HTTPException) as exc_info:
            stream_cache_entry(cache, key, size, _Request(), "bytes=10000-20000")
        assert exc_info.value.status_code == 416
        assert f"bytes */{size}" in exc_info.value.headers["Content-Range"]

    def test_stream_cache_entry_should_raise_400_on_malformed_range(
        self, cached
    ):
        """Test that a malformed Range header yields HTTP 400.

        Given:
            A syntactically invalid Range header.
        When:
            stream_cache_entry is called.
        Then:
            It should raise HTTPException(400).
        """
        # Arrange
        cache, key, size = cached

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            stream_cache_entry(cache, key, size, _Request(), "garbage=oops")
        assert exc_info.value.status_code == 400


class _StubProcessor(Processor):
    """Minimal processor stub for cache-stream tests."""

    processor_version = 0
    supported_formats = frozenset({"BAM"})
    artifact_kinds = (ArtifactKind.DATA, ArtifactKind.INDEX)

    def __init__(
        self,
        *,
        needs: bool = True,
        produced: tuple[ArtifactKind, ...] = (ArtifactKind.DATA, ArtifactKind.INDEX),
    ) -> None:
        self._needs = needs
        self._produced = produced

    def needs_processing(self, file_meta):
        return self._needs

    def artifact_kinds_produced(self, file_meta=None):
        return self._produced

    async def run(self, file_meta, workdir, cache_root):
        yield {"event": "complete", "artifacts": {}}


def _file_doc(**overrides) -> dict[str, Any]:
    """Return a minimal file_doc accepted by extract_identity."""
    doc = {
        "dcc": {"dcc_abbreviation": "ENCODE"},
        "local_id": "ENCFF123",
        "md5": FIXTURE_MD5,
        "file_format": {"name": "BAM"},
    }
    doc.update(overrides)
    return doc


class _RecordingExecutor:
    """Executor double whose ``ensure_workflow`` returns a configured value."""

    def __init__(self, result=None, exc=None):
        self._result = result
        self._exc = exc
        self.calls: list[dict] = []

    async def ensure_workflow(self, file_doc):
        self.calls.append(file_doc)
        if self._exc is not None:
            raise self._exc
        return self._result


def _make_record() -> JobRecord:
    now = utcnow_aware()
    return JobRecord(
        job_id="job-1",
        workflow_key=f"encode/ENCFF123/{FIXTURE_MD5}/v1",
        status=JobStatus.PENDING,
        dcc="encode",
        local_id="ENCFF123",
        md5=FIXTURE_MD5,
        pipeline_version=1,
        submitted_at=now,
        updated_at=now,
    )


class TestServeWorkflowArtifactOrDispatch:
    @pytest.mark.asyncio
    async def test_should_return_none_when_subsystem_unwired(self, mocker):
        """Test (CS-001) that the helper bails when subsystem fields are None.

        Given:
            ``api.processor_registry``, ``api.cache``, and ``api.executor``
            are all None.
        When:
            ``serve_workflow_artifact_or_dispatch`` is awaited.
        Then:
            It should return None so the caller falls through to its
            direct-streaming path.
        """
        # Arrange
        mocker.patch.object(api, "processor_registry", None)
        mocker.patch.object(api, "cache", None)
        mocker.patch.object(api, "executor", None)

        # Act
        result = await serve_workflow_artifact_or_dispatch(
            _file_doc(),
            ArtifactKind.INDEX,
            _Request(),
            None,
            head_404_detail="missing",
        )

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_should_return_none_when_processor_does_not_need_processing(
        self, mocker, tmp_path
    ):
        """Test (CS-002) that processors with no work return None.

        Given:
            Subsystem wired; registry returns a processor whose
            ``needs_processing`` is False.
        When:
            Helper is awaited.
        Then:
            It should return None.
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_StubProcessor(needs=False))
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", LocalFsCache(tmp_path / "cache"))
        mocker.patch.object(api, "executor", _RecordingExecutor())

        # Act
        result = await serve_workflow_artifact_or_dispatch(
            _file_doc(),
            ArtifactKind.DATA,
            _Request(),
            None,
            head_404_detail="missing",
        )

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_should_return_none_when_artifact_kind_not_produced(
        self, mocker, tmp_path
    ):
        """Test (CS-003) that artifact_kind absence returns None.

        Given:
            Processor whose ``artifact_kinds_produced`` returns
            ``(INDEX,)`` only, with ``artifact_kind=DATA``.
        When:
            Helper is awaited.
        Then:
            It should return None.
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_StubProcessor(produced=(ArtifactKind.INDEX,)))
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", LocalFsCache(tmp_path / "cache"))
        mocker.patch.object(api, "executor", _RecordingExecutor())

        # Act
        result = await serve_workflow_artifact_or_dispatch(
            _file_doc(),
            ArtifactKind.DATA,
            _Request(),
            None,
            head_404_detail="missing",
        )

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_should_return_none_when_extract_identity_raises(
        self, mocker, tmp_path
    ):
        """Test (CS-004) that incomplete file_doc → None fall-through.

        Given:
            A processor that applies and a file_doc missing ``md5``.
        When:
            Helper is awaited.
        Then:
            It should return None — the missing-field ValueError is
            treated as "workflow not applicable".
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", LocalFsCache(tmp_path / "cache"))
        mocker.patch.object(api, "executor", _RecordingExecutor())
        broken = _file_doc()
        del broken["md5"]

        # Act
        result = await serve_workflow_artifact_or_dispatch(
            broken,
            ArtifactKind.DATA,
            _Request(),
            None,
            head_404_detail="missing",
        )

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_should_stream_on_cache_hit_for_get(self, mocker, tmp_path):
        """Test (CS-005) that a cache hit returns a 200 StreamingResponse.

        Given:
            Cache pre-populated with the derived key and a GET request.
        When:
            Helper is awaited.
        Then:
            It should return a 200 ``StreamingResponse`` whose body is
            the cached bytes.
        """
        # Arrange
        from cfdb.workflows import keys as key_utils

        processor = _StubProcessor()
        registry = ProcessorRegistry()
        registry.register(processor)
        cache = LocalFsCache(tmp_path / "cache")
        src = tmp_path / "src"
        src.write_bytes(b"cached-data")
        key = key_utils.cache_key(
            dcc="encode",
            local_id="ENCFF123",
            artifact_kind=ArtifactKind.DATA,
            md5=FIXTURE_MD5,
            processor_version=processor.processor_version,
        )
        await cache.put(key, src)
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", cache)
        mocker.patch.object(api, "executor", _RecordingExecutor())

        # Act
        resp = await serve_workflow_artifact_or_dispatch(
            _file_doc(),
            ArtifactKind.DATA,
            _Request(),
            None,
            head_404_detail="missing",
        )

        # Assert
        assert isinstance(resp, StreamingResponse)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_should_raise_404_on_head_cache_miss(self, mocker, tmp_path):
        """Test (CS-006) that HEAD on a cache miss raises 404 with no dispatch.

        Given:
            Cache miss + HEAD request.
        When:
            Helper is awaited.
        Then:
            It should raise ``HTTPException(404)`` with the supplied
            ``head_404_detail``.
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", LocalFsCache(tmp_path / "cache"))
        executor = _RecordingExecutor()
        mocker.patch.object(api, "executor", executor)

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await serve_workflow_artifact_or_dispatch(
                _file_doc(),
                ArtifactKind.DATA,
                _Request("HEAD"),
                None,
                head_404_detail="head-detail-marker",
            )
        assert exc_info.value.status_code == 404
        assert "head-detail-marker" in (exc_info.value.detail or "")
        assert executor.calls == []

    @pytest.mark.asyncio
    async def test_should_raise_503_when_executor_is_draining(
        self, mocker, tmp_path
    ):
        """Test (CS-007) that ``ExecutorDraining`` is translated to 503.

        Given:
            Cache miss + GET + executor that raises ``ExecutorDraining``.
        When:
            Helper is awaited.
        Then:
            It should raise ``HTTPException(503)`` with a ``Retry-After``
            header.
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", LocalFsCache(tmp_path / "cache"))
        mocker.patch.object(
            api,
            "executor",
            _RecordingExecutor(exc=ExecutorDraining("draining")),
        )

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await serve_workflow_artifact_or_dispatch(
                _file_doc(),
                ArtifactKind.DATA,
                _Request(),
                None,
                head_404_detail="missing",
            )
        assert exc_info.value.status_code == 503
        assert "Retry-After" in exc_info.value.headers

    @pytest.mark.asyncio
    async def test_should_return_none_on_workflow_not_applicable_race(
        self, mocker, tmp_path
    ):
        """Test (CS-008) that a ``WorkflowNotApplicable`` race falls through.

        Given:
            Cache miss + GET + executor that raises
            ``WorkflowNotApplicable``.
        When:
            Helper is awaited.
        Then:
            It should return None — the caller's direct path handles
            the race.
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", LocalFsCache(tmp_path / "cache"))
        mocker.patch.object(
            api,
            "executor",
            _RecordingExecutor(exc=WorkflowNotApplicable("race")),
        )

        # Act
        result = await serve_workflow_artifact_or_dispatch(
            _file_doc(),
            ArtifactKind.DATA,
            _Request(),
            None,
            head_404_detail="missing",
        )

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_should_return_202_with_location_on_successful_dispatch(
        self, mocker, tmp_path
    ):
        """Test (CS-009) that a successful claim returns 202 with Location.

        Given:
            Cache miss + GET + executor returning a fresh JobRecord.
        When:
            Helper is awaited.
        Then:
            It should return a 202 ``JSONResponse`` with ``Location:
            /jobs/{job_id}`` and ``Retry-After: 5``.
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", LocalFsCache(tmp_path / "cache"))
        record = _make_record()
        mocker.patch.object(
            api,
            "executor",
            _RecordingExecutor(result=(record, True)),
        )

        # Act
        resp = await serve_workflow_artifact_or_dispatch(
            _file_doc(),
            ArtifactKind.DATA,
            _Request(),
            None,
            head_404_detail="missing",
        )

        # Assert
        assert isinstance(resp, JSONResponse)
        assert resp.status_code == 202
        assert resp.headers["location"] == f"/jobs/{record.job_id}"
        assert resp.headers["retry-after"] == "5"
