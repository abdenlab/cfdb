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
    probe_workflow_readiness,
    serve_workflow_artifact_or_dispatch,
    stream_cache_entry,
)
from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.executor import (
    AdmissionRejected,
    ExecutorDraining,
    WorkflowNotApplicable,
)
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


#: The key the retired scheme minted for ``_file_doc``'s identity. An
#: artifact sitting here must be unreachable through every serving path,
#: which is what makes the ``purge-legacy-cache`` sweep safe to run.
_LEGACY_CACHE_KEY = f"encode/ENCFF123/data/{FIXTURE_MD5}-v0"


def _foreign_processor_key(processor: Processor) -> str:
    """Return the key a *different* processor derives for the same file.

    Identical in every component the two processors share — file, artifact
    kind, md5, and processor version — differing only in the identity
    segment. That is precisely the artifact the pre-#109 scheme would have
    served as a cache hit.
    """
    from cfdb.workflows import keys as key_utils

    return key_utils.cache_key(
        dcc="encode",
        local_id="ENCFF123",
        artifact_kind=ArtifactKind.DATA,
        md5=FIXTURE_MD5,
        processor_id="some-other-processor",
        processor_version=processor.processor_version,
    )


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
        processor = _StubProcessor()
        registry = ProcessorRegistry()
        registry.register(processor)
        cache = LocalFsCache(tmp_path / "cache")
        src = tmp_path / "src"
        src.write_bytes(b"cached-data")
        # Seed through the processor's own derivation rather than
        # restating the formula: this test is about the router probing
        # the key the processor writes under, so re-deriving it here
        # would pass even if the two stopped agreeing.
        key = processor.cache_key_for(_file_doc(), ArtifactKind.DATA)
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
    async def test_should_dispatch_when_only_another_processors_artifact_is_cached(
        self, mocker, tmp_path
    ):
        """Test that one processor never serves another's artifact.

        Given:
            A cache holding an artifact for the same file, artifact kind,
            md5, and processor version, but written under a *different*
            processor's identity — and a GET.
        When:
            The helper is awaited.
        Then:
            It should treat the cache as a miss and dispatch a fresh
            workflow. This is the defect the issue exists to close, made
            observable where it would actually serve a wrong answer
            rather than only at key derivation.
        """
        # Arrange
        processor = _StubProcessor()
        registry = ProcessorRegistry()
        registry.register(processor)
        cache = LocalFsCache(tmp_path / "cache")
        src = tmp_path / "src"
        src.write_bytes(b"other-processors-artifact")
        await cache.put(_foreign_processor_key(processor), src)
        executor = _RecordingExecutor(result=(_make_record(), True))
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", cache)
        mocker.patch.object(api, "executor", executor)

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
        assert len(executor.calls) == 1

    @pytest.mark.asyncio
    async def test_should_raise_404_on_head_when_only_another_processors_artifact_is_cached(
        self, mocker, tmp_path
    ):
        """Test that the cross-processor miss holds on the probe path too.

        Given:
            The same cache seeded under a different processor's identity,
            and a HEAD request.
        When:
            The helper is awaited.
        Then:
            It should raise ``HTTPException(404)`` and dispatch nothing,
            so the side-effect-free path agrees that another processor's
            artifact is not this one's.
        """
        # Arrange
        processor = _StubProcessor()
        registry = ProcessorRegistry()
        registry.register(processor)
        cache = LocalFsCache(tmp_path / "cache")
        src = tmp_path / "src"
        src.write_bytes(b"other-processors-artifact")
        await cache.put(_foreign_processor_key(processor), src)
        executor = _RecordingExecutor()
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", cache)
        mocker.patch.object(api, "executor", executor)

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await serve_workflow_artifact_or_dispatch(
                _file_doc(),
                ArtifactKind.DATA,
                _Request("HEAD"),
                None,
                head_404_detail="missing",
            )
        assert exc_info.value.status_code == 404
        assert executor.calls == []

    @pytest.mark.asyncio
    async def test_should_dispatch_when_only_a_legacy_key_artifact_is_cached(
        self, mocker, tmp_path
    ):
        """Test that a retired-scheme artifact is unreachable.

        Given:
            A cache holding an artifact for this file under the retired
            four-segment key, and a GET.
        When:
            The helper is awaited.
        Then:
            It should dispatch a fresh workflow rather than serve it.
            This is what makes the purge safe: nothing reads a legacy
            key, so deleting one cannot take a servable artifact with it,
            and the deploy shipping this change starts fully cold.
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        cache = LocalFsCache(tmp_path / "cache")
        src = tmp_path / "src"
        src.write_bytes(b"stale-artifact")
        await cache.put(_LEGACY_CACHE_KEY, src)
        executor = _RecordingExecutor(result=(_make_record(), True))
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", cache)
        mocker.patch.object(api, "executor", executor)

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
        assert len(executor.calls) == 1

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
    async def test_should_raise_429_when_admission_rejected(
        self, mocker, tmp_path
    ):
        """Test that ``AdmissionRejected`` is translated to 429 with Retry-After.

        Given:
            Cache miss + GET + an executor that raises ``AdmissionRejected``
            (the active-workflow ceiling is hit).
        When:
            The helper is awaited.
        Then:
            It should raise ``HTTPException(429)`` carrying the exception's
            ``retry_after_seconds`` as the ``Retry-After`` header — so a
            flood backs off rather than falling through to direct streaming
            (which would bypass the bounded pipeline).
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", LocalFsCache(tmp_path / "cache"))
        mocker.patch.object(
            api,
            "executor",
            _RecordingExecutor(
                exc=AdmissionRejected(active=12, ceiling=12, retry_after_seconds=7)
            ),
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
        assert exc_info.value.status_code == 429
        assert exc_info.value.headers["Retry-After"] == "7"

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


class TestProbeWorkflowReadiness:
    @pytest.mark.asyncio
    async def test_probe_workflow_readiness_should_return_none_when_subsystem_unwired(
        self, mocker
    ):
        """Test that the probe bails when subsystem fields are None.

        Given:
            ``api.processor_registry``, ``api.cache``, and ``api.executor``
            are all None.
        When:
            ``probe_workflow_readiness`` is awaited.
        Then:
            It should return None so the caller decides readiness for its
            own non-workflow path.
        """
        # Arrange
        mocker.patch.object(api, "processor_registry", None)
        mocker.patch.object(api, "cache", None)
        mocker.patch.object(api, "executor", None)

        # Act
        result = await probe_workflow_readiness(_file_doc(), ArtifactKind.DATA)

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_probe_workflow_readiness_should_return_none_when_processor_does_not_need_processing(
        self, mocker, tmp_path
    ):
        """Test that a no-work processor yields None.

        Given:
            Subsystem wired; registry returns a processor whose
            ``needs_processing`` is False (a passthrough format).
        When:
            The probe is awaited.
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
        result = await probe_workflow_readiness(_file_doc(), ArtifactKind.DATA)

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_probe_workflow_readiness_should_return_none_when_artifact_kind_not_produced(
        self, mocker, tmp_path
    ):
        """Test that an unproduced artifact kind yields None.

        Given:
            A processor whose ``artifact_kinds_produced`` returns
            ``(INDEX,)`` only, probed with ``artifact_kind=DATA``.
        When:
            The probe is awaited.
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
        result = await probe_workflow_readiness(_file_doc(), ArtifactKind.DATA)

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_probe_workflow_readiness_should_return_none_when_extract_identity_raises(
        self, mocker, tmp_path
    ):
        """Test that an incomplete file_doc yields None.

        Given:
            A processor that applies and a file_doc missing ``md5`` so
            ``extract_identity`` raises ``ValueError``.
        When:
            The probe is awaited.
        Then:
            It should return None — the missing-field error is treated as
            "workflow not applicable".
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
        result = await probe_workflow_readiness(broken, ArtifactKind.DATA)

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_probe_workflow_readiness_should_return_true_on_cache_hit(
        self, mocker, tmp_path
    ):
        """Test that a cached artifact reports ready without dispatching.

        Given:
            The cache pre-populated with the derived key and an executor
            that fails if dispatched.
        When:
            The probe is awaited.
        Then:
            It should return True and ``ensure_workflow`` MUST NOT be
            called.
        """
        # Arrange
        processor = _StubProcessor()
        registry = ProcessorRegistry()
        registry.register(processor)
        cache = LocalFsCache(tmp_path / "cache")
        src = tmp_path / "src"
        src.write_bytes(b"cached-data")
        # Seeded through the processor's own derivation — see the
        # equivalent note on the dispatch-path cache-hit test.
        key = processor.cache_key_for(_file_doc(), ArtifactKind.DATA)
        await cache.put(key, src)
        executor = _RecordingExecutor()
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", cache)
        mocker.patch.object(api, "executor", executor)

        # Act
        result = await probe_workflow_readiness(_file_doc(), ArtifactKind.DATA)

        # Assert
        assert result is True
        assert executor.calls == []

    @pytest.mark.asyncio
    async def test_probe_workflow_readiness_should_return_false_for_a_foreign_key(
        self, mocker, tmp_path
    ):
        """Test that the probe agrees another processor's artifact is a miss.

        Given:
            A cache seeded only under a different processor's identity
            for this file and artifact kind.
        When:
            The probe is awaited.
        Then:
            It should return False, matching what a GET would actually
            do — the readiness probe and the dispatch path must not
            disagree across the identity seam.
        """
        # Arrange
        processor = _StubProcessor()
        registry = ProcessorRegistry()
        registry.register(processor)
        cache = LocalFsCache(tmp_path / "cache")
        src = tmp_path / "src"
        src.write_bytes(b"other-processors-artifact")
        await cache.put(_foreign_processor_key(processor), src)
        executor = _RecordingExecutor()
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", cache)
        mocker.patch.object(api, "executor", executor)

        # Act
        result = await probe_workflow_readiness(_file_doc(), ArtifactKind.DATA)

        # Assert
        assert result is False
        assert executor.calls == []

    @pytest.mark.asyncio
    async def test_probe_workflow_readiness_should_return_false_for_a_legacy_key(
        self, mocker, tmp_path
    ):
        """Test that the probe reports a retired-scheme artifact as absent.

        Given:
            A cache seeded only under the retired four-segment key for
            this file.
        When:
            The probe is awaited.
        Then:
            It should return False, so ``/status`` tells the truth about
            a cache the migration has made cold.
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        cache = LocalFsCache(tmp_path / "cache")
        src = tmp_path / "src"
        src.write_bytes(b"stale-artifact")
        await cache.put(_LEGACY_CACHE_KEY, src)
        executor = _RecordingExecutor()
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", cache)
        mocker.patch.object(api, "executor", executor)

        # Act
        result = await probe_workflow_readiness(_file_doc(), ArtifactKind.DATA)

        # Assert
        assert result is False
        assert executor.calls == []

    @pytest.mark.asyncio
    async def test_probe_workflow_readiness_should_return_false_on_cache_miss(
        self, mocker, tmp_path
    ):
        """Test that a workflow-applicable cache miss reports not-ready.

        Given:
            A processor that applies, an empty cache, and an executor that
            fails if dispatched.
        When:
            The probe is awaited.
        Then:
            It should return False (a GET would dispatch) and
            ``ensure_workflow`` MUST NOT be called.
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        executor = _RecordingExecutor()
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "cache", LocalFsCache(tmp_path / "cache"))
        mocker.patch.object(api, "executor", executor)

        # Act
        result = await probe_workflow_readiness(_file_doc(), ArtifactKind.DATA)

        # Assert
        assert result is False
        assert executor.calls == []
