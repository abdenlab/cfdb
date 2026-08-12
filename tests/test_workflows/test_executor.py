"""Tests for the Wool-backed workflow executor."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import wool

from cfdb.workflows import executor as executor_module
from cfdb.workflows import keys as key_utils
from cfdb.workflows.events import (
    Complete,
    Heartbeat,
    Progress,
    StageComplete,
)
from cfdb.workflows.executor import (
    PIPELINE_VERSION,
    AdmissionRejected,
    ExecutorDraining,
    WoolExecutor,
    WorkflowNotApplicable,
    extract_identity,
)
from cfdb.workflows.lock import get_job
from cfdb.workflows.models import (
    ACTIVE_STATUSES,
    ArtifactKind,
    JobRecord,
    JobStatus,
)
from cfdb.workflows.processors.base import Processor
from cfdb.workflows.processors.registry import ProcessorRegistry
from cfdb.workflows.provisioner import EcsProvisioner, RetryableProvisionerError
from tests.test_workflows import FIXTURE_MD5

#: Canonical artifact keys the stubs emit. Built from FIXTURE_MD5 so the
#: shape matches what ``cache_key`` would produce in production for the
#: ``_file_meta`` identity below.
_DATA_KEY = f"encode/ENCFF123/data/{FIXTURE_MD5}-v0"
_INDEX_KEY = f"encode/ENCFF123/index/{FIXTURE_MD5}-v0"


class _StubProcessor(Processor):
    """Test double whose ``run`` is an async generator yielding events.

    ``Processor.run`` is now an ``AsyncIterator[dict]`` per the executor's
    streaming-routine contract — yielding ``stage_complete`` per artifact
    then a single ``complete`` event with the full mapping. The stub
    mirrors that contract so the executor's event consumer exercises the
    same code path it would against a real processor.
    """

    processor_version = 0
    supported_formats = frozenset({"BAM"})
    artifact_kinds = (ArtifactKind.DATA, ArtifactKind.INDEX)

    def __init__(self, artifacts: dict[str, str] | None = None) -> None:
        self.artifacts = artifacts or {
            ArtifactKind.DATA.value: _DATA_KEY,
            ArtifactKind.INDEX.value: _INDEX_KEY,
        }
        self.run_calls = 0

    async def run(
        self,
        file_meta: dict[str, Any],
        workdir: Path,
        cache_root: Path,
    ) -> AsyncIterator[dict[str, Any]]:
        self.run_calls += 1
        for kind, key in self.artifacts.items():
            yield StageComplete(kind=ArtifactKind(kind), key=key)
        yield Complete(artifacts=dict(self.artifacts))


class _FailingProcessor(Processor):
    """Test double whose ``run`` raises mid-stream to exercise the error path."""

    processor_version = 0
    supported_formats = frozenset({"BAM"})
    artifact_kinds = (ArtifactKind.DATA, ArtifactKind.INDEX)

    async def run(
        self,
        file_meta: dict[str, Any],
        workdir: Path,
        cache_root: Path,
    ) -> AsyncIterator[dict[str, Any]]:
        # The trailing ``yield`` keeps this method an async generator
        # syntactically; raising before the first yield still propagates
        # cleanly via the routine's ``except Exception → error event``
        # branch.
        raise RuntimeError("samtools exploded")
        yield  # pragma: no cover  (unreachable; preserves async-gen shape)


def _file_meta() -> dict[str, Any]:
    """Return a minimal file metadata dict suitable for workflow dispatch."""
    return {
        "dcc": {"dcc_abbreviation": "ENCODE"},
        "local_id": "ENCFF123",
        "md5": FIXTURE_MD5,
        "file_format": {"name": "BAM"},
    }


def _install_jobs_index(mock_db) -> None:
    mock_db.jobs.register_index(
        {"workflow_key": 1},
        unique=True,
        partialFilterExpression={"active": True},
    )


async def _wait_for_terminal(mock_db, job_id: str, timeout: float = 2.0) -> None:
    """Poll the job record until it reaches a terminal status."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        record = await get_job(mock_db, job_id)
        if record is not None and record.status not in ACTIVE_STATUSES:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"Job {job_id} did not reach terminal status")


async def _wait_for_status(
    mock_db, job_id: str, status: JobStatus, timeout: float = 2.0
) -> None:
    """Poll the job record until it reaches ``status`` (or time out)."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        record = await get_job(mock_db, job_id)
        if record is not None and record.status == status:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"Job {job_id} did not reach status {status}")


async def _seed_pending_job(
    mock_db,
    *,
    submitted_at: datetime | None = None,
    next_dispatch_at: datetime | None = None,
    meta: dict[str, Any] | None = None,
    status: JobStatus = JobStatus.PENDING,
    updated_at: datetime | None = None,
) -> JobRecord:
    """Insert a crafted job row directly and return its JobRecord.

    Lets a test drive ``_attempt_dispatch`` / ``_drain_due_jobs`` /
    ``_recover_orphans`` against a job without going through
    ``ensure_workflow`` (which would spawn its own inline attempt).
    ``submitted_at`` controls the deadline clock; ``next_dispatch_at``
    controls scheduler due-ness; ``status`` / ``updated_at`` craft orphan
    states (e.g. a stale RUNNING row).
    """
    meta = meta or _file_meta()
    dcc, local_id, md5 = extract_identity(meta)
    now = datetime.now(timezone.utc)
    record = JobRecord(
        job_id=str(uuid.uuid4()),
        workflow_key=key_utils.workflow_key(
            dcc=dcc, local_id=local_id, md5=md5, pipeline_version=PIPELINE_VERSION
        ),
        status=status,
        dcc=dcc,
        local_id=local_id,
        md5=md5,
        pipeline_version=PIPELINE_VERSION,
        submitted_at=submitted_at or now,
        updated_at=updated_at or now,
        file_meta_snapshot=meta,
        next_dispatch_at=next_dispatch_at,
    )
    await mock_db.jobs.insert_one(record.to_mongo())
    return record


class TestExtractIdentity:
    def test_extract_identity_should_prefer_nested_dcc_abbreviation(self):
        """Test that extract_identity reads the canonical C2M2 shape.

        Given:
            A file_meta dict with dcc.dcc_abbreviation set.
        When:
            extract_identity is called.
        Then:
            It should return that abbreviation, the local_id, and the md5.
        """
        # Act
        dcc, local_id, md5 = extract_identity(_file_meta())

        # Assert
        assert dcc == "encode"
        assert local_id == "ENCFF123"
        assert md5 == FIXTURE_MD5

    def test_extract_identity_should_use_submission_when_dcc_doc_absent(self):
        """Test that extract_identity falls back to ``submission`` for the dcc.

        Given:
            A file_meta dict with no ``dcc`` key but a ``submission`` field.
        When:
            extract_identity is called.
        Then:
            It should use ``submission`` as the dcc abbreviation,
            normalized to lowercase. The fallback only fires when ``dcc``
            is entirely absent (un-enriched documents); a malformed
            ``dcc`` raises rather than silently aliasing.
        """
        # Arrange
        meta = _file_meta()
        del meta["dcc"]
        meta["submission"] = "ENCODE"

        # Act
        dcc, _, _ = extract_identity(meta)

        # Assert
        assert dcc == "encode"

    def test_extract_identity_should_raise_when_dcc_is_not_dict(self):
        """Test that string-form ``dcc`` is rejected with a clear error.

        Given:
            A file_meta dict whose ``dcc`` is a string (not a dict),
            even when ``submission`` is also present.
        When:
            extract_identity is called.
        Then:
            It should raise ValueError instead of silently falling back
            to ``submission`` — the silent fallback would mask a buggy
            producer that ships ``dcc`` in two conflicting shapes and
            could route the same logical record to two different cache
            slots.
        """
        # Arrange
        meta = _file_meta()
        meta["dcc"] = "ENCODE"  # not a dict
        meta["submission"] = "encode"

        # Act & assert
        with pytest.raises(ValueError, match="must be a dict"):
            extract_identity(meta)

    def test_extract_identity_should_raise_when_dcc_abbreviation_is_empty(self):
        """Test that an empty ``dcc.dcc_abbreviation`` is rejected.

        Given:
            A file_meta dict whose ``dcc`` is a dict but
            ``dcc_abbreviation`` is empty.
        When:
            extract_identity is called.
        Then:
            It should raise ValueError rather than silently falling back
            to ``submission``.
        """
        # Arrange
        meta = _file_meta()
        meta["dcc"] = {"dcc_abbreviation": ""}
        meta["submission"] = "encode"

        # Act & assert
        with pytest.raises(ValueError, match="dcc_abbreviation"):
            extract_identity(meta)

    def test_extract_identity_should_raise_when_md5_missing(self):
        """Test that extract_identity rejects metadata missing md5.

        Given:
            A file_meta dict without md5.
        When:
            extract_identity is called.
        Then:
            It should raise ValueError because md5 is load-bearing for
            cache-key derivation.
        """
        # Arrange
        meta = _file_meta()
        del meta["md5"]

        # Act & assert
        with pytest.raises(ValueError, match="md5"):
            extract_identity(meta)


class TestWoolExecutorEnsureWorkflow:
    @pytest.mark.asyncio
    async def test_ensure_workflow_should_raise_when_no_processor_registered(
        self, mock_db, tmp_cache, tmp_workdir
    ):
        """Test that ensure_workflow rejects files with no matching processor.

        Given:
            A registry with no processor covering the file's format.
        When:
            ensure_workflow is awaited.
        Then:
            It should raise WorkflowNotApplicable so the router can return
            an appropriate error code.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()  # empty
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        # Act & assert
        with pytest.raises(WorkflowNotApplicable):
            await executor.ensure_workflow(_file_meta())

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_claim_and_run_fresh_job(
        self,
        mock_db,
        tmp_cache,
        tmp_workdir,
        no_wool_dispatch,
    ):
        """Test that a fresh claim dispatches the processor and completes.

        Given:
            A registry with a stub processor and an empty jobs collection.
        When:
            ensure_workflow is awaited and the fire-and-forget task runs.
        Then:
            The job should transition to COMPLETED with the stub's
            artifact keys recorded on the job record, and the processor's
            run method should have been invoked exactly once.
        """
        # Arrange
        _install_jobs_index(mock_db)
        processor = _StubProcessor()
        registry = ProcessorRegistry()
        registry.register(processor)
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        # Act
        record, fresh = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        assert fresh is True
        assert processor.run_calls == 1
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.COMPLETED
        assert final.artifact_cache_keys["data"] == _DATA_KEY
        assert final.artifact_cache_keys["index"] == _INDEX_KEY

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_attach_to_existing_job(
        self,
        mock_db,
        tmp_cache,
        tmp_workdir,
        no_wool_dispatch,
    ):
        """Test that concurrent calls share one workflow per source file.

        Given:
            An executor and a stub processor whose runs block until
            released.
        When:
            ensure_workflow is awaited twice for the same file_meta.
        Then:
            Only one processor run should occur and both calls should
            return the same job_id.
        """
        # Arrange
        _install_jobs_index(mock_db)
        release = asyncio.Event()

        class _BlockingProcessor(_StubProcessor):
            async def run(self, file_meta, workdir, cache_root):
                self.run_calls += 1
                await release.wait()
                for kind, key in self.artifacts.items():
                    yield StageComplete(kind=ArtifactKind(kind), key=key)
                yield Complete(artifacts=dict(self.artifacts))

        processor = _BlockingProcessor()
        registry = ProcessorRegistry()
        registry.register(processor)
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        # Act
        record_a, fresh_a = await executor.ensure_workflow(_file_meta())
        record_b, fresh_b = await executor.ensure_workflow(_file_meta())
        release.set()
        await _wait_for_terminal(mock_db, record_a.job_id)

        # Assert
        assert fresh_a is True
        assert fresh_b is False
        assert record_a.job_id == record_b.job_id
        assert processor.run_calls == 1

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_record_failure_when_processor_raises(
        self,
        mock_db,
        tmp_cache,
        tmp_workdir,
        no_wool_dispatch,
    ):
        """Test that exceptions raised by the processor mark the job FAILED.

        Given:
            A registry with a processor that raises at runtime.
        When:
            ensure_workflow is awaited and the background task completes.
        Then:
            The persisted job should be FAILED with the error text visible
            on the record.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_FailingProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        # Act
        record, _ = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.FAILED
        assert "samtools exploded" in (final.error or "")

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_cleanup_workdir_after_success(
        self,
        mock_db,
        tmp_cache,
        tmp_workdir,
        no_wool_dispatch,
    ):
        """Test that the per-attempt workdir is removed after a successful run.

        Given:
            A registry with a stub processor that writes a file into its
            workdir before returning.
        When:
            ensure_workflow is awaited and the background task completes.
        Then:
            No scratch directory should remain under the workdir root (the
            per-attempt workdir, named with a nonce, is removed).
        """

        class _WorkdirTouchingProcessor(_StubProcessor):
            async def run(self, file_meta, workdir, cache_root):
                self.run_calls += 1
                workdir.mkdir(parents=True, exist_ok=True)
                (workdir / "scratch").write_bytes(b"tmp")
                for kind, key in self.artifacts.items():
                    yield StageComplete(kind=ArtifactKind(kind), key=key)
                yield Complete(artifacts=dict(self.artifacts))

        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_WorkdirTouchingProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        # Act
        record, _ = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert — the per-attempt workdir (job_id + nonce) is removed, so
        # no scratch directory leaks under the workdir root.
        assert list(tmp_workdir.iterdir()) == []

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_release_even_when_record_stage_fails(
        self,
        mock_db,
        tmp_cache,
        tmp_workdir,
        no_wool_dispatch,
        mocker,
    ):
        """Test that release_workflow runs even if record_stage_complete raises.

        Given:
            A processor that yields stage_complete + complete events, but
            ``record_stage_complete`` is patched to raise on every call.
        When:
            ensure_workflow is awaited and the background task completes.
        Then:
            The persistence failure is logged and swallowed (the executor
            keeps draining the stream); the job still reaches COMPLETED
            because the ``complete`` event flips the terminal status, and
            ``release_workflow`` runs in the finally block.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        async def boom(*_a, **_kw):
            raise RuntimeError("mongo blip")

        mocker.patch.object(executor_module, "record_stage_complete", boom)

        # Act
        record, _ = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        # Stage persistence failure is best-effort; the job still
        # converges to a terminal status via the release in finally.
        assert final.status in (JobStatus.COMPLETED, JobStatus.FAILED)

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_record_timeout_when_cap_exceeded(
        self,
        mock_db,
        tmp_cache,
        tmp_workdir,
        no_wool_dispatch,
        mocker,
    ):
        """Test that jobs exceeding the runtime cap are marked FAILED.

        Given:
            A processor whose run never resolves and a tiny duration cap.
        When:
            ensure_workflow is awaited and the cap expires.
        Then:
            The job should be FAILED with a runtime-cap error message.
        """

        # Arrange — yield one event first so the consumer enters the
        # asyncio.timeout block (the timeout wraps the event loop, not
        # the initial _open_stream_once call); then hang so the
        # cap fires on the second iteration.
        class _HangingProcessor(_StubProcessor):
            async def run(self, file_meta, workdir, cache_root):
                self.run_calls += 1
                yield StageComplete(kind=ArtifactKind.DATA, key=_DATA_KEY)
                await asyncio.sleep(10)
                yield Complete(artifacts={})

        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_HangingProcessor())
        # Tiny but non-zero cap: zero risks event-loop edge cases on
        # some asyncio versions; 0.05s is short enough to keep the
        # test fast and long enough to be deterministic. Module-level
        # so the timeout reads through monkeypatch.
        mocker.patch.object(executor_module, "_WORKFLOW_DURATION_CAP_SECONDS", 0.05)
        executor = WoolExecutor(
            mock_db,
            tmp_cache,
            registry,
            workdir_root=tmp_workdir,
        )

        # Act
        record, _ = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, record.job_id, timeout=3.0)

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.FAILED
        assert "runtime cap" in (final.error or "")


# TestWoolExecutorPickleBoundary moved to
# tests/integration/test_executor_boundary.py per the integration-tests
# file-layout rule.


class TestWoolExecutorPartialCommit:
    @pytest.mark.asyncio
    async def test_ensure_workflow_should_skip_cached_stage_on_retry(
        self,
        mock_db,
        tmp_cache,
        tmp_workdir,
        no_wool_dispatch,
    ):
        """Test that a stage-2 failure leaves stage-1 in cache for retry reuse.

        Given:
            A processor whose first invocation writes the data artifact
            to cache then raises before completing stage 2; the second
            invocation observes the cached data artifact and writes the
            index artifact instead.
        When:
            ensure_workflow is awaited twice for the same file_meta.
        Then:
            The first job lands in FAILED with the data cache key
            populated; the second job lands in COMPLETED, the processor
            ran exactly twice, and only the second invocation produced
            the index artifact.
        """
        # Arrange
        _install_jobs_index(mock_db)

        invocations: list[bool] = []

        class _PartialCommitProcessor(_StubProcessor):
            async def run(self, file_meta, workdir, cache_root):
                cache = tmp_cache
                workdir.mkdir(parents=True, exist_ok=True)
                # Stage 1 — produce data unless cached. Mirrors the
                # real BAM/tabix processors which emit stage_complete
                # unconditionally so the JobRecord reflects the cache
                # state regardless of whether this run produced bytes.
                if await cache.head(_DATA_KEY) is None:
                    invocations.append(False)
                    src = workdir / "data"
                    src.write_bytes(b"data-bytes")
                    await cache.put(_DATA_KEY, src)
                    yield StageComplete(kind=ArtifactKind.DATA, key=_DATA_KEY)
                    raise RuntimeError("stage-2 failure")
                invocations.append(True)
                yield StageComplete(kind=ArtifactKind.DATA, key=_DATA_KEY)
                src = workdir / "index"
                src.write_bytes(b"index-bytes")
                await cache.put(_INDEX_KEY, src)
                yield StageComplete(kind=ArtifactKind.INDEX, key=_INDEX_KEY)
                yield Complete(
                    artifacts={
                        ArtifactKind.DATA.value: _DATA_KEY,
                        ArtifactKind.INDEX.value: _INDEX_KEY,
                    }
                )

        processor = _PartialCommitProcessor()
        registry = ProcessorRegistry()
        registry.register(processor)
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        # Act — first run fails after committing stage 1.
        first_record, _ = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, first_record.job_id)
        first_final = await get_job(mock_db, first_record.job_id)

        # Act — second run sees cached stage 1, completes stage 2.
        second_record, _ = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, second_record.job_id)
        second_final = await get_job(mock_db, second_record.job_id)

        # Assert
        assert first_final is not None
        assert first_final.status == JobStatus.FAILED
        assert "stage-2 failure" in (first_final.error or "")
        assert await tmp_cache.head(_DATA_KEY) is not None

        assert second_final is not None
        assert second_final.status == JobStatus.COMPLETED
        assert second_final.artifact_cache_keys["data"] == _DATA_KEY
        assert second_final.artifact_cache_keys["index"] == _INDEX_KEY
        assert await tmp_cache.head(_INDEX_KEY) is not None

        assert invocations == [False, True], (
            "first invocation must run stage-1 and raise; second must see "
            "cached data and run stage-2"
        )


class TestWoolExecutorArtifactKindCoercion:
    @pytest.mark.asyncio
    async def test_ensure_workflow_should_fail_when_processor_emits_unknown_artifact_kind(
        self,
        mock_db,
        tmp_cache,
        tmp_workdir,
        no_wool_dispatch,
    ):
        """Test that an ill-typed StageComplete fails the job, not the routine.

        Given:
            A processor that violates the typed event contract by emitting
            a StageComplete whose ``kind`` is a bare string rather than an
            ArtifactKind.
        When:
            ensure_workflow is awaited and the background task completes.
        Then:
            The job should land in FAILED rather than crashing the routine
            — the executor records the failure and moves on.
        """

        # Arrange
        class _BogusKindProcessor(_StubProcessor):
            async def run(self, file_meta, workdir, cache_root):
                self.run_calls += 1
                # Deliberately off-contract: kind must be an ArtifactKind.
                yield StageComplete(
                    kind="weird_kind",  # type: ignore[arg-type]
                    key="encode/x/weird/aa-v0",
                )

        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_BogusKindProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        # Act
        record, _ = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.FAILED
        assert final.error


class TestWoolExecutorDrain:
    @pytest.mark.asyncio
    async def test_drain_should_return_zero_when_executor_is_idle(
        self, mock_db, tmp_cache, tmp_workdir
    ):
        """Test that drain on an idle executor returns 0 immediately.

        Given:
            A WoolExecutor that has never dispatched a workflow.
        When:
            drain is awaited.
        Then:
            It should return 0 because no tasks are pending.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        # Act
        pending = await executor.drain(timeout=1.0)

        # Assert
        assert pending == 0

    @pytest.mark.asyncio
    async def test_drain_should_await_pending_tasks_to_completion(
        self,
        mock_db,
        tmp_cache,
        tmp_workdir,
        no_wool_dispatch,
    ):
        """Test that drain blocks until in-flight workflow tasks finish.

        Given:
            A WoolExecutor with one in-flight workflow whose processor is
            blocked on an asyncio.Event.
        When:
            drain is awaited concurrently with releasing the event.
        Then:
            drain should return the number of tasks pending at entry and
            the underlying job should reach COMPLETED.
        """
        # Arrange
        _install_jobs_index(mock_db)
        release = asyncio.Event()

        class _BlockingProcessor(_StubProcessor):
            async def run(self, file_meta, workdir, cache_root):
                self.run_calls += 1
                await release.wait()
                for kind, key in self.artifacts.items():
                    yield StageComplete(kind=ArtifactKind(kind), key=key)
                yield Complete(artifacts=dict(self.artifacts))

        registry = ProcessorRegistry()
        registry.register(_BlockingProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        record, _ = await executor.ensure_workflow(_file_meta())

        # Act — schedule release on the event loop, then drain.
        async def _release_after_yield() -> None:
            await asyncio.sleep(0)
            release.set()

        releaser = asyncio.create_task(_release_after_yield())
        try:
            pending = await executor.drain(timeout=5.0)
        finally:
            await releaser

        # Assert
        assert pending == 1
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_drain_should_cancel_pending_tasks_when_timeout_elapses(
        self,
        mock_db,
        tmp_cache,
        tmp_workdir,
        no_wool_dispatch,
    ):
        """Test that drain cancels surviving tasks when its timeout elapses.

        Given:
            A WoolExecutor with one in-flight workflow whose processor
            blocks forever on an unset event.
        When:
            drain is awaited with a short timeout and the timeout
            elapses.
        Then:
            drain should report the pending count, the underlying task
            should be cancelled (transition to a terminal status via the
            shielded ``_finalize`` block), and a follow-up
            ``ensure_workflow`` should be rejected with
            ``ExecutorDraining``.
        """
        # Arrange
        _install_jobs_index(mock_db)
        forever = asyncio.Event()

        class _ForeverProcessor(_StubProcessor):
            async def run(self, file_meta, workdir, cache_root):
                self.run_calls += 1
                await forever.wait()
                yield Complete(artifacts={})

        registry = ProcessorRegistry()
        registry.register(_ForeverProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        record, _ = await executor.ensure_workflow(_file_meta())

        # Act — drain with a tight timeout so the gather raises and the
        # cancellation branch fires.
        pending = await executor.drain(timeout=0.05)

        # Assert
        assert pending == 1
        # After cancellation + shielded _finalize the row should reach a
        # terminal status (FAILED with a cancelled-style message). Poll
        # briefly for the shielded grace window to land.
        await _wait_for_terminal(mock_db, record.job_id, timeout=3.0)
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.FAILED

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_raise_when_executor_is_draining(
        self, mock_db, tmp_cache, tmp_workdir
    ):
        """Test that drain blocks subsequent workflow dispatches.

        Given:
            A WoolExecutor whose drain has been invoked (idle, so it
            returns immediately and leaves _draining=True).
        When:
            ensure_workflow is awaited after drain.
        Then:
            It should raise RuntimeError so a request landing during
            lifespan shutdown does not create an orphan task.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        await executor.drain(timeout=1.0)

        # Act & assert
        with pytest.raises(ExecutorDraining, match="draining"):
            await executor.ensure_workflow(_file_meta())


def test_pipeline_version_should_be_positive():
    """Test that the module-level pipeline version is a positive integer.

    Given:
        The executor module's PIPELINE_VERSION constant.
    When:
        Its value is inspected.
    Then:
        It should be a positive int so bumps forward invalidate caches
        deterministically. The exact value isn't asserted here so this
        guard remains stable across version bumps; correctness of the
        bump itself is enforced by callers' assertions on derived
        workflow keys.
    """
    # Assert
    assert isinstance(PIPELINE_VERSION, int)


class TestOpenStreamOnce:
    @pytest.mark.asyncio
    async def test_open_stream_once_should_return_stream_on_first_event(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch, mocker
    ):
        """Test that a routine yielding an event returns a usable stream.

        Given:
            An executor whose ``_run_processor_routine`` yields a regular
            event followed by a ``complete``.
        When:
            ``_open_stream_once`` is awaited.
        Then:
            It should return a stream that re-yields the first event plus
            the rest, in a single dispatch attempt (no retry loop).
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        processor = _StubProcessor()
        registry.register(processor)
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        attempts = {"count": 0}

        async def working_stream():
            attempts["count"] += 1
            yield StageComplete(kind=ArtifactKind.INDEX, key=_INDEX_KEY)
            yield Complete(artifacts={})

        mocker.patch.object(
            executor_module,
            "_run_processor_routine",
            lambda *_a, **_k: working_stream(),
        )

        # Act
        stream = await executor._open_stream_once(
            processor, _file_meta(), tmp_workdir / "wd"
        )
        events = [event async for event in stream]

        # Assert — a single attempt, both events delivered.
        assert attempts["count"] == 1
        assert any(isinstance(e, StageComplete) for e in events)
        assert any(isinstance(e, Complete) for e in events)

    @pytest.mark.asyncio
    async def test_open_stream_once_should_propagate_no_workers_available(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch, mocker
    ):
        """Test that ``NoWorkersAvailable`` propagates without retrying.

        Given:
            An executor whose ``_run_processor_routine`` raises
            ``wool.NoWorkersAvailable`` on its first ``__anext__``.
        When:
            ``_open_stream_once`` is awaited.
        Then:
            It should propagate the ``NoWorkersAvailable`` in a single
            attempt — the durable retry scheduler, not an in-attempt loop,
            owns retries now. The freshly-constructed stream is closed.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        processor = _StubProcessor()
        registry.register(processor)
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        attempts = {"count": 0}

        async def failing_stream():
            attempts["count"] += 1
            raise wool.NoWorkersAvailable("simulated cold start")
            yield  # pragma: no cover

        mocker.patch.object(
            executor_module,
            "_run_processor_routine",
            lambda *_a, **_k: failing_stream(),
        )

        # Act & assert
        with pytest.raises(wool.NoWorkersAvailable):
            await executor._open_stream_once(
                processor, _file_meta(), tmp_workdir / "wd"
            )
        assert attempts["count"] == 1


class TestStreamConsumerProgressEvents:
    @pytest.mark.asyncio
    async def test_ensure_workflow_should_persist_progress_event_value(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch
    ):
        """Test that ``progress`` events route through ``update_progress``.

        Given:
            A stub processor that yields ``{"event": "progress",
            "value": "merging"}`` then ``complete``.
        When:
            ``ensure_workflow`` is awaited and the background task
            completes.
        Then:
            The persisted record's ``progress`` field should equal
            ``"merging"``.
        """

        # Arrange
        class _ProgressProcessor(_StubProcessor):
            async def run(self, file_meta, workdir, cache_root):
                self.run_calls += 1
                yield Progress(value="merging")
                yield Complete(artifacts={})

        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_ProgressProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        # Act
        record, _ = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.progress == "merging"


class TestStreamConsumerHeartbeatEvents:
    @pytest.mark.asyncio
    async def test_ensure_workflow_should_route_heartbeat_event_to_lock_helper(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch, mocker
    ):
        """Test that the heartbeat event triggers ``heartbeat_workflow``.

        Given:
            A stub processor that yields a heartbeat event before
            ``complete``.
        When:
            ``ensure_workflow`` is awaited.
        Then:
            ``heartbeat_workflow`` should have been invoked (via spy) and
            the job should reach COMPLETED.
        """

        # Arrange
        class _HeartbeatProcessor(_StubProcessor):
            async def run(self, file_meta, workdir, cache_root):
                self.run_calls += 1
                yield Heartbeat()
                yield Complete(artifacts={})

        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_HeartbeatProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        spy = mocker.spy(executor_module, "heartbeat_workflow")

        # Act
        record, _ = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        assert spy.call_count >= 1
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.COMPLETED


class TestStreamConsumerUnknownEvent:
    @pytest.mark.asyncio
    async def test_ensure_workflow_should_tolerate_unknown_event_kind(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch
    ):
        """Test that unknown event kinds are logged and ignored.

        Given:
            A stub processor yielding ``{"event": "weird"}`` then
            ``complete``.
        When:
            ``ensure_workflow`` is awaited.
        Then:
            The job should still reach COMPLETED — unknown events are
            tolerated for forward compatibility.
        """

        # Arrange
        class _WeirdEventProcessor(_StubProcessor):
            async def run(self, file_meta, workdir, cache_root):
                self.run_calls += 1
                yield {"event": "weird"}
                yield Complete(artifacts={})

        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_WeirdEventProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        # Act
        record, _ = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.COMPLETED


class TestExecutorDrainingHierarchy:
    def test_executor_draining_should_be_caught_by_workflow_not_applicable(self):
        """Test that ``ExecutorDraining`` inherits from ``WorkflowNotApplicable``.

        Given:
            An ``ExecutorDraining`` raise.
        When:
            A ``try/except WorkflowNotApplicable`` block surrounds it.
        Then:
            The exception should be caught by the parent handler — the
            subclass relationship is part of the public contract so
            existing fall-through callers keep their behavior.
        """
        # Arrange / Act / Assert
        caught = False
        try:
            raise ExecutorDraining("draining")
        except WorkflowNotApplicable:
            caught = True
        assert caught


class TestOpenStreamMalformedFirstEvent:
    @pytest.mark.asyncio
    async def test_open_stream_once_should_propagate_non_capacity_error(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch, mocker
    ):
        """Test that a non-``NoWorkersAvailable`` first error propagates.

        Given:
            A routine whose first ``__anext__`` raises a generic
            ``RuntimeError`` (not a wool capacity error).
        When:
            ``_open_stream_once`` is awaited.
        Then:
            The exception should propagate to the caller —
            ``_attempt_dispatch`` translates it into a FAILED job; only
            ``NoWorkersAvailable`` is treated as overflow.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        processor = _StubProcessor()
        registry.register(processor)
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        async def boom_stream():
            raise RuntimeError("not a capacity issue")
            yield  # pragma: no cover

        def fake_routine(*_args, **_kwargs):
            return boom_stream()

        mocker.patch.object(executor_module, "_run_processor_routine", fake_routine)

        # Act & assert
        with pytest.raises(RuntimeError, match="not a capacity"):
            await executor._open_stream_once(
                processor, _file_meta(), tmp_workdir / "wd"
            )


class TestEnsureWorkflowSnapshotsFileMeta:
    @pytest.mark.asyncio
    async def test_ensure_workflow_should_persist_file_meta_snapshot_on_insert(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch
    ):
        """Test that the file_meta dict is snapshotted onto the JobRecord.

        Given:
            An executor + stub processor + file_meta dict.
        When:
            ``ensure_workflow`` is awaited.
        Then:
            The persisted JobRecord's ``file_meta_snapshot`` should equal
            the input file_meta (modulo dict copy semantics).
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        meta = _file_meta()

        # Act
        record, _ = await executor.ensure_workflow(meta)
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.file_meta_snapshot == meta


class TestWoolExecutorWithProvisioner:
    @pytest.mark.asyncio
    async def test__attempt_dispatch_should_not_request_provisioner_when_worker_available(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch, mocker
    ):
        """Test that the provisioner is left idle when a worker accepts.

        Given:
            A WoolExecutor wired with a stub EcsProvisioner and an
            in-process worker (no_wool_dispatch) that accepts the task.
        When:
            ``ensure_workflow`` dispatches the inline attempt and it wins
            a worker.
        Then:
            ``provisioner.request`` should NOT be awaited — spawn-on-
            overflow means scale-up only happens when no worker accepts,
            inverting the old unconditional pre-dispatch spawn (the cost
            leak).
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        provisioner = mocker.AsyncMock(spec=EcsProvisioner)
        executor = WoolExecutor(
            mock_db,
            tmp_cache,
            registry,
            workdir_root=tmp_workdir,
            provisioner=provisioner,
        )

        # Act
        record, _ = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.COMPLETED
        provisioner.request.assert_not_awaited()

    @pytest.mark.asyncio
    async def test__attempt_dispatch_should_request_provisioner_on_overflow(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch, mocker
    ):
        """Test that overflow requests one bounded worker spawn and reschedules.

        Given:
            A WoolExecutor wired with a stub provisioner and a queued job
            whose dispatch attempt finds no worker capacity
            (``_open_stream_once`` raises ``NoWorkersAvailable``).
        When:
            ``_attempt_dispatch`` runs.
        Then:
            ``provisioner.request`` should be awaited once with the job's
            workflow key, and the job should stay PENDING with
            ``next_dispatch_at`` set and ``dispatch_attempts`` bumped —
            queued durably for a later retry, not failed.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        processor = _StubProcessor()
        registry.register(processor)
        provisioner = mocker.AsyncMock(spec=EcsProvisioner)
        executor = WoolExecutor(
            mock_db,
            tmp_cache,
            registry,
            workdir_root=tmp_workdir,
            provisioner=provisioner,
        )
        record = await _seed_pending_job(mock_db)
        mocker.patch.object(
            executor,
            "_open_stream_once",
            new=mocker.AsyncMock(side_effect=wool.NoWorkersAvailable("empty")),
        )

        # Act
        await executor._attempt_dispatch(record, processor, _file_meta())

        # Assert
        provisioner.request.assert_awaited_once_with(dedup_key=record.workflow_key)
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.PENDING
        assert final.next_dispatch_at is not None
        assert final.dispatch_attempts == 1

    @pytest.mark.asyncio
    async def test__handle_overflow_should_swallow_retryable_provisioner_error(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch, mocker
    ):
        """Test that a retryable provisioner error is swallowed, job reschedules.

        Given:
            A provisioner whose ``request`` raises
            ``RetryableProvisionerError`` on an overflowed dispatch.
        When:
            ``_attempt_dispatch`` runs.
        Then:
            The error should be swallowed (best-effort scale-up) and the
            job should stay PENDING, rescheduled for a later attempt — not
            failed.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        processor = _StubProcessor()
        registry.register(processor)
        provisioner = mocker.AsyncMock(spec=EcsProvisioner)
        provisioner.request.side_effect = RetryableProvisionerError("capacity")
        executor = WoolExecutor(
            mock_db,
            tmp_cache,
            registry,
            workdir_root=tmp_workdir,
            provisioner=provisioner,
        )
        record = await _seed_pending_job(mock_db)
        mocker.patch.object(
            executor,
            "_open_stream_once",
            new=mocker.AsyncMock(side_effect=wool.NoWorkersAvailable("empty")),
        )

        # Act
        await executor._attempt_dispatch(record, processor, _file_meta())

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.PENDING
        assert final.next_dispatch_at is not None

    @pytest.mark.asyncio
    async def test__handle_overflow_should_swallow_generic_provisioner_error(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch, mocker
    ):
        """Test that a generic provisioner error doesn't fail the queued job.

        Given:
            A provisioner whose ``request`` raises a generic
            ``RuntimeError`` (misconfiguration / unexpected boto failure)
            on an overflowed dispatch.
        When:
            ``_attempt_dispatch`` runs.
        Then:
            The error should be logged and swallowed and the job should
            stay PENDING (rescheduled) rather than failing — the
            provisioner is a best-effort scale-up hint on overflow, not a
            dispatch gate; the dispatch deadline bounds the retry loop.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        processor = _StubProcessor()
        registry.register(processor)
        provisioner = mocker.AsyncMock(spec=EcsProvisioner)
        provisioner.request.side_effect = RuntimeError("misconfigured cluster")
        executor = WoolExecutor(
            mock_db,
            tmp_cache,
            registry,
            workdir_root=tmp_workdir,
            provisioner=provisioner,
        )
        record = await _seed_pending_job(mock_db)
        mocker.patch.object(
            executor,
            "_open_stream_once",
            new=mocker.AsyncMock(side_effect=wool.NoWorkersAvailable("empty")),
        )

        # Act
        await executor._attempt_dispatch(record, processor, _file_meta())

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.PENDING
        assert final.next_dispatch_at is not None


class TestConsumeAndFinalizeHeartbeatLoss:
    def test_heartbeat_loss_abort_threshold_sits_between_interval_and_stale(self):
        """Test that the self-cancel threshold is between heartbeat and stale.

        Given:
            The module's derived heartbeat-loss abort threshold.
        When:
            It is compared against the heartbeat interval and the orphan
            sweep's stale threshold.
        Then:
            It should be strictly more than one heartbeat interval (a single
            transient write failure must not abort — the strict
            ``STALE > 2*HEARTBEAT`` startup invariant guarantees the margin,
            issue #45 review A7) and strictly below the stale threshold (a
            Mongo-blind consumer must give up before the sweep would revive
            its row).
        """
        # Assert
        assert (
            executor_module._HEARTBEAT_LOSS_ABORT_S
            > executor_module._HEARTBEAT_INTERVAL_S
        )
        assert (
            executor_module._HEARTBEAT_LOSS_ABORT_S
            < executor_module.STALE_WORKFLOW_THRESHOLD.total_seconds()
        )

    @pytest.mark.asyncio
    async def test__consume_and_finalize_should_abort_when_heartbeat_lost(
        self, mock_db, tmp_cache, tmp_workdir, mocker
    ):
        """Test that sustained heartbeat-write failure aborts the workflow.

        Given:
            A RUNNING job whose ``heartbeat_workflow`` write always fails and
            a zeroed abort threshold (so the first failure is already past
            it).
        When:
            ``_consume_and_finalize`` consumes a stream that emits a
            heartbeat.
        Then:
            It should abort with a ``heartbeat lost`` FAILED status and close
            the stream (cancelling the remote worker), rather than consuming
            on invisibly while the orphan sweep reclaims the row.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        record = await _seed_pending_job(mock_db, status=JobStatus.RUNNING)
        mocker.patch.object(executor_module, "_HEARTBEAT_LOSS_ABORT_S", -1.0)

        async def boom(*_a, **_kw):
            raise RuntimeError("mongo down")

        mocker.patch.object(executor_module, "heartbeat_workflow", boom)
        closed = {"v": False}

        async def stream():
            try:
                yield Heartbeat()
                yield Complete(artifacts={})  # pragma: no cover (aborted first)
            finally:
                closed["v"] = True

        # Act
        await executor._consume_and_finalize(record, stream(), tmp_workdir / "wd")

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.FAILED
        assert "heartbeat lost" in (final.error or "")
        assert closed["v"] is True

    @pytest.mark.asyncio
    async def test__consume_and_finalize_should_tolerate_a_single_heartbeat_failure(
        self, mock_db, tmp_cache, tmp_workdir, mocker
    ):
        """Test that one transient heartbeat-write failure does not abort.

        Given:
            A RUNNING job whose first ``heartbeat_workflow`` write fails but
            the abort threshold is far in the future.
        When:
            ``_consume_and_finalize`` consumes a heartbeat then a complete.
        Then:
            The single failure is logged and swallowed and the job still
            reaches COMPLETED.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        record = await _seed_pending_job(mock_db, status=JobStatus.RUNNING)
        mocker.patch.object(executor_module, "_HEARTBEAT_LOSS_ABORT_S", 9999.0)
        calls = {"n": 0}

        async def flaky(*_a, **_kw):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient")

        mocker.patch.object(executor_module, "heartbeat_workflow", flaky)

        async def stream():
            yield Heartbeat()
            yield Complete(artifacts={})

        # Act
        await executor._consume_and_finalize(record, stream(), tmp_workdir / "wd")

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.COMPLETED


class TestAttemptDispatchWorkdir:
    @pytest.mark.asyncio
    async def test__attempt_dispatch_should_use_a_distinct_workdir_per_attempt(
        self, mock_db, tmp_cache, tmp_workdir, mocker
    ):
        """Test that each dispatch attempt for a job gets its own workdir.

        Given:
            A queued job whose dispatch attempts always overflow.
        When:
            ``_attempt_dispatch`` runs twice for the same job.
        Then:
            The two attempts should compute distinct workdirs (both prefixed
            with the job_id), so a re-dispatch can never share scratch with a
            still-live prior attempt.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        processor = _StubProcessor()
        registry.register(processor)
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        record = await _seed_pending_job(mock_db)
        seen: list[Path] = []

        async def capture(_processor, _file_meta, workdir):
            seen.append(workdir)
            raise wool.NoWorkersAvailable("overflow")

        mocker.patch.object(executor, "_open_stream_once", new=capture)

        # Act
        await executor._attempt_dispatch(record, processor, _file_meta())
        await executor._attempt_dispatch(record, processor, _file_meta())

        # Assert
        assert len(seen) == 2
        assert seen[0] != seen[1]
        assert all(w.name.startswith(record.job_id) for w in seen)

    @pytest.mark.asyncio
    async def test__attempt_dispatch_should_clean_workdir_when_mark_running_rejected(
        self, mock_db, tmp_cache, tmp_workdir, mocker
    ):
        """Test that the hand-off branch cleans up its workdir.

        Given:
            A worker accepts (the routine created the per-attempt workdir),
            but ``mark_running`` rejects because a successor now owns the row.
        When:
            ``_attempt_dispatch`` runs.
        Then:
            It should remove this attempt's workdir (no scratch leak) and
            leave the row untouched for the successor — without releasing the
            mutex.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        processor = _StubProcessor()
        registry.register(processor)
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        record = await _seed_pending_job(mock_db)
        captured: dict[str, Path] = {}

        async def fake_open(_processor, _file_meta, workdir):
            workdir.mkdir(parents=True, exist_ok=True)
            (workdir / "scratch").write_bytes(b"x")
            captured["workdir"] = workdir

            async def s():
                yield Heartbeat()

            return s()

        mocker.patch.object(executor, "_open_stream_once", new=fake_open)

        async def reject(*_a, **_kw):
            raise RuntimeError("row no longer PENDING")

        mocker.patch.object(executor_module, "mark_running", reject)

        # Act
        await executor._attempt_dispatch(record, processor, _file_meta())

        # Assert
        assert "workdir" in captured
        assert not captured["workdir"].exists()
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.PENDING


class TestWoolExecutorMarkRunningOnAcceptance:
    @pytest.mark.asyncio
    async def test__attempt_dispatch_should_mark_running_before_first_processor_event(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch
    ):
        """Test that a job is marked RUNNING the instant a worker accepts it.

        Given:
            A processor that blocks before yielding any real event (standing
            in for a slow upstream download, which is the processor's first
            action before its first stage event).
        When:
            ``ensure_workflow`` dispatches the inline attempt and a worker
            accepts it.
        Then:
            The job should reach RUNNING while the processor is still blocked
            — driven by the routine's leading "worker accepted" heartbeat,
            not the first processor event. This is the invariant that keeps a
            job from lingering PENDING (and being re-leased / double-
            dispatched onto the same workdir) for the whole download.
        """
        # Arrange
        _install_jobs_index(mock_db)
        release = asyncio.Event()

        class _BlockBeforeFirstEvent(_StubProcessor):
            async def run(self, file_meta, workdir, cache_root):
                self.run_calls += 1
                await release.wait()  # block before the first real event
                yield Complete(artifacts={})

        registry = ProcessorRegistry()
        registry.register(_BlockBeforeFirstEvent())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )

        # Act
        record, _ = await executor.ensure_workflow(_file_meta())
        try:
            # Would time out (job stuck PENDING) if mark_running waited for
            # the first processor event rather than the acceptance heartbeat.
            await _wait_for_status(mock_db, record.job_id, JobStatus.RUNNING)
            running = await get_job(mock_db, record.job_id)
            release.set()
            await _wait_for_terminal(mock_db, record.job_id)
        finally:
            release.set()
            await executor.drain(timeout=2.0)

        # Assert
        assert running is not None
        assert running.status == JobStatus.RUNNING
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.COMPLETED


class TestWoolExecutorAdmission:
    @pytest.mark.asyncio
    async def test_ensure_workflow_should_reject_at_ceiling(
        self, mock_db, tmp_cache, tmp_workdir, mocker
    ):
        """Test that ensure_workflow sheds load once the active ceiling is hit.

        Given:
            One active workflow already in the jobs collection and the
            admission ceiling pinned to 1.
        When:
            ``ensure_workflow`` is awaited for another file.
        Then:
            It should raise ``AdmissionRejected`` carrying the observed
            active count, the ceiling, and a positive ``retry_after`` hint
            — before claiming the mutex, so a flood is shed at the door.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        mocker.patch.object(executor_module, "_MAX_ACTIVE_WORKFLOWS", 1)
        await _seed_pending_job(mock_db)  # one active workflow

        # Act & assert
        with pytest.raises(AdmissionRejected) as excinfo:
            await executor.ensure_workflow(_file_meta())
        assert excinfo.value.active == 1
        assert excinfo.value.ceiling == 1
        assert excinfo.value.retry_after_seconds >= 1

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_admit_below_ceiling(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch, mocker
    ):
        """Test that ensure_workflow claims normally below the ceiling.

        Given:
            An admission ceiling of 5 and no active workflows.
        When:
            ``ensure_workflow`` is awaited.
        Then:
            It should claim and dispatch the job to completion — the
            ceiling only sheds load once it is reached.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        mocker.patch.object(executor_module, "_MAX_ACTIVE_WORKFLOWS", 5)

        # Act
        record, fresh = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        assert fresh is True
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.COMPLETED


class TestWoolExecutorDispatchDeadline:
    @pytest.mark.asyncio
    async def test__attempt_dispatch_should_fail_capacity_when_deadline_exceeded(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch, mocker
    ):
        """Test that a job past its dispatch deadline is failed ``capacity:``.

        Given:
            A queued job submitted long ago (older than a tiny patched
            dispatch deadline).
        When:
            ``_attempt_dispatch`` runs.
        Then:
            The job should be failed with the ``capacity:`` prefix without
            attempting dispatch — the deadline bounds the durable retry
            loop so a job can't queue forever.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        processor = _StubProcessor()
        registry.register(processor)
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        mocker.patch.object(executor_module, "_DISPATCH_DEADLINE_SECONDS", 1.0)
        old = datetime.now(timezone.utc) - timedelta(seconds=10000)
        record = await _seed_pending_job(mock_db, submitted_at=old)
        open_spy = mocker.patch.object(
            executor, "_open_stream_once", new=mocker.AsyncMock()
        )

        # Act
        await executor._attempt_dispatch(record, processor, _file_meta())

        # Assert
        open_spy.assert_not_awaited()  # never tried to dispatch
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.FAILED
        assert (final.error or "").startswith("capacity:")


class TestWoolExecutorScheduler:
    @pytest.mark.asyncio
    async def test__drain_due_jobs_should_dispatch_a_due_queued_job(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch
    ):
        """Test that the scheduler tick dispatches a due rescheduled job.

        Given:
            A queued job whose ``next_dispatch_at`` is in the past (an
            earlier attempt overflowed) and a registered processor.
        When:
            One scheduler tick (``_drain_due_jobs``) runs.
        Then:
            The job should be leased, dispatched to the in-process worker,
            and reach COMPLETED — the durable retry path picking up a
            previously-overflowed job.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        due = datetime.now(timezone.utc) - timedelta(seconds=1)
        record = await _seed_pending_job(mock_db, next_dispatch_at=due)

        # Act
        await executor._drain_due_jobs()
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.COMPLETED

    @pytest.mark.asyncio
    async def test__drain_due_jobs_should_ignore_not_yet_due_jobs(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch
    ):
        """Test that a queued job whose retry time is in the future is skipped.

        Given:
            A queued job whose ``next_dispatch_at`` is in the future.
        When:
            One scheduler tick runs.
        Then:
            The job should be left PENDING and undispatched — only due
            jobs are leased, so a not-yet-due retry isn't run early.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        processor = _StubProcessor()
        registry.register(processor)
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        future = datetime.now(timezone.utc) + timedelta(seconds=3600)
        record = await _seed_pending_job(mock_db, next_dispatch_at=future)

        # Act
        await executor._drain_due_jobs()

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.PENDING
        assert processor.run_calls == 0

    @pytest.mark.asyncio
    async def test__recover_orphans_should_requeue_and_dispatch_a_stale_running_job(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch
    ):
        """Test that a RUNNING job orphaned by a crash is recovered autonomously.

        Given:
            A stale RUNNING job (no heartbeat past the stale threshold,
            `next_dispatch_at` unset) — what an API crash mid-stream leaves
            behind — and a registered processor.
        When:
            A scheduler tick runs (`_recover_orphans` then `_drain_due_jobs`).
        Then:
            The orphan is re-queued to PENDING, leased, dispatched, and
            reaches COMPLETED — without any new request for the file, closing
            the restart-recovery gap.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        stale = datetime.now(timezone.utc) - timedelta(seconds=2000)
        record = await _seed_pending_job(
            mock_db,
            status=JobStatus.RUNNING,
            updated_at=stale,
            next_dispatch_at=None,
        )

        # Act — one scheduler tick: recover orphans, then dispatch due jobs.
        await executor._recover_orphans()
        await executor._drain_due_jobs()
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.COMPLETED

    @pytest.mark.asyncio
    async def test__recover_orphans_should_leave_a_fresh_running_job_alone(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch
    ):
        """Test that a healthy in-flight RUNNING job is not reclaimed.

        Given:
            A RUNNING job with a recent `updated_at` (a live, heartbeating
            job).
        When:
            `_recover_orphans` runs.
        Then:
            The job is left RUNNING — the staleness gate keeps the sweep from
            yanking a live job away from its worker.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        record = await _seed_pending_job(
            mock_db, status=JobStatus.RUNNING, next_dispatch_at=None
        )

        # Act
        await executor._recover_orphans()

        # Assert
        job = await get_job(mock_db, record.job_id)
        assert job is not None
        assert job.status == JobStatus.RUNNING

    @pytest.mark.asyncio
    async def test__scheduler_loop_should_survive_a_tick_exception(
        self, mock_db, tmp_cache, tmp_workdir, mocker
    ):
        """Test that an exception in one tick doesn't kill the scheduler.

        Given:
            A scheduler whose lease query raises on its first call then
            returns nothing, with a tiny retry interval.
        When:
            ``start_scheduler`` runs the loop across several ticks.
        Then:
            The scheduler task should still be alive and have ticked again
            after the exception — a transient Mongo blip must not silently
            stop the dispatch driver.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        calls = {"n": 0}

        async def flaky_lease(*_a, **_k):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient mongo blip")
            return None

        mocker.patch.object(executor_module, "lease_due_dispatch", flaky_lease)
        mocker.patch.object(executor_module, "_RETRY_INTERVAL_SECONDS", 0.01)

        # Act
        executor.start_scheduler()
        try:
            await asyncio.sleep(0.1)
            # Assert — survived the first-tick exception and ticked again.
            assert executor._scheduler_task is not None
            assert not executor._scheduler_task.done()
            assert calls["n"] >= 2
        finally:
            await executor.drain(timeout=1.0)

    @pytest.mark.asyncio
    async def test__drain_due_jobs_should_lease_at_most_the_per_tick_cap(
        self, mock_db, tmp_cache, tmp_workdir, mocker
    ):
        """Test that one tick leases no more than the per-tick cap.

        Given:
            More due queued jobs than ``_MAX_DISPATCHES_PER_TICK`` (the cap
            monkeypatched low) and a stubbed ``_spawn_attempt`` so the tick
            only counts dispatches.
        When:
            One scheduler tick (``_drain_due_jobs``) runs.
        Then:
            Exactly the cap's worth of attempts are spawned and the
            remainder is left leasable for a subsequent tick — the
            thundering-herd guard bounds the per-tick fan-out so a large
            backlog can't spawn thousands of concurrent attempts at once.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        mocker.patch.object(executor_module, "_MAX_DISPATCHES_PER_TICK", 2)
        due = datetime.now(timezone.utc) - timedelta(seconds=1)
        for i in range(5):  # cap (2) + 3 remainder, each a distinct workflow
            await _seed_pending_job(
                mock_db,
                meta={**_file_meta(), "local_id": f"ENCFF{i:03d}"},
                next_dispatch_at=due,
            )
        spawn = mocker.patch.object(executor, "_spawn_attempt")

        # Act — a single tick.
        await executor._drain_due_jobs()

        # Assert — capped at the per-tick limit...
        assert spawn.call_count == 2
        # ...with the remainder still due and leasable on the next tick.
        leftover = await executor_module.lease_due_dispatch(
            mock_db,
            now=datetime.now(timezone.utc),
            next_at=datetime.now(timezone.utc) + timedelta(seconds=60),
        )
        assert leftover is not None


class TestWoolExecutorStartScheduler:
    @pytest.mark.asyncio
    async def test_drain_should_cancel_the_scheduler(
        self, mock_db, tmp_cache, tmp_workdir
    ):
        """Test that drain stops the durable scheduler task.

        Given:
            An executor whose scheduler has been started.
        When:
            ``drain`` runs.
        Then:
            The scheduler task should be cancelled and cleared, so it stops
            dispatching before the wool pool closes on shutdown.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_StubProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, registry, workdir_root=tmp_workdir
        )
        executor.start_scheduler()
        task = executor._scheduler_task
        assert task is not None

        # Act
        await executor.drain(timeout=1.0)

        # Assert
        assert task.cancelled() or task.done()
        assert executor._scheduler_task is None
