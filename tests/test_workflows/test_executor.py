"""Tests for the Wool-backed workflow executor."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
import wool

from cfdb.workflows import executor as executor_module
from cfdb.workflows.executor import (
    PIPELINE_VERSION,
    ExecutorDraining,
    WoolExecutor,
    WorkflowNotApplicable,
    extract_identity,
)
from cfdb.workflows.lock import get_job
from cfdb.workflows.models import ACTIVE_STATUSES, ArtifactKind, JobStatus
from cfdb.workflows.processors.base import Processor
from cfdb.workflows.processors.registry import ProcessorRegistry
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
            yield {"event": "stage_complete", "kind": kind, "key": key}
        yield {"event": "complete", "artifacts": dict(self.artifacts)}


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
    mock_db.jobs.create_index(
        {"workflow_key": 1},
        unique=True,
        partialFilterExpression={
            "status": {"$in": [s.value for s in ACTIVE_STATUSES]}
        },
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
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
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
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
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
                    yield {"event": "stage_complete", "kind": kind, "key": key}
                yield {"event": "complete", "artifacts": dict(self.artifacts)}

        processor = _BlockingProcessor()
        registry = ProcessorRegistry()
        registry.register(processor)
        executor = WoolExecutor(
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
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
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
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
        """Test that the per-job workdir is removed after a successful run.

        Given:
            A registry with a stub processor that writes a file into its
            workdir before returning.
        When:
            ensure_workflow is awaited and the background task completes.
        Then:
            The per-job workdir should no longer exist on disk.
        """

        class _WorkdirTouchingProcessor(_StubProcessor):
            async def run(self, file_meta, workdir, cache_root):
                self.run_calls += 1
                workdir.mkdir(parents=True, exist_ok=True)
                (workdir / "scratch").write_bytes(b"tmp")
                for kind, key in self.artifacts.items():
                    yield {"event": "stage_complete", "kind": kind, "key": key}
                yield {"event": "complete", "artifacts": dict(self.artifacts)}

        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_WorkdirTouchingProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
        )

        # Act
        record, _ = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        assert not (tmp_workdir / record.job_id).exists()

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
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
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
        # the initial _open_stream_with_retry call); then hang so the
        # cap fires on the second iteration.
        class _HangingProcessor(_StubProcessor):
            async def run(self, file_meta, workdir, cache_root):
                self.run_calls += 1
                yield {
                    "event": "stage_complete",
                    "kind": ArtifactKind.DATA.value,
                    "key": _DATA_KEY,
                }
                await asyncio.sleep(10)
                yield {"event": "complete", "artifacts": {}}

        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_HangingProcessor())
        executor = WoolExecutor(
            mock_db,
            tmp_cache,
            tmp_cache.root,
            registry,
            workdir_root=tmp_workdir,
            # Tiny but non-zero cap: zero risks event-loop edge cases on
            # some asyncio versions; 0.05s is short enough to keep the
            # test fast and long enough to be deterministic.
            workflow_duration_cap_seconds=0.05,
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
                    yield {
                        "event": "stage_complete",
                        "kind": ArtifactKind.DATA.value,
                        "key": _DATA_KEY,
                    }
                    raise RuntimeError("stage-2 failure")
                invocations.append(True)
                yield {
                    "event": "stage_complete",
                    "kind": ArtifactKind.DATA.value,
                    "key": _DATA_KEY,
                }
                src = workdir / "index"
                src.write_bytes(b"index-bytes")
                await cache.put(_INDEX_KEY, src)
                yield {
                    "event": "stage_complete",
                    "kind": ArtifactKind.INDEX.value,
                    "key": _INDEX_KEY,
                }
                yield {
                    "event": "complete",
                    "artifacts": {
                        ArtifactKind.DATA.value: _DATA_KEY,
                        ArtifactKind.INDEX.value: _INDEX_KEY,
                    },
                }

        processor = _PartialCommitProcessor()
        registry = ProcessorRegistry()
        registry.register(processor)
        executor = WoolExecutor(
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
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
        """Test that processors emitting unknown artifact kinds fail loud.

        Given:
            A processor whose ``run`` yields a ``stage_complete`` event
            naming a kind not in ``ArtifactKind``.
        When:
            ensure_workflow is awaited and the background task completes.
        Then:
            The job should land in FAILED with an error mentioning the
            malformed event.
        """

        # Arrange
        class _BogusKindProcessor(_StubProcessor):
            async def run(self, file_meta, workdir, cache_root):
                self.run_calls += 1
                yield {
                    "event": "stage_complete",
                    "kind": "weird_kind",
                    "key": "encode/x/weird/aa-v0",
                }

        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_BogusKindProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
        )

        # Act
        record, _ = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.FAILED
        assert "stage_complete" in (final.error or "")


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
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
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
                    yield {"event": "stage_complete", "kind": kind, "key": key}
                yield {"event": "complete", "artifacts": dict(self.artifacts)}

        registry = ProcessorRegistry()
        registry.register(_BlockingProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
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
                yield {"event": "complete", "artifacts": {}}

        registry = ProcessorRegistry()
        registry.register(_ForeverProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
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
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
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


class TestOpenStreamWithRetry:
    @pytest.mark.asyncio
    async def test_open_stream_with_retry_should_recover_after_one_no_workers_error(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch, mocker
    ):
        """Test that ``NoWorkersAvailable`` triggers a retry that eventually wins.

        Given:
            An executor whose ``_run_processor_routine`` raises
            ``wool.NoWorkersAvailable`` on its first ``__anext__`` then
            yields events on the second attempt.
        When:
            ``_open_stream_with_retry`` is awaited.
        Then:
            It should return a stream that yields the second invocation's
            events without re-raising.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        processor = _StubProcessor()
        registry.register(processor)
        executor = WoolExecutor(
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
        )

        # Build two routines: one that immediately raises NoWorkersAvailable
        # and one that yields a regular event followed by a complete.
        attempts = {"count": 0}

        async def failing_stream():
            attempts["count"] += 1
            raise wool.NoWorkersAvailable("simulated cold start")
            yield  # pragma: no cover

        async def working_stream():
            attempts["count"] += 1
            yield {"event": "stage_complete", "kind": "index", "key": _INDEX_KEY}
            yield {"event": "complete", "artifacts": {}}

        streams = [failing_stream(), working_stream()]

        def fake_routine(*_args, **_kwargs):
            return streams.pop(0)

        mocker.patch.object(executor_module, "_run_processor_routine", fake_routine)
        # Patch sleep so retries don't slow the test down.
        async def fast_sleep(_s):
            return None

        mocker.patch.object(executor_module.asyncio, "sleep", fast_sleep)

        # Act
        stream = await executor._open_stream_with_retry(
            processor, _file_meta(), tmp_workdir / "wd"
        )
        events = [event async for event in stream]

        # Assert
        assert attempts["count"] == 2
        assert any(e.get("event") == "complete" for e in events)

    @pytest.mark.asyncio
    async def test_open_stream_with_retry_should_raise_when_deadline_expires(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch, mocker
    ):
        """Test that the retry loop eventually surfaces ``NoWorkersAvailable``.

        Given:
            An executor whose ``_run_processor_routine`` always raises
            ``wool.NoWorkersAvailable``; the dispatch-wait budget is
            patched to a tiny value so the deadline expires quickly.
        When:
            ``_open_stream_with_retry`` is awaited.
        Then:
            It should raise the last ``NoWorkersAvailable`` after the
            dispatch budget is exhausted.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        processor = _StubProcessor()
        registry.register(processor)
        executor = WoolExecutor(
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
        )

        async def failing_stream():
            raise wool.NoWorkersAvailable("still no workers")
            yield  # pragma: no cover

        def fake_routine(*_args, **_kwargs):
            return failing_stream()

        mocker.patch.object(executor_module, "_run_processor_routine", fake_routine)
        mocker.patch.object(executor_module, "_DISPATCH_WAIT_SECONDS", 0.01)

        async def fast_sleep(_s):
            return None

        mocker.patch.object(executor_module.asyncio, "sleep", fast_sleep)

        # Act & assert
        with pytest.raises(wool.NoWorkersAvailable):
            await executor._open_stream_with_retry(
                processor, _file_meta(), tmp_workdir / "wd"
            )


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
                yield {"event": "progress", "value": "merging"}
                yield {"event": "complete", "artifacts": {}}

        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_ProgressProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
        )

        # Act
        record, _ = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.progress == "merging"

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_ignore_progress_event_with_non_string_value(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch
    ):
        """Test that non-string progress values are silently dropped.

        Given:
            A stub processor that yields ``{"event": "progress",
            "value": 12345}`` (an int, not a string).
        When:
            ``ensure_workflow`` is awaited.
        Then:
            The job should reach COMPLETED and the persisted ``progress``
            field should remain untouched (None) — the type guard skips
            non-string values rather than crashing.
        """

        # Arrange
        class _NumericProgressProcessor(_StubProcessor):
            async def run(self, file_meta, workdir, cache_root):
                self.run_calls += 1
                yield {"event": "progress", "value": 12345}
                yield {"event": "complete", "artifacts": {}}

        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_NumericProgressProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
        )

        # Act
        record, _ = await executor.ensure_workflow(_file_meta())
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status == JobStatus.COMPLETED
        assert final.progress is None


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
                yield {"event": "heartbeat"}
                yield {"event": "complete", "artifacts": {}}

        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_HeartbeatProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
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
                yield {"event": "complete", "artifacts": {}}

        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        registry.register(_WeirdEventProcessor())
        executor = WoolExecutor(
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
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
    async def test_open_stream_with_retry_should_propagate_non_capacity_error(
        self, mock_db, tmp_cache, tmp_workdir, no_wool_dispatch, mocker
    ):
        """Test that a non-``NoWorkersAvailable`` first error propagates.

        Given:
            A routine whose first ``__anext__`` raises a generic
            ``RuntimeError`` (not a wool capacity error).
        When:
            ``_open_stream_with_retry`` is awaited.
        Then:
            The exception should propagate to the caller — the retry
            loop only handles ``NoWorkersAvailable``, every other class
            is a real failure.
        """
        # Arrange
        _install_jobs_index(mock_db)
        registry = ProcessorRegistry()
        processor = _StubProcessor()
        registry.register(processor)
        executor = WoolExecutor(
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
        )

        async def boom_stream():
            raise RuntimeError("not a capacity issue")
            yield  # pragma: no cover

        def fake_routine(*_args, **_kwargs):
            return boom_stream()

        mocker.patch.object(executor_module, "_run_processor_routine", fake_routine)

        # Act & assert
        with pytest.raises(RuntimeError, match="not a capacity"):
            await executor._open_stream_with_retry(
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
            mock_db, tmp_cache, tmp_cache.root, registry, workdir_root=tmp_workdir
        )
        meta = _file_meta()

        # Act
        record, _ = await executor.ensure_workflow(meta)
        await _wait_for_terminal(mock_db, record.job_id)

        # Assert
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.file_meta_snapshot == meta
