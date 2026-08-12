"""Integration test for the Wool pickle boundary in the executor.

Exercises ``ensure_workflow`` through a real ``wool.WorkerPool`` with
a stub processor that does no real I/O — so the cloudpickle
serialization of the processor instance and its return value is the
thing actually under test. For per-format end-to-end coverage through
Wool + real tools + real cache, see ``test_processor_e2e.py``.
"""

from __future__ import annotations

import asyncio
import functools

import pytest
import wool

from cfdb.workflows import executor as executor_module
from cfdb.workflows.backpressure import TaskCountBackpressure
from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.executor import WoolExecutor
from cfdb.workflows.loadbalancer import PriorityLoadBalancer
from cfdb.workflows.lock import get_job
from cfdb.workflows.models import ACTIVE_STATUSES, JobStatus
from cfdb.workflows.processors.registry import ProcessorRegistry

from tests.integration.conftest import _wait_for_terminal
from tests.integration.routines import StubProcessor, stub_file_meta


pytestmark = pytest.mark.integration


def _install_jobs_index(mock_db) -> None:
    mock_db.jobs.register_index(
        {"workflow_key": 1},
        unique=True,
        partialFilterExpression={"active": True},
    )


def _make_executor(
    mock_db,
    tmp_path,
    processor: StubProcessor,
) -> WoolExecutor:
    """Wire a WoolExecutor with a single StubProcessor registered.

    The duration cap is module-level; tests that need to shrink it
    monkeypatch ``executor._WORKFLOW_DURATION_CAP_SECONDS`` directly.
    """
    cache = LocalFsCache(tmp_path / "cache")
    registry = ProcessorRegistry()
    registry.register(processor)
    return WoolExecutor(
        mock_db,
        cache,
        registry,
        workdir_root=tmp_path / "jobs",
    )


class TestWoolExecutorPickleBoundary:
    @pytest.mark.asyncio
    async def test_ensure_workflow_should_complete_when_dispatched_to_worker(
        self, mock_db, tmp_path, wool_pool
    ):
        """Test that ensure_workflow works end-to-end through a real Wool pool.

        Given:
            A real ``wool.WorkerPool(spawn=1)`` and a picklable stub
            processor.
        When:
            ensure_workflow is awaited inside the pool's context.
        Then:
            The routine should dispatch to the worker, the stub's
            canned artifacts should round-trip via cloudpickle, and the
            job should land in COMPLETED.
        """
        # Arrange
        _install_jobs_index(mock_db)
        executor = _make_executor(mock_db, tmp_path, StubProcessor())

        # Act
        try:
            record, fresh = await executor.ensure_workflow(stub_file_meta())
            await _wait_for_terminal(mock_db, record.job_id)
            final = await get_job(mock_db, record.job_id)
        finally:
            await executor.drain(timeout=10.0)

        # Assert
        assert fresh is True
        assert final is not None
        assert final.status == JobStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_record_failed_when_routine_raises_across_pickle_boundary(
        self, mock_db, tmp_path, wool_pool
    ):
        """Test that a routine exception lands as FAILED with scrubbed text.

        Given:
            A real wool pool and a ``StubProcessor`` configured to
            raise ``RuntimeError("boom")`` during its run.
        When:
            ensure_workflow is awaited and the background task reaches
            terminal status.
        Then:
            The final JobRecord status is FAILED, the persisted
            ``error`` carries the underlying exception message, and any
            absolute filesystem paths have been scrubbed before
            persistence.
        """
        # Arrange
        _install_jobs_index(mock_db)
        processor = StubProcessor(raise_during_stage=RuntimeError("boom"))
        executor = _make_executor(mock_db, tmp_path, processor)

        # Act
        try:
            record, _ = await executor.ensure_workflow(stub_file_meta())
            await _wait_for_terminal(mock_db, record.job_id)
            final = await get_job(mock_db, record.job_id)
        finally:
            await executor.drain(timeout=10.0)

        # Assert
        assert final is not None
        assert final.status == JobStatus.FAILED
        assert final.error is not None
        # When the routine raises before yielding, wool propagates the
        # exception across the boundary; the executor records ``str(exc)``
        # from ``_attempt_dispatch``'s stream-open failure path.
        # The exception class name is not part of the persisted error.
        assert "boom" in final.error
        # The scrub regex strips multi-segment absolute paths from the
        # error text; a 2+ segment ``/<segment>/<segment>`` pattern
        # MUST NOT survive into the persisted error.
        import re

        assert re.search(r"/[A-Za-z][A-Za-z0-9_.-]*/[A-Za-z]", final.error) is None

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_record_failed_when_workflow_duration_cap_fires(
        self, mock_db, tmp_path, wool_pool, monkeypatch
    ):
        """Test that the runtime cap forces a clean FAILED termination.

        Given:
            A real wool pool, a ``StubProcessor`` that yields its first
            stage_complete promptly then sleeps 5s before the next event
            (so the executor's ``asyncio.timeout`` block is active while
            the routine blocks), and a ``WoolExecutor`` configured with
            a 1s ``workflow_duration_cap_seconds``.
        When:
            ensure_workflow is awaited and the background task reaches
            terminal status.
        Then:
            The job lands in FAILED, the ``error`` field mentions the
            runtime cap, the per-job workdir is cleaned up, and the
            executor's internal task sets are emptied.
        """
        # Arrange
        _install_jobs_index(mock_db)
        # ``sleep_between_yields`` runs the delay AFTER the first
        # stage_complete event; the executor's cap timer is only active
        # around the ``async for`` loop, so a pre-yield sleep does not
        # exercise the cap path. Between-yields blocking does.
        processor = StubProcessor(sleep_between_yields=5.0)
        from cfdb.workflows import executor as executor_module

        monkeypatch.setattr(executor_module, "_WORKFLOW_DURATION_CAP_SECONDS", 1)
        executor = _make_executor(mock_db, tmp_path, processor)

        # Act
        try:
            record, _ = await executor.ensure_workflow(stub_file_meta())
            await _wait_for_terminal(mock_db, record.job_id, timeout=15.0)
            final = await get_job(mock_db, record.job_id)
        finally:
            await executor.drain(timeout=10.0)

        # Assert
        assert final is not None
        assert final.status == JobStatus.FAILED
        assert final.error is not None
        assert "runtime cap" in final.error.lower()
        # Workdir is cleaned up regardless of failure mode. Assert against
        # the workdir ROOT, not ``root / job_id``: the per-attempt workdir is
        # ``root / f"{job_id}-{uuid}"`` (B1), so a bare-``job_id`` path is
        # never created and asserting its absence would be vacuously true.
        assert list(executor._workdir_root.iterdir()) == []
        # The internal bookkeeping sets are drained after the await.
        assert len(executor._pending_tasks) == 0
        assert len(executor._finalize_tasks) == 0

    @pytest.mark.asyncio
    async def test_drain_should_finalize_in_flight_workflow_when_called_mid_run(
        self, mock_db, tmp_path, wool_pool
    ):
        """Test that drain() cleanly finalizes an in-flight workflow.

        Given:
            A real wool pool, a ``StubProcessor`` that sleeps for 10s,
            and an ``ensure_workflow`` call that claims the workflow
            but cannot complete within the drain budget.
        When:
            ``executor.drain(timeout=2.0)`` is invoked mid-run.
        Then:
            drain returns 1; the JobRecord reaches a terminal FAILED
            status (cancellation surfaces as a FAILED release); both
            internal task sets are empty after the drain awaits.
        """
        # Arrange
        _install_jobs_index(mock_db)
        processor = StubProcessor(sleep_seconds=10.0)
        executor = _make_executor(mock_db, tmp_path, processor)

        # Act
        record, _ = await executor.ensure_workflow(stub_file_meta())
        # Give the routine a moment to start running before we drain.
        await asyncio.sleep(0.2)
        drained = await executor.drain(timeout=2.0)

        # Assert
        assert drained == 1
        final = await get_job(mock_db, record.job_id)
        assert final is not None
        assert final.status not in ACTIVE_STATUSES
        # Drain semantics: a workflow cancelled mid-run lands as FAILED
        # (the executor's _finalize releases with FAILED + a cancellation
        # error message).
        assert final.status == JobStatus.FAILED
        assert len(executor._pending_tasks) == 0
        assert len(executor._finalize_tasks) == 0

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_attach_second_caller_to_active_job_across_wool_boundary(
        self, mock_db, tmp_path, wool_pool
    ):
        """Test that two concurrent ensure_workflow calls share one JobRecord.

        Given:
            A real wool pool and a ``StubProcessor`` with a 1s delay so
            the workflow is reliably in-flight when the second call
            races in.
        When:
            Two ``ensure_workflow`` calls on the same file_meta are
            awaited via asyncio.gather.
        Then:
            The first returns ``fresh=True``; the second returns
            ``fresh=False`` with the same ``job_id``; exactly one
            JobRecord persists for the workflow_key; both ultimately
            land in COMPLETED after the background task drains.
        """
        # Arrange
        _install_jobs_index(mock_db)
        processor = StubProcessor(sleep_seconds=1.0)
        executor = _make_executor(mock_db, tmp_path, processor)

        # Act
        try:
            (rec_a, fresh_a), (rec_b, fresh_b) = await asyncio.gather(
                executor.ensure_workflow(stub_file_meta()),
                executor.ensure_workflow(stub_file_meta()),
            )
            await _wait_for_terminal(mock_db, rec_a.job_id, timeout=20.0)
            final = await get_job(mock_db, rec_a.job_id)
        finally:
            await executor.drain(timeout=10.0)

        # Assert
        # One side dispatched and the other attached — order is racy
        # but the invariant is that exactly one fresh claim happened.
        assert {fresh_a, fresh_b} == {True, False}
        assert rec_a.job_id == rec_b.job_id
        assert final is not None
        assert final.status == JobStatus.COMPLETED
        records_for_key = [
            d for d in mock_db.jobs.docs if d["workflow_key"] == rec_a.workflow_key
        ]
        assert len(records_for_key) == 1


class TestProductionPoolConfiguration:
    @pytest.mark.asyncio
    async def test_ensure_workflow_should_distribute_and_reschedule_across_priority_pool(
        self, mock_db, tmp_path, monkeypatch
    ):
        """Test the production pool config over the real pickle boundary.

        Given:
            A two-worker ``wool.WorkerPool`` wired exactly as production —
            ``PriorityLoadBalancer`` plus a ``TaskCountBackpressure(1)``-bound
            spawn factory — the durable retry scheduler running, and three
            distinct slow workflows that each hold their worker busy for the
            whole test.
        When:
            All three are dispatched via ``ensure_workflow`` and the system
            is allowed to settle.
        Then:
            Exactly two reach RUNNING concurrently (one per worker — proving
            the load balancer distributes across both over the cloudpickle /
            gRPC boundary), and the third is left PENDING with a
            ``next_dispatch_at``: the priority balancer rotates past both
            ``RESOURCE_EXHAUSTED`` rejections, surfaces ``NoWorkersAvailable``,
            and the durable scheduler keeps it queued rather than dropping or
            double-running it.
        """
        # Arrange — fast retries so an early overflow (a worker not yet
        # surfaced at pool startup) is healed by the scheduler promptly.
        monkeypatch.setattr(executor_module, "_RETRY_INTERVAL_SECONDS", 0.2)
        monkeypatch.setattr(executor_module, "_RETRY_JITTER_MAX_SECONDS", 0.1)
        _install_jobs_index(mock_db)
        cache = LocalFsCache(tmp_path / "cache")
        registry = ProcessorRegistry()
        registry.register(StubProcessor(sleep_seconds=30.0))
        executor = WoolExecutor(
            mock_db, cache, registry, workdir_root=tmp_path / "jobs"
        )
        metas = [{**stub_file_meta(), "local_id": f"ENCFF-{i}"} for i in range(3)]
        pool = wool.WorkerPool(
            spawn=2,
            worker=functools.partial(
                wool.LocalWorker, backpressure=TaskCountBackpressure(1)
            ),
            loadbalancer=PriorityLoadBalancer(),
        )

        async def _settled() -> tuple[int, int]:
            running = pending_resched = 0
            for meta in metas:
                # job_id isn't known until ensure_workflow returns; match on
                # the workflow_key the records carry instead.
                for doc in mock_db.jobs.docs:
                    if doc["local_id"] != meta["local_id"]:
                        continue
                    if doc["status"] == JobStatus.RUNNING.value:
                        running += 1
                    elif (
                        doc["status"] == JobStatus.PENDING.value
                        and doc.get("next_dispatch_at") is not None
                    ):
                        pending_resched += 1
            return running, pending_resched

        # Act / Assert
        try:
            async with pool:
                executor.start_scheduler()
                for meta in metas:
                    await executor.ensure_workflow(meta)

                # Backpressure(1) on two workers caps concurrent RUNNING at
                # two, so the steady state is (2 running, 1 overflowed). The
                # long-running stubs hold that state for the whole poll.
                deadline = asyncio.get_event_loop().time() + 25.0
                running = pending_resched = 0
                while asyncio.get_event_loop().time() < deadline:
                    running, pending_resched = await _settled()
                    if running == 2 and pending_resched == 1:
                        break
                    await asyncio.sleep(0.2)

                assert running == 2, f"expected 2 RUNNING (one per worker), got {running}"
                assert pending_resched == 1, (
                    f"expected 1 overflowed+rescheduled job, got {pending_resched}"
                )
        finally:
            await executor.drain(timeout=15.0)
