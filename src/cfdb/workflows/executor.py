"""Job executor — dispatches processor workflows to a Wool worker pool.

The executor is the bridge between the HTTP layer (``/data``, ``/index``)
and the processing pipeline. On cache miss, the router calls
``ensure_workflow(file_meta)``; the executor atomically claims the workflow
mutex (see ``workflows.lock``) and, if freshly claimed, dispatches the
workflow body to a Wool worker as a fire-and-forget background task. The
router returns ``202 + Location: /jobs/{job_id}`` and the client polls for
completion.

Key properties:

- **Wool dispatch as a stream**: ``@wool.routine`` wraps an async
  generator that yields a typed event stream — heartbeat, stage_complete,
  complete, error — to the API process via wool's bidirectional gRPC
  channel. The API consumes the stream, refreshing JobRecord.updated_at
  on each heartbeat and persisting stages_done as each stage_complete
  arrives.
- **Bounded runtime**: ``async with asyncio.timeout(CAP)`` wraps the
  stream consumer, so cancellation propagates cleanly into the routine
  and through ``stream.aclose()`` into the processor's finally blocks
  (subprocess killpg, partial-cleanup logic, etc.). Wool's own
  dispatch timeout bounds only the client → worker handoff.
- **Partial-commit recovery**: processors check cache state for each
  artifact before running the corresponding stage, so a stage-2 failure
  leaves the stage-1 artifact in cache and the next retry only reruns
  stage 2.
- **Heartbeat-driven stale reclaim**: long-running stages keep their
  JobRecord fresh via the routine's heartbeat stream rather than
  depending on stage transitions; the stale-reclaim threshold drops
  from the original 1-hour conservative bound to a value driven by the
  configured heartbeat interval (see workflows.lock and the
  ``CFDB_WORKFLOW_HEARTBEAT_INTERVAL_S`` env var).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import shutil
import uuid
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import wool

from cfdb.workflows import (
    WORKFLOW_DISPATCH_DEADLINE_S,
    WORKFLOW_DURATION_CAP_S,
    WORKFLOW_HEARTBEAT_INTERVAL_S,
    WORKFLOW_MAX_ACTIVE,
    WORKFLOW_RETRY_INTERVAL_S,
    keys as key_utils,
)
from cfdb.workflows.cache import CacheBackend
from cfdb.workflows.events import (
    Complete,
    Error,
    Heartbeat,
    Progress,
    StageComplete,
    WorkflowEvent,
)
from cfdb.workflows.lock import (
    STALE_WORKFLOW_THRESHOLD,
    claim_workflow,
    count_active_workflows,
    heartbeat_workflow,
    lease_due_dispatch,
    mark_running,
    record_stage_complete,
    release_workflow,
    requeue_orphaned_dispatch,
    reschedule_dispatch,
    update_progress,
)
from cfdb.workflows.models import JobRecord, JobStatus
from cfdb.workflows.processors.base import Processor
from cfdb.workflows.processors.registry import ProcessorRegistry
from cfdb.workflows.provisioner import EcsProvisioner, RetryableProvisionerError

logger = logging.getLogger(__name__)

#: Pipeline version embedded in workflow keys. Bump to invalidate every
#: in-flight workflow and force fresh executions (e.g., after a semantic
#: change to the workflow orchestration itself).
PIPELINE_VERSION = 1

#: Job-runtime cap. 4 hours covers multi-hour preprocessing runs (e.g.,
#: a ``samtools sort`` on a multi-GB BAM followed by ``samtools index``);
#: exceptional files that need longer should be addressed individually.
#: Sourced from ``CFDB_WORKFLOW_DURATION_CAP_S`` (default 14400 s = 4 h)
#: so deployments can override without a code change. Module-level
#: rather than a ctor kwarg because tests cap it via ``monkeypatch.setattr``
#: alongside the other dispatch knobs (``_RETRY_INTERVAL_SECONDS``,
#: ``_HEARTBEAT_INTERVAL_S``) and uniform module-state keeps fixture
#: shape consistent.
_WORKFLOW_DURATION_CAP_SECONDS = WORKFLOW_DURATION_CAP_S

#: Persisted ``error`` prefix surfaced to clients when a job is failed for
#: lack of worker capacity (its dispatch deadline elapsed while queued).
#: Part of the on-wire contract — clients parse it to decide whether to
#: resubmit. Kept as a module constant so tests can import the same symbol
#: the executor writes.
_ERR_PREFIX_CAPACITY = "capacity:"

#: Admission ceiling on concurrently-active workflows (pending + running).
#: ``ensure_workflow`` sheds requests beyond this with ``AdmissionRejected``
#: → HTTP 429. Module-level so tests ``monkeypatch.setattr`` it alongside
#: the other dispatch knobs. Soft cap: a count-then-claim race can briefly
#: overshoot, which is acceptable for a flood guard.
_MAX_ACTIVE_WORKFLOWS = WORKFLOW_MAX_ACTIVE

#: Base cadence for the durable retry scheduler. A dispatch attempt that
#: finds no worker capacity leaves the job PENDING and reschedules its next
#: attempt this far out (plus jitter). Also the scheduler loop's idle wait
#: between ticks, so a freshly-rescheduled job is re-attempted promptly.
_RETRY_INTERVAL_SECONDS = float(WORKFLOW_RETRY_INTERVAL_S)

#: Upper bound on the random jitter added to each reschedule, to spread a
#: thundering herd of queued jobs across retry ticks instead of stampeding
#: the pool in lockstep. Clamped to the retry interval, so tests that
#: monkeypatch ``_RETRY_INTERVAL_SECONDS`` low get correspondingly small,
#: bounded jitter (determinism in tests comes from that clamp, not from
#: zeroing this constant).
_RETRY_JITTER_MAX_SECONDS = min(30.0, _RETRY_INTERVAL_SECONDS)

#: Max wall-clock a job may wait for capacity (measured from
#: ``submitted_at``) before the scheduler fails it ``capacity:``. Replaces
#: the old in-request 240s wait: instead of blocking one request, the job
#: queues durably and is retried until it runs or this deadline elapses.
_DISPATCH_DEADLINE_SECONDS = float(WORKFLOW_DISPATCH_DEADLINE_S)

#: Cadence at which the wool routine emits ``heartbeat`` events into its
#: stream during quiet periods. The API process consumes the stream and
#: refreshes ``JobRecord.updated_at`` on each heartbeat so a healthy
#: long-running stage doesn't get reclaimed as stale.
_HEARTBEAT_INTERVAL_S = float(WORKFLOW_HEARTBEAT_INTERVAL_S)

#: How long a consumer may fail to refresh its heartbeat before it aborts
#: its own job. This makes a stale ``RUNNING`` row reliably mean "the
#: consumer is gone": a consumer that cannot reach Mongo stops computing
#: (and lets ``stream.aclose`` cancel the remote worker) *before* the
#: orphan sweep's ``STALE_WORKFLOW_THRESHOLD`` would revive the row, so
#: recovery is spared a redundant second run of a still-live job. Sized
#: strictly between one heartbeat interval (a single transient write
#: failure must not abort) and the sweep threshold (the consumer must give
#: up with margin to spare); the strict ``STALE > 2*HEARTBEAT`` startup
#: invariant keeps that window strictly positive (at ``STALE == 2*HEARTBEAT``
#: it would collapse to exactly one interval, leaving no transient-failure
#: tolerance — see issue #45 review A7).
_HEARTBEAT_LOSS_ABORT_S = max(
    _HEARTBEAT_INTERVAL_S,
    STALE_WORKFLOW_THRESHOLD.total_seconds() - _HEARTBEAT_INTERVAL_S,
)

#: Max jobs leased per scheduler tick. Bounds the per-tick dispatch
#: fan-out so a large queued backlog (up to ``WORKFLOW_MAX_ACTIVE``) can't
#: spawn thousands of concurrent attempts in one tick; the remainder is
#: leased on subsequent ticks. Generous enough to keep a reasonable fleet
#: saturated, small enough to avoid a thundering herd against the pool.
_MAX_DISPATCHES_PER_TICK = 64

#: Grace given to ``_finalize`` tasks during ``drain``'s second phase
#: after the primary ``_pending_tasks`` gather is done. A workflow that
#: failed mid-write — Mongo connection blip, S3 cache cleanup blocked
#: on a slow workdir rmtree — gets this long for its ``release_workflow``
#: write and ``shutil.rmtree`` to land before the lifespan teardown
#: closes the Motor client. ``release_workflow`` not completing within
#: the grace is *recoverable*: the row stays at ``RUNNING`` and
#: stale-reclamation on next service start releases it as FAILED.
#: Tuning above ~3s would protect more cases but stretches the lifespan
#: shutdown budget; operators that want tighter coupling should pair an
#: increase here with a matching decrease in ``STALE_WORKFLOW_THRESHOLD``.
_FINALIZE_DRAIN_GRACE_SECONDS = 3.0


class WorkflowNotApplicable(ValueError):
    """Raised when ``ensure_workflow`` is called for a file that needs none."""


class ExecutorDraining(WorkflowNotApplicable):
    """Raised by ``ensure_workflow`` once lifespan drain has started.

    A subclass of ``WorkflowNotApplicable`` so callers that catch the
    parent class keep their existing fall-through behavior; the router
    catches ``ExecutorDraining`` explicitly to translate it into a 503
    response with ``Retry-After``.
    """


class AdmissionRejected(RuntimeError):
    """Raised by ``ensure_workflow`` when the active-workflow ceiling is hit.

    Deliberately NOT a :class:`WorkflowNotApplicable` subclass: a caller
    that catches ``WorkflowNotApplicable`` falls through to direct upstream
    streaming, but an admission rejection must surface as a distinct 429
    (with ``Retry-After``) so the client backs off rather than silently
    bypassing the bounded pipeline. Carries the observed ``active`` count,
    the ``ceiling`` it hit, and a ``retry_after_seconds`` hint for the
    response header.
    """

    def __init__(
        self,
        *,
        active: int,
        ceiling: int,
        retry_after_seconds: Optional[int] = None,
    ) -> None:
        self.active = active
        self.ceiling = ceiling
        self.retry_after_seconds = (
            retry_after_seconds
            if retry_after_seconds is not None
            else max(1, int(_RETRY_INTERVAL_SECONDS))
        )
        super().__init__(
            f"Active workflow ceiling reached ({active} >= {ceiling}); "
            "shedding request"
        )


class HeartbeatLost(RuntimeError):
    """Raised by the stream consumer when it can no longer refresh the job.

    Signals that ``heartbeat_workflow`` has been failing for longer than
    :data:`_HEARTBEAT_LOSS_ABORT_S` while a worker is still streaming
    ``heartbeat`` events — i.e. this consumer has lost its connection to
    Mongo and can no longer keep the row fresh. Raising this aborts the
    attempt and ``aclose``s the stream, cancelling the remote worker so it
    stops computing.

    This is a *best-effort optimization*, NOT the load-bearing
    double-dispatch guard. A ``heartbeat`` event is only injected after a
    quiet ``heartbeat_interval`` of stage silence, so a stage that streams
    ``stage_complete`` / ``progress`` faster than that interval never trips
    the check — meaning a Mongo-blind consumer is not guaranteed to abort
    before the orphan sweep revives its row. What actually keeps a recovered
    re-dispatch from corrupting a still-live attempt is the per-attempt
    workdir nonce (``_attempt_dispatch``) plus content-addressed, atomically
    committed cache artifacts. The abort just spares the wasted second run
    when it can; do not remove the per-attempt workdir on the assumption
    this covers the race.
    """


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _next_dispatch_time(now: datetime) -> datetime:
    """Compute the next dispatch attempt time for an overflowed job.

    ``now + retry_interval + jitter``. The jitter spreads a queued backlog
    across ticks; it is bounded by ``_RETRY_JITTER_MAX_SECONDS`` (itself
    clamped to the retry interval), so a test that monkeypatches
    ``_RETRY_INTERVAL_SECONDS`` low gets correspondingly small, bounded
    jitter — no test zeroes the constant. Always strictly after ``now``
    (retry interval is >= 1s), which ``lease_due_dispatch`` requires so a
    leased row moves out of the due-window before its attempt resolves.
    """
    jitter = (
        random.uniform(0, _RETRY_JITTER_MAX_SECONDS)
        if _RETRY_JITTER_MAX_SECONDS > 0
        else 0.0
    )
    return now + timedelta(seconds=_RETRY_INTERVAL_SECONDS + jitter)


class JobExecutor(ABC):
    """Abstract interface for workflow dispatch + job tracking."""

    @abstractmethod
    async def ensure_workflow(
        self, file_meta: dict[str, Any]
    ) -> tuple[JobRecord, bool]:
        """Claim or attach to the workflow for ``file_meta``.

        Returns:
            A tuple ``(job, is_fresh_claim)``. When ``is_fresh_claim`` is
            True, the executor has scheduled the workflow to run in the
            background. Otherwise, the caller has attached to an
            already-running job and should poll ``/jobs/{job.job_id}``.
        """

    @abstractmethod
    async def drain(self, *, timeout: float) -> int:
        """Wait for in-flight workflow tasks to complete, capped by timeout.

        Returns the number of tasks that were pending at drain entry.
        Tasks still running when the timeout elapses are left for
        stale-reclamation on next service start.
        """


@wool.routine
async def _run_processor_routine(
    processor: Processor,
    file_meta: dict[str, Any],
    workdir_str: str,
    cache: CacheBackend,
    heartbeat_interval: float,
) -> AsyncIterator[WorkflowEvent]:
    """Wool routine body: stream processor events back to the dispatcher.

    The processor itself is an async generator that yields
    :class:`~cfdb.workflows.events.StageComplete` and
    :class:`~cfdb.workflows.events.Complete` events. We wrap its stream
    with a heartbeat-aware iterator: while waiting on the next event from
    the processor, every ``heartbeat_interval`` seconds we inject a
    :class:`~cfdb.workflows.events.Heartbeat` so the API process can
    refresh ``JobRecord.updated_at`` and signal liveness without requiring
    the worker to touch Mongo. ``heartbeat_interval`` is passed explicitly
    (resolved API-side at dispatch) rather than read from the worker's
    module globals, because cloudpickle ships this routine by value and the
    worker's own ``_HEARTBEAT_INTERVAL_S`` would otherwise be ignored. An
    exception from the processor is converted to an
    :class:`~cfdb.workflows.events.Error` event so the API consumer can
    record a clean terminal state.

    The routine yields an immediate :class:`~cfdb.workflows.events.Heartbeat`
    as its very first event — the instant a worker *accepts* the dispatch,
    before the (potentially slow) upstream download. This is the dispatcher's
    "worker accepted" signal: ``_attempt_dispatch`` marks the job RUNNING on
    the first event, so a job leaves the leasable PENDING set the moment a
    worker takes it rather than only after the first processor event lands.
    Without this, a long download (which happens before the processor's
    first event) keeps the job PENDING, and the durable scheduler would
    re-lease it and double-dispatch onto the same workdir once the retry
    interval elapsed. A rejected dispatch (worker backpressure) raises
    ``NoWorkersAvailable`` before the routine body runs, so this leading
    heartbeat fires only on acceptance — cleanly distinguishing accepted
    from overflowed.

    Arguments are cloudpickle-serializable: ``processor`` is a stateless
    class instance, ``file_meta`` is a plain dict (B1 strips Mongo's
    ``_id``), ``workdir_str`` crosses as a string, and ``cache`` is the
    configured :class:`CacheBackend` itself — ``LocalFsCache`` carries
    only its root ``Path``; ``S3Cache`` drops its boto3 client in
    ``__getstate__`` and rebuilds it on the worker in ``__setstate__``.
    Handing the real backend across (rather than a bare cache-root path
    the worker would wrap in a ``LocalFsCache``) is what lets the
    S3/ECS profile persist artifacts to the shared S3 store the API
    reads from; otherwise the worker writes to its own local disk and
    the API's ``cache.head`` never finds the artifact.
    """
    workdir = Path(workdir_str)
    workdir.mkdir(parents=True, exist_ok=True)
    # "Worker accepted" signal — see the docstring. Emitted before the
    # processor runs so the dispatcher marks the job RUNNING the instant a
    # worker takes it (before the download), closing the re-lease /
    # double-dispatch window. A rejected dispatch never reaches here.
    yield Heartbeat()
    inner = processor.run(file_meta, workdir, cache).__aiter__()
    next_task: Optional[asyncio.Task] = None
    try:
        while True:
            next_task = asyncio.ensure_future(inner.__anext__())
            # Loop on a per-event wait_for so a heartbeat injects every
            # heartbeat_interval of stage silence; shield the task so
            # a timeout doesn't propagate cancellation into the inner
            # generator mid-stage.
            while True:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(next_task),
                        timeout=heartbeat_interval,
                    )
                    break
                except asyncio.TimeoutError:
                    yield Heartbeat()
                except StopAsyncIteration:
                    # ``wait_for`` re-raises whatever the underlying
                    # task raised. Per PEP 479 a StopAsyncIteration
                    # leaking from an async generator's body is
                    # converted to RuntimeError, which on the wool
                    # boundary surfaces as the unhelpful "async
                    # generator raised StopAsyncIteration". Catch it
                    # here and exit the routine cleanly.
                    return
            try:
                event = next_task.result()
            except StopAsyncIteration:
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Surface processor failure as a terminal event so the
                # API process records a clean FAILED status. Re-raising
                # would interleave with wool's exception-streaming path
                # and yields a less specific error string to /jobs/{id}.
                yield Error(type=type(exc).__name__, error=str(exc))
                return
            next_task = None
            yield event
    finally:
        # Cancel any in-flight __anext__ task and close the inner
        # generator so a worker-side cancel (caller aclose / drain)
        # propagates cleanly into ``processor.run``'s finally blocks
        # (subprocess killpg in B4, partial-cleanup logic, etc.).
        if next_task is not None and not next_task.done():
            next_task.cancel()
            with contextlib.suppress(BaseException):
                await next_task
        with contextlib.suppress(BaseException):
            await inner.aclose()


class WoolExecutor(JobExecutor):
    """Executor backed by a ``wool.WorkerPool`` and a Mongo jobs collection.

    Dispatch tries existing workers first via the priority load balancer.
    Only when an attempt overflows (no worker accepts) does the executor —
    when configured with an :class:`EcsProvisioner` — issue a best-effort
    ``RunTask`` to scale the fleet up, then leave the job queued for a later
    retry; it never spawns a worker pre-dispatch. Without a provisioner
    (PoC dev profile) the executor relies on workers already published into
    the discovery namespace.

    A job that cannot find capacity before its dispatch deadline is failed
    with a ``capacity:``-prefixed error string, which clients parse to
    decide whether to resubmit. Provisioner failures on the overflow path
    are best-effort and swallowed (the job simply retries on the next tick),
    so they never surface as a terminal job state.

    Args:
        db: Motor database handle holding the ``jobs`` collection.
        cache: Cache backend for processor artifacts. The executor
            hands this backend across the Wool boundary to the worker so
            processors persist artifacts to the deployment's real store
            (local FS or S3), not a worker-local filesystem.
        registry: Processor registry mapping artifact kinds to runners.
        workdir_root: Parent directory under which per-attempt workdirs land.
        pipeline_version: Embedded in workflow keys; bump to invalidate
            every in-flight workflow.
        provisioner: Optional :class:`EcsProvisioner`. When ``None``
            (PoC profile), no ``RunTask`` is issued and dispatch
            relies on pre-existing workers.

    The per-workflow wall-clock cap is module-level
    (:data:`_WORKFLOW_DURATION_CAP_SECONDS`) so it monkey-patches the
    same way as the other dispatch-tier knobs.
    """

    def __init__(
        self,
        db,
        cache: CacheBackend,
        registry: ProcessorRegistry,
        *,
        workdir_root: Path,
        pipeline_version: int = PIPELINE_VERSION,
        provisioner: Optional[EcsProvisioner] = None,
    ) -> None:
        self._db = db
        self._cache = cache
        self._registry = registry
        self._workdir_root = Path(workdir_root)
        self._workdir_root.mkdir(parents=True, exist_ok=True)
        self._pipeline_version = pipeline_version
        self._provisioner = provisioner
        self._pending_tasks: set[asyncio.Task] = set()
        #: Finalize tasks (release_workflow + workdir cleanup) created by
        #: _consume_and_finalize's `finally`. Tracked separately so drain()
        #: can await them after the main _pending_tasks gather, ensuring
        #: they complete before the lifespan teardown closes the Motor
        #: client.
        self._finalize_tasks: set[asyncio.Task] = set()
        self._draining = False
        #: The durable retry scheduler — re-attempts dispatch for jobs that
        #: overflowed (no worker capacity) and are awaiting a free worker.
        #: Fresh claims dispatch inline via ``ensure_workflow``; the
        #: scheduler only handles the retry path. Started by
        #: ``start_scheduler`` inside the lifespan's wool-pool context (so
        #: it inherits wool's dispatch contextvars) and cancelled by
        #: ``drain`` before the pool closes.
        self._scheduler_task: Optional[asyncio.Task] = None

    async def ensure_workflow(
        self, file_meta: dict[str, Any]
    ) -> tuple[JobRecord, bool]:
        if self._draining:
            # Reject new claims once drain has started so a request landing
            # during lifespan shutdown does not create a task that the
            # already-snapshotted ``drain`` will not await. The router
            # catches ExecutorDraining → 503 Retry-After.
            raise ExecutorDraining(
                "Executor is draining and cannot accept new workflows"
            )
        processor = self._registry.lookup_for(file_meta)
        if processor is None or not processor.needs_processing(file_meta):
            raise WorkflowNotApplicable(
                "No processor registered for this file, or no work required"
            )

        # Admission ceiling: shed new work once the active backlog
        # (pending + running) hits the cap, so an unauthenticated flood on
        # /data and /index can't queue unbounded jobs in Mongo. Checked
        # AFTER applicability (don't 429 a file that needs no workflow) and
        # BEFORE claiming the mutex. Soft cap — a count-then-claim race may
        # transiently overshoot, acceptable for a flood guard; the
        # per-source mutex still dedups same-file requests below the cap.
        # NOTE: because this precedes the claim, at the ceiling a re-GET for
        # a file whose workflow is already active is also shed with 429
        # (rather than attaching to the in-flight job); the client retries.
        # This is the deliberate trade for shedding before an unbounded
        # count-then-insert race window.
        active = await count_active_workflows(self._db)
        if active >= _MAX_ACTIVE_WORKFLOWS:
            raise AdmissionRejected(active=active, ceiling=_MAX_ACTIVE_WORKFLOWS)

        dcc, local_id, md5 = extract_identity(file_meta)
        wf_key = key_utils.workflow_key(
            dcc=dcc,
            local_id=local_id,
            md5=md5,
            pipeline_version=self._pipeline_version,
        )
        record, fresh = await claim_workflow(
            self._db,
            wf_key,
            dcc=dcc,
            local_id=local_id,
            md5=md5,
            pipeline_version=self._pipeline_version,
            file_meta_snapshot=file_meta,
        )

        if fresh:
            # Dispatch the first attempt inline (fire-and-forget), so a
            # fresh claim runs immediately rather than waiting out a
            # scheduler tick. The job's ``next_dispatch_at`` stays None
            # until this attempt overflows, so the durable scheduler — which
            # only leases jobs with a due ``next_dispatch_at`` — cannot
            # race this inline attempt. Retries (after an overflow sets
            # ``next_dispatch_at``) are driven by the scheduler.
            self._spawn_attempt(record, processor, file_meta)

        return record, fresh

    def _spawn_attempt(
        self,
        record: JobRecord,
        processor: Processor,
        file_meta: dict[str, Any],
    ) -> None:
        """Spawn a tracked background task running one dispatch attempt."""
        task = asyncio.create_task(
            self._attempt_dispatch(record, processor, file_meta)
        )
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)
        task.add_done_callback(_log_unexpected_exception)

    def start_scheduler(self) -> None:
        """Start the durable retry scheduler as a background task.

        MUST be called from inside the lifespan's ``wool.WorkerPool``
        context so the created task inherits wool's dispatch contextvars
        (the same mechanism request-spawned tasks rely on via
        ``attach_wool_context``); otherwise ``@wool.routine`` dispatch from
        the scheduler would fail with no pool in context. Idempotent.
        """
        if self._scheduler_task is not None:
            return
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        self._scheduler_task.add_done_callback(_log_unexpected_exception)

    async def _scheduler_loop(self) -> None:
        """Re-attempt dispatch for queued jobs until drain.

        Fresh claims dispatch inline (``ensure_workflow``); this loop drives
        the *retry* path. Each tick leases every currently-due job — one
        whose inline (or prior) attempt overflowed and set a
        ``next_dispatch_at`` now in the past — and spawns a fresh dispatch
        attempt for it, then sleeps one retry interval. A per-tick exception
        is logged and swallowed so the scheduler never dies; cancellation
        (from ``drain``) breaks the loop. DB-backed, so a freshly-started
        replica resumes whatever queue the previous process left behind.
        """
        while not self._draining:
            try:
                await self._recover_orphans()
                await self._drain_due_jobs()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Scheduler dispatch tick failed; continuing")
            await asyncio.sleep(_RETRY_INTERVAL_SECONDS)

    async def _recover_orphans(self) -> None:
        """Re-queue jobs orphaned by an API/worker crash for re-dispatch.

        Runs at the top of every scheduler tick — including the first, on
        boot — so a process that died mid-flight does not strand jobs the
        normal lease query cannot pick up (``RUNNING`` rows whose consumer
        died, and fresh ``PENDING`` claims that never rescheduled). Recovery
        is then autonomous: it no longer depends on a client re-requesting
        the same file to trigger ``claim_workflow``'s stale-reclaim. Gated
        on the stale threshold so healthy heartbeating jobs are untouched.

        Note the recovery promise is bounded by the same dispatch deadline
        as a fresh job: ``requeue_orphaned_dispatch`` preserves the original
        ``submitted_at``, and ``_attempt_dispatch`` measures the deadline
        from it, so an orphan older than ``CFDB_WORKFLOW_DISPATCH_DEADLINE_S``
        is failed ``capacity:`` on its first recovery attempt rather than
        resumed (its committed cache artifacts survive for a later fresh
        ``GET`` to reuse). Recovery is best-effort, not unbounded.
        """
        now = _utcnow()
        requeued = await requeue_orphaned_dispatch(
            self._db, now=now, stale_before=now - STALE_WORKFLOW_THRESHOLD
        )
        if requeued:
            logger.info(
                "Re-queued %d orphaned workflow(s) for re-dispatch after a "
                "restart or worker crash",
                requeued,
            )

    async def _drain_due_jobs(self) -> None:
        """Lease currently-due queued jobs and spawn their retry attempts.

        ``lease_due_dispatch`` atomically claims one due job and pushes its
        ``next_dispatch_at`` forward, so concurrent ticks (or replicas)
        can't double-lease and a crashed attempt is still retried. We loop
        until nothing is due (or the per-tick cap is hit), dispatching each
        leased job as a tracked background task so a winning attempt's long
        stream-consume doesn't block the scheduler from filling the pool.

        The per-tick cap (``_MAX_DISPATCHES_PER_TICK``) bounds the dispatch
        fan-out: after a large backlog (up to ``WORKFLOW_MAX_ACTIVE``) a
        single tick would otherwise spawn thousands of concurrent attempts,
        each opening a wool stream that immediately overflows. The remainder
        is leased on subsequent ticks; the deadline/retry machinery tolerates
        the delay.
        """
        dispatched = 0
        while not self._draining and dispatched < _MAX_DISPATCHES_PER_TICK:
            now = _utcnow()
            leased = await lease_due_dispatch(
                self._db, now=now, next_at=_next_dispatch_time(now)
            )
            if leased is None:
                break
            file_meta = leased.file_meta_snapshot
            if file_meta is None:
                logger.error(
                    "Leased job %s has no file_meta_snapshot — failing",
                    leased.job_id,
                )
                await release_workflow(
                    self._db,
                    leased.job_id,
                    JobStatus.FAILED,
                    error="internal: leased job missing file_meta_snapshot",
                )
                continue
            processor = self._registry.lookup_for(file_meta)
            if processor is None:
                logger.error(
                    "No processor for leased job %s — failing", leased.job_id
                )
                await release_workflow(
                    self._db,
                    leased.job_id,
                    JobStatus.FAILED,
                    error="internal: no processor for leased job",
                )
                continue
            self._spawn_attempt(leased, processor, file_meta)
            dispatched += 1

        # Leading operability signal: a saturated queue is otherwise silent
        # until jobs start failing ``capacity:`` at the dispatch deadline.
        # Emitted at most once per tick (cadence ``_RETRY_INTERVAL_SECONDS``),
        # so it does not flood the log even under a sustained backlog.
        if dispatched:
            logger.info(
                "Scheduler tick dispatched %d queued workflow(s)%s",
                dispatched,
                " (per-tick cap hit; remainder deferred to the next tick)"
                if dispatched >= _MAX_DISPATCHES_PER_TICK
                else "",
            )

    async def drain(self, *, timeout: float) -> int:
        """Wait for in-flight workflow tasks to complete.

        Used by the FastAPI lifespan on shutdown to give fire-and-forget
        workflows a chance to finish cleanly against the still-open
        ``wool.WorkerPool`` and Mongo client before teardown races their
        writes.

        Args:
            timeout: Upper bound in seconds. Tasks still running after
                the timeout are left for stale-reclamation on next
                service start.

        Returns:
            The number of tasks that were pending when drain began. 0
            means the executor was idle and the call returned
            immediately.
        """
        self._draining = True
        # Stop the dispatch driver first so no new attempt tasks are spawned
        # into the set we're about to snapshot, and so it stops dispatching
        # into a pool that's about to close.
        if self._scheduler_task is not None:
            self._scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._scheduler_task
            self._scheduler_task = None
        pending = list(self._pending_tasks)
        if not pending:
            return 0
        try:
            await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            # Tasks still running after the timeout would race the
            # imminent WorkerPool/Mongo teardown — release_workflow
            # against a closed Motor client surfaces as a noisy
            # exception in the lifespan exit, and the wool stream they
            # hold blocks pool aclose. Cancel them explicitly and
            # await the cancellation so _consume_and_finalize's shielded
            # _finalize gets its 2s best-effort grace before we let
            # the lifespan continue to teardown.
            for task in pending:
                if not task.done():
                    task.cancel()
            with contextlib.suppress(Exception):
                await asyncio.gather(*pending, return_exceptions=True)

        # Second phase: await any finalize tasks that the cancellation
        # path may have left running under their shield. Bounded by
        # ``_FINALIZE_DRAIN_GRACE_SECONDS`` so a stuck Mongo write
        # doesn't hold up shutdown indefinitely; tasks not done after
        # this are abandoned, and their jobs will be reclaimed as
        # stale on next service start.
        finalize_pending = list(self._finalize_tasks)
        if finalize_pending:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(
                    asyncio.gather(*finalize_pending, return_exceptions=True),
                    timeout=_FINALIZE_DRAIN_GRACE_SECONDS,
                )
        return len(pending)

    async def _attempt_dispatch(
        self,
        record: JobRecord,
        processor: Processor,
        file_meta: dict[str, Any],
    ) -> None:
        """Run one dispatch attempt for a leased PENDING job.

        Offers the task to the existing worker pool **once** via the
        priority load balancer. On success the job is marked RUNNING (only
        now that a worker has accepted — a queued job stays PENDING until
        then) and its event stream is consumed to completion. On overflow
        (every worker rejected, or none exist) the job stays PENDING, a
        best-effort bounded worker spawn is requested, and it is rescheduled
        for a later tick — unless it has blown its dispatch deadline, in
        which case it is failed ``capacity:``. This inverts the old
        unconditional pre-dispatch spawn: existing capacity is tried first
        and scale-up happens only on overflow.
        """
        # Per-attempt workdir (unique nonce), not per-job. If the orphan
        # sweep ever re-dispatches a job whose previous attempt is somehow
        # still alive (e.g. event-loop starvation that defeated the
        # heartbeat-loss abort), the two attempts use disjoint scratch dirs
        # and cannot corrupt each other's downloads / sort temp / cleanup.
        # Partial-commit recovery is unaffected — it keys off the cache, not
        # the workdir.
        workdir = self._workdir_root / f"{record.job_id}-{uuid.uuid4().hex}"

        # Deadline: a job that has waited for capacity longer than the
        # dispatch deadline (measured from ``submitted_at``) is failed rather
        # than retried forever. ``capacity:`` is the stable on-wire prefix
        # clients parse to decide whether to resubmit. Note the clock runs
        # from original submission and is NOT reset on orphan recovery, so a
        # job recovered after its deadline is failed here rather than resumed
        # — recovery is best-effort, bounded by the same deadline.
        waited = (_utcnow() - record.submitted_at).total_seconds()
        if waited > _DISPATCH_DEADLINE_SECONDS:
            logger.warning(
                "Dispatch deadline exceeded for %s after %.0fs; failing",
                record.job_id,
                waited,
            )
            await self._finalize(
                workdir,
                record.job_id,
                JobStatus.FAILED,
                f"{_ERR_PREFIX_CAPACITY} dispatch deadline exceeded",
            )
            return

        try:
            stream = await self._open_stream_once(processor, file_meta, workdir)
        except asyncio.CancelledError:
            # Cancelled (e.g. lifespan drain) before a worker accepted.
            # Finalize FAILED so the row doesn't dangle PENDING until
            # stale-reclaim; the client resubmits. Best-effort, then
            # propagate the cancellation.
            with contextlib.suppress(Exception):
                await self._finalize(
                    workdir,
                    record.job_id,
                    JobStatus.FAILED,
                    "Workflow cancelled (worker shutdown)",
                )
            raise
        except wool.NoWorkersAvailable:
            # No capacity this pass — keep the job PENDING, request a
            # bounded scale-up (ECS only), and reschedule for a later tick.
            await self._handle_overflow(record)
            return
        except Exception as exc:
            logger.exception("Failed to open routine stream for %s", record.job_id)
            await self._finalize(
                workdir, record.job_id, JobStatus.FAILED, str(exc)
            )
            return

        # A worker accepted the task. Claim RUNNING now — after the stream
        # opened — so a job only leaves PENDING once it is genuinely
        # running. ``mark_running`` is fenced on PENDING; a RuntimeError
        # means a racing claimant (or stale-reclaim) already owns the row,
        # so hand off: close the stream (cancelling the remote routine) and
        # bail without duplicate work.
        try:
            await mark_running(self._db, record.job_id)
        except asyncio.CancelledError:
            with contextlib.suppress(BaseException):
                await stream.aclose()
            raise
        except RuntimeError:
            logger.info(
                "mark_running rejected for %s (row no longer PENDING); "
                "closing stream and handing off",
                record.job_id,
            )
            with contextlib.suppress(BaseException):
                await stream.aclose()
            # The successor owns the row, so do NOT release the mutex here —
            # but this attempt's workdir was already created by the routine,
            # so clean it up to avoid leaking scratch on the hand-off (A1).
            with contextlib.suppress(Exception):
                await asyncio.to_thread(
                    shutil.rmtree, str(workdir), ignore_errors=True
                )
            return
        except Exception as exc:
            logger.exception("Failed to mark job %s as running", record.job_id)
            with contextlib.suppress(BaseException):
                await stream.aclose()
            await self._finalize(
                workdir,
                record.job_id,
                JobStatus.FAILED,
                f"mark_running failed: {exc}",
            )
            return

        await self._consume_and_finalize(record, stream, workdir)

    async def _handle_overflow(self, record: JobRecord) -> None:
        """Handle a dispatch attempt that found no worker capacity.

        Requests one best-effort worker spawn through the provisioner (ECS
        profile only; a no-op in the local LAN profile, where the pool is
        fixed) and reschedules the job's next dispatch attempt. The spawn is
        strictly best-effort — existing workers may free up before the next
        tick — so any provisioner error is logged and swallowed rather than
        failing the job; the dispatch deadline bounds the retry loop.
        """
        if self._provisioner is not None:
            try:
                await self._provisioner.request(dedup_key=record.workflow_key)
            except asyncio.CancelledError:
                raise
            except RetryableProvisionerError as exc:
                logger.warning(
                    "Provisioner retryable capacity error for %s; will retry "
                    "on the next tick: %s",
                    record.job_id,
                    exc,
                )
            except Exception:
                logger.exception(
                    "Provisioner request failed for %s; will retry on the "
                    "next tick",
                    record.job_id,
                )
        # Leading operability signal that the pool is over capacity: the job
        # found no worker and is being deferred to a later tick. Logged at
        # info so a steadily-overflowing fleet is visible before jobs start
        # failing ``capacity:`` at the dispatch deadline.
        logger.info(
            "No worker capacity for %s; rescheduling (pool overflow)",
            record.job_id,
        )
        await reschedule_dispatch(
            self._db, record.job_id, next_at=_next_dispatch_time(_utcnow())
        )

    async def _consume_and_finalize(
        self,
        record: JobRecord,
        stream: AsyncIterator[Any],
        workdir: Path,
    ) -> None:
        """Consume a running workflow's event stream and release it terminal.

        The routine emits ``heartbeat``, ``stage_complete``, ``complete``,
        and ``error`` events as an async generator over wool's gRPC stream.
        This consumer refreshes ``JobRecord.updated_at`` on each heartbeat
        (so a healthy long-running stage doesn't trip stale-reclaim) and
        persists ``stages_done`` / ``artifact_cache_keys`` as each
        ``stage_complete`` arrives, so /jobs/{id} reflects partial
        progress in real time rather than all-at-once at the end.

        A ``try/finally`` ensures the job always reaches a terminal status
        and the per-job workdir is cleaned up, even when the task is
        cancelled during shutdown. ``CancelledError`` propagates after the
        terminal write. The caller has already marked the job RUNNING and
        opened ``stream``.
        """
        final_status: JobStatus = JobStatus.FAILED
        final_error: Optional[str] = None
        # Time of the last successful heartbeat write. Seeded now because
        # the caller just marked the job RUNNING (the row is fresh). If
        # heartbeat writes then fail for longer than _HEARTBEAT_LOSS_ABORT_S
        # we raise HeartbeatLost and abort, so a Mongo-blind consumer stops
        # computing before the orphan sweep revives its row. Best-effort: it
        # only re-evaluates on a Heartbeat event (i.e. during quiet stages),
        # so the per-attempt workdir — not this abort — is the actual
        # corruption guard (see HeartbeatLost).
        last_heartbeat_ok = _utcnow()

        try:

            try:
                async with asyncio.timeout(_WORKFLOW_DURATION_CAP_SECONDS):
                    async for event in stream:
                        if isinstance(event, Heartbeat):
                            try:
                                await heartbeat_workflow(self._db, record.job_id)
                                last_heartbeat_ok = _utcnow()
                            except Exception:
                                stalled = (
                                    _utcnow() - last_heartbeat_ok
                                ).total_seconds()
                                if stalled > _HEARTBEAT_LOSS_ABORT_S:
                                    # Lost Mongo long enough that the orphan
                                    # sweep will treat this row as dead; abort
                                    # so recovery is spared a redundant second
                                    # run of this still-live attempt.
                                    raise HeartbeatLost(
                                        f"no successful heartbeat for "
                                        f"{stalled:.0f}s "
                                        f"(> {_HEARTBEAT_LOSS_ABORT_S:.0f}s)"
                                    )
                                logger.exception(
                                    "Heartbeat failed for %s; continuing",
                                    record.job_id,
                                )
                        elif isinstance(event, Progress):
                            if event.value:
                                try:
                                    await update_progress(
                                        self._db, record.job_id, event.value
                                    )
                                except Exception:
                                    logger.exception(
                                        "update_progress failed for %s; continuing",
                                        record.job_id,
                                    )
                        elif isinstance(event, StageComplete):
                            try:
                                await record_stage_complete(
                                    self._db,
                                    record.job_id,
                                    stage=event.kind.value,
                                    artifact_kind=event.kind,
                                    cache_key=event.key,
                                )
                            except Exception as exc:
                                # The cache artifact is already committed —
                                # a retry will hit the cache via ``head()``
                                # and skip this stage. Fail the workflow so
                                # the next request re-dispatches cleanly
                                # rather than completing with an incomplete
                                # ``artifact_cache_keys`` map.
                                logger.exception(
                                    "record_stage_complete failed for %s; "
                                    "marking workflow FAILED",
                                    record.job_id,
                                )
                                final_status = JobStatus.FAILED
                                final_error = (
                                    f"record_stage_complete failed: {exc}"
                                )
                                break
                        elif isinstance(event, Complete):
                            final_status = JobStatus.COMPLETED
                            final_error = None
                            break
                        elif isinstance(event, Error):
                            final_status = JobStatus.FAILED
                            final_error = f"{event.type}: {event.error}"
                            break
                        else:
                            logger.warning(
                                "Unknown routine event %r for job %s",
                                event,
                                record.job_id,
                            )
            except asyncio.TimeoutError:
                final_error = (
                    f"Workflow exceeded {_WORKFLOW_DURATION_CAP_SECONDS}s "
                    "runtime cap"
                )
            except asyncio.CancelledError:
                # Only overwrite the cancellation message when the
                # stream hadn't already reached a terminal status. A
                # ``complete`` event flips ``final_status`` to
                # COMPLETED and ``break``s; a cancel delivered between
                # the ``break`` and this except clause would otherwise
                # overwrite ``final_error`` with the cancel reason and
                # persist the contradictory (status=COMPLETED,
                # error=cancelled) pair via ``release_workflow``.
                if final_status == JobStatus.FAILED:
                    final_error = "Workflow cancelled (worker shutdown)"
                raise
            except HeartbeatLost as exc:
                # Expected abort, not a crash: this consumer lost Mongo and
                # is giving up the job so the orphan sweep can recover it
                # without a concurrent live attempt. The terminal write
                # below will likely no-op (Mongo is unreachable for us); the
                # inner finally still aclose()s the stream, cancelling the
                # remote worker so it stops computing.
                logger.warning(
                    "Aborting workflow %s: %s", record.job_id, exc
                )
                final_status = JobStatus.FAILED
                final_error = f"heartbeat lost: {exc}"
            except Exception as exc:
                logger.exception(
                    "Workflow %s failed during stream consumption",
                    record.job_id,
                )
                final_error = str(exc)
            finally:
                # Close the stream so wool propagates cancellation into
                # the remote routine (which in turn lets the processor's
                # finally blocks run — subprocess killpg from B4, etc.).
                with contextlib.suppress(BaseException):
                    await stream.aclose()
        finally:
            # Always release the mutex and clean up the workdir, even on
            # cancellation. The release/cleanup pair is shielded from outer
            # cancellation so a drain-timeout cancel does not abort the
            # Mongo write or rmtree mid-flight; if the outer await is
            # cancelled we make one more bounded best-effort wait before
            # propagating, giving release_workflow a fair shot at landing
            # before lifespan teardown closes the Mongo client.
            finalize = asyncio.ensure_future(
                self._finalize(workdir, record.job_id, final_status, final_error)
            )
            # Track the finalize task so drain() can await it explicitly.
            # Without this, a finalize that doesn't complete within the
            # 2s grace below survives as an orphan and may race
            # Motor client close in the lifespan teardown.
            self._finalize_tasks.add(finalize)
            finalize.add_done_callback(self._finalize_tasks.discard)
            try:
                await asyncio.shield(finalize)
            except asyncio.CancelledError:
                try:
                    await asyncio.wait_for(asyncio.shield(finalize), timeout=2.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
                raise

    async def _finalize(
        self,
        workdir: Path,
        job_id: str,
        final_status: JobStatus,
        final_error: Optional[str],
    ) -> None:
        """Release the workflow mutex and clean up the per-job workdir.

        Designed to run inside an ``asyncio.shield`` so it survives outer
        cancellation. Each step is independently guarded so a Mongo write
        failure does not skip the workdir cleanup, and vice versa.
        """
        try:
            await release_workflow(self._db, job_id, final_status, error=final_error)
        except Exception:
            logger.exception(
                "Unable to release workflow %s — record will be reclaimed "
                "after the stale threshold",
                job_id,
            )
        try:
            await asyncio.to_thread(shutil.rmtree, str(workdir), ignore_errors=True)
        except Exception:
            logger.exception("Unable to clean workdir %s", workdir)

    async def _open_stream_once(
        self,
        processor: Processor,
        file_meta: dict[str, Any],
        workdir: Path,
    ) -> AsyncIterator[dict[str, Any]]:
        """Open the routine's event stream in a single dispatch attempt.

        Constructs the ``@wool.routine`` stream and pulls its first event,
        which is where wool surfaces ``wool.NoWorkersAvailable`` when the
        priority load balancer found no worker willing to accept the task.
        That exception propagates to ``_attempt_dispatch``, which reschedules
        the job — the durable retry scheduler is now the retry mechanism, so
        there is no in-attempt polling loop (a single reschedule tick
        covers an ECS cold start). On success returns an async iterator
        yielding the first event followed by the rest of the stream.
        """
        stream = _run_processor_routine(
            processor,
            file_meta,
            str(workdir),
            self._cache,
            _HEARTBEAT_INTERVAL_S,
        )
        # ``returning`` flips True only after we've handed the stream off to
        # ``_prepend_first_event``. Any other exit (NoWorkersAvailable, a
        # CancelledError from __anext__, an unexpected exception) leaves the
        # freshly-constructed stream unawaited; the finally aclose()s it so
        # wool's pool can release the lease cleanly.
        returning = False
        try:
            first_event = await stream.__anext__()
            returning = True
            return _prepend_first_event(first_event, stream)
        finally:
            if not returning:
                with contextlib.suppress(BaseException):
                    await stream.aclose()


async def _prepend_first_event(
    first: dict[str, Any], stream: AsyncIterator[dict[str, Any]]
) -> AsyncIterator[dict[str, Any]]:
    """Yield ``first`` then the rest of ``stream``.

    Used by ``_open_stream_once`` to consume the initial ``__anext__``
    (where wool surfaces ``NoWorkersAvailable``), then expose the remaining
    events to the workflow consumer through a single uniform async-iterator
    interface.
    """
    try:
        yield first
        async for event in stream:
            yield event
    finally:
        with contextlib.suppress(BaseException):
            await stream.aclose()


def _log_unexpected_exception(task: asyncio.Task) -> None:
    """Done-callback that surfaces an unexpected exception via the logger.

    The scheduler loop and ``_attempt_dispatch`` are supposed to catch
    every failure and route it to a terminal release or a reschedule; if
    anything escapes that contract (e.g. a future refactor leaves a bare
    ``raise`` outside the try), asyncio's default behavior is to log the
    unretrieved exception only at interpreter shutdown. Surfacing it
    through the project logger immediately makes the regression visible.
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("Workflow background task ended with unhandled exception: %r", exc)


# ``extract_identity`` moved to ``cfdb.workflows.keys`` — re-exported below
# for backward compatibility with existing importers in this module.
extract_identity = key_utils.extract_identity
