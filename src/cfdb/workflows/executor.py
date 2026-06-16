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
import shutil
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, Optional

import wool

from cfdb.workflows import (
    WORKFLOW_DISPATCH_WAIT_S,
    WORKFLOW_DURATION_CAP_S,
    WORKFLOW_HEARTBEAT_INTERVAL_S,
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
    claim_workflow,
    heartbeat_workflow,
    mark_running,
    record_stage_complete,
    release_workflow,
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
#: alongside the other dispatch knobs (``_DISPATCH_WAIT_SECONDS``,
#: ``_HEARTBEAT_INTERVAL_S``) and uniform module-state keeps fixture
#: shape consistent.
_WORKFLOW_DURATION_CAP_SECONDS = WORKFLOW_DURATION_CAP_S

#: Persisted ``error`` prefix surfaced to clients when the dispatch
#: budget is exhausted with no workers available. The prefix is part
#: of the on-wire contract — clients parse it to decide whether to
#: resubmit. Kept as a module constant so tests can import the same
#: symbol the executor writes.
_ERR_PREFIX_CAPACITY = "capacity:"

#: Persisted ``error`` prefix surfaced to clients when the provisioner
#: itself raises a non-retryable exception. Distinct from
#: :data:`_ERR_PREFIX_CAPACITY` so clients can tell "retry me later,
#: capacity issue" apart from "the provisioner crashed".
_ERR_PREFIX_PROVISIONER = "provisioner:"

#: Total wall-clock budget waiting for a leased worker to surface during
#: dispatch. Sized for an ECS cold start (target ~30-90s on Fargate) so a
#: ``NoWorkersAvailable`` at the moment of dispatch does not immediately
#: fail the job — instead we poll the pool until a worker shows up or
#: the budget expires.
_DISPATCH_WAIT_SECONDS = float(WORKFLOW_DISPATCH_WAIT_S)

#: Cadence at which we re-attempt the dispatch while waiting on capacity.
#: Sub-second polling buys us nothing because ECS scale-up dominates the
#: wait; a 1-second cadence keeps the noise floor low while still
#: surfacing a freshly-available worker quickly.
_DISPATCH_RETRY_INTERVAL_SECONDS = 1.0

#: Cadence at which the wool routine emits ``heartbeat`` events into its
#: stream during quiet periods. The API process consumes the stream and
#: refreshes ``JobRecord.updated_at`` on each heartbeat so a healthy
#: long-running stage doesn't get reclaimed as stale.
_HEARTBEAT_INTERVAL_S = float(WORKFLOW_HEARTBEAT_INTERVAL_S)

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
) -> AsyncIterator[WorkflowEvent]:
    """Wool routine body: stream processor events back to the dispatcher.

    The processor itself is an async generator that yields
    :class:`~cfdb.workflows.events.StageComplete` and
    :class:`~cfdb.workflows.events.Complete` events. We wrap its stream
    with a heartbeat-aware iterator: while waiting on the next event from
    the processor, every ``_HEARTBEAT_INTERVAL_S`` we inject a
    :class:`~cfdb.workflows.events.Heartbeat` so the API process can
    refresh ``JobRecord.updated_at`` and signal liveness without requiring
    the worker to touch Mongo. An exception from the processor is
    converted to an :class:`~cfdb.workflows.events.Error` event so the API
    consumer can record a clean terminal state.

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
    inner = processor.run(file_meta, workdir, cache).__aiter__()
    next_task: Optional[asyncio.Task] = None
    try:
        while True:
            next_task = asyncio.ensure_future(inner.__anext__())
            # Loop on a per-event wait_for so a heartbeat injects every
            # _HEARTBEAT_INTERVAL_S of stage silence; shield the task so
            # a timeout doesn't propagate cancellation into the inner
            # generator mid-stage.
            while True:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(next_task),
                        timeout=_HEARTBEAT_INTERVAL_S,
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

    When configured with an :class:`EcsProvisioner`, the executor also
    issues a ``RunTask`` on each fresh claim before opening the routine
    stream, so a Fargate worker boots before the dispatch retry loop
    begins polling for it. Without a provisioner (PoC dev profile) the
    executor relies on workers already published into the discovery
    namespace.

    A :class:`RetryableProvisionerError` from the provisioner surfaces
    as a terminal ``FAILED`` job with a ``capacity:``-prefixed error
    string; any other provisioner failure surfaces with a
    ``provisioner:`` prefix. Clients parse the prefix to decide
    whether to resubmit.

    Args:
        db: Motor database handle holding the ``jobs`` collection.
        cache: Cache backend for processor artifacts. The executor
            hands this backend across the Wool boundary to the worker so
            processors persist artifacts to the deployment's real store
            (local FS or S3), not a worker-local filesystem.
        registry: Processor registry mapping artifact kinds to runners.
        workdir_root: Parent directory under which per-job workdirs land.
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
        #: _run_workflow's `finally`. Tracked separately so drain() can
        #: await them after the main _pending_tasks gather, ensuring they
        #: complete before the lifespan teardown closes the Motor client.
        self._finalize_tasks: set[asyncio.Task] = set()
        self._draining = False

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
            task = asyncio.create_task(
                self._run_workflow(record, processor, file_meta)
            )
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)
            task.add_done_callback(_log_unexpected_exception)

        return record, fresh

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
            # await the cancellation so _run_workflow's shielded
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

    async def _run_workflow(
        self,
        record: JobRecord,
        processor: Processor,
        file_meta: dict[str, Any],
    ) -> None:
        """Background coroutine: mark running, consume the routine's event
        stream, and release a terminal status.

        The routine emits ``heartbeat``, ``stage_complete``, ``complete``,
        and ``error`` events as an async generator over wool's gRPC stream.
        This consumer refreshes ``JobRecord.updated_at`` on each heartbeat
        (so a healthy long-running stage doesn't trip stale-reclaim) and
        persists ``stages_done`` / ``artifact_cache_keys`` as each
        ``stage_complete`` arrives, so /jobs/{id} reflects partial
        progress in real time rather than all-at-once at the end.

        A ``try/finally`` ensures the job always reaches a terminal
        status and the per-job workdir is cleaned up, even when
        ``mark_running`` raises or the task is cancelled during
        shutdown. ``CancelledError`` propagates after the terminal write.
        """
        workdir = self._workdir_root / record.job_id

        final_status: JobStatus = JobStatus.FAILED
        final_error: Optional[str] = None

        try:
            # mark_running lives inside the try so a Mongo write failure
            # here still routes through the finally — the row gets
            # released to FAILED rather than dangling at PENDING for
            # the full stale-reclaim window.
            try:
                await mark_running(self._db, record.job_id)
            except Exception as exc:
                logger.exception("Failed to mark job %s as running", record.job_id)
                final_error = f"mark_running failed: {exc}"
                return

            # Request a worker via the external provisioner (e.g. ECS
            # ``RunTask``) before opening the routine stream. The
            # provisioner dedup-keys on the workflow mutex so two
            # concurrent fresh claims for the same source file
            # share one ``RunTask`` and one worker.
            #
            # ``RetryableProvisionerError`` is treated as a best-effort
            # scale-up failure, not a workflow-level failure: the Wool
            # pool routes ``@wool.routine`` calls to *any* available
            # worker via the discovery namespace, so an existing idle
            # worker can still service this job. Log a warning and fall
            # through to ``_open_stream_with_retry``; the
            # ``capacity:``-prefixed terminal status is reserved for the
            # case where dispatch *also* exhausts its budget with no
            # workers available.
            if self._provisioner is not None:
                try:
                    await self._provisioner.request(dedup_key=record.workflow_key)
                except asyncio.CancelledError:
                    final_error = "Workflow cancelled (worker shutdown)"
                    raise
                except RetryableProvisionerError as exc:
                    logger.warning(
                        "Provisioner reported retryable capacity error for %s; "
                        "falling through to dispatch against existing workers: %s",
                        record.job_id,
                        exc,
                    )
                except Exception as exc:
                    logger.exception(
                        "Provisioner request failed for %s", record.job_id
                    )
                    final_error = f"{_ERR_PREFIX_PROVISIONER} {exc}"
                    return
                # Bound the stale-reclaim window across the provisioner
                # round-trip: ``mark_running`` ran before
                # ``provisioner.request`` and a Fargate cold start can
                # plausibly exceed STALE_WORKFLOW_THRESHOLD. Heartbeat
                # so the row stays fresh through the upcoming dispatch
                # wait. A failed heartbeat is logged but does not
                # abort the workflow — the next ``heartbeat`` event in
                # the stream loop will retry.
                try:
                    await heartbeat_workflow(self._db, record.job_id)
                except Exception:
                    logger.exception(
                        "Post-provisioner heartbeat failed for %s; continuing",
                        record.job_id,
                    )

            try:
                stream = await self._open_stream_with_retry(
                    processor, file_meta, workdir
                )
            except asyncio.CancelledError:
                final_error = "Workflow cancelled (worker shutdown)"
                raise
            except wool.NoWorkersAvailable as exc:
                # Dispatch budget exhausted with no workers reachable.
                # ``capacity:`` is the stable on-wire prefix clients
                # parse to decide whether to resubmit the job — its
                # origin shifted from the provisioner-failure branch
                # (which now falls through) to here, but the wire
                # contract is unchanged.
                logger.warning(
                    "Dispatch budget exhausted for %s with no workers available",
                    record.job_id,
                )
                final_error = f"{_ERR_PREFIX_CAPACITY} {exc}"
                return
            except Exception as exc:
                logger.exception("Failed to open routine stream for %s", record.job_id)
                final_error = str(exc)
                return

            try:
                async with asyncio.timeout(_WORKFLOW_DURATION_CAP_SECONDS):
                    async for event in stream:
                        if isinstance(event, Heartbeat):
                            try:
                                await heartbeat_workflow(self._db, record.job_id)
                            except Exception:
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

    async def _open_stream_with_retry(
        self,
        processor: Processor,
        file_meta: dict[str, Any],
        workdir: Path,
    ) -> AsyncIterator[dict[str, Any]]:
        """Open the routine's event stream, retrying on ``NoWorkersAvailable``.

        The API leases workers from a pool that's provisioned externally
        (ECS in production); cold starts can take ~30-90s. Until wool
        exposes a quorum-style readiness gate we poll the pool with
        ``NoWorkersAvailable`` retries on each attempt to open the
        stream. ``_DISPATCH_WAIT_SECONDS`` caps the total wait so a
        stuck scale-up surfaces as a workflow failure rather than
        hanging until the much-larger ``_WORKFLOW_DURATION_CAP_SECONDS``
        cap fires.

        Returns an async iterator that yields the first event followed
        by the rest of the routine's stream. The retry-on-capacity logic
        only applies to the first ``__anext__`` call (which is where
        wool surfaces ``NoWorkersAvailable``); once events start
        flowing, subsequent ``__anext__`` calls just wait on the worker.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _DISPATCH_WAIT_SECONDS
        attempt = 0
        last_exc: wool.NoWorkersAvailable | None = None
        while True:
            stream = _run_processor_routine(
                processor,
                file_meta,
                str(workdir),
                self._cache,
            )
            # ``returning`` flips True only after we've handed the
            # stream off to ``_prepend_first_event``. Any other exit
            # from this iteration (NoWorkersAvailable, CancelledError
            # from __anext__ or the inter-iteration sleep, unexpected
            # exception) leaves the freshly-constructed stream
            # unawaited; the finally aclose()s it so wool's pool can
            # release the lease cleanly on drain.
            returning = False
            try:
                try:
                    first_event = await stream.__anext__()
                except wool.NoWorkersAvailable as exc:
                    last_exc = exc
                    attempt += 1
                    if loop.time() >= deadline:
                        break
                    if attempt >= 2:
                        logger.warning(
                            "No workers available — retrying dispatch "
                            "(attempt %d, %.1fs of %.1fs budget remaining)",
                            attempt,
                            deadline - loop.time(),
                            _DISPATCH_WAIT_SECONDS,
                        )
                    await asyncio.sleep(_DISPATCH_RETRY_INTERVAL_SECONDS)
                    continue
                returning = True
                return _prepend_first_event(first_event, stream)
            finally:
                if not returning:
                    with contextlib.suppress(BaseException):
                        await stream.aclose()
        assert last_exc is not None
        raise last_exc


async def _prepend_first_event(
    first: dict[str, Any], stream: AsyncIterator[dict[str, Any]]
) -> AsyncIterator[dict[str, Any]]:
    """Yield ``first`` then the rest of ``stream``.

    Used by ``_open_stream_with_retry`` to consume the initial
    ``__anext__`` (where wool surfaces ``NoWorkersAvailable``) inside
    the retry loop, then expose the remaining events to the workflow
    consumer through a single uniform async-iterator interface.
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

    ``_run_workflow`` is supposed to catch every failure and route it to
    a terminal release; if anything escapes that contract (e.g. a future
    refactor leaves a bare ``raise`` outside the try), asyncio's
    default behavior is to log the unretrieved exception only at
    interpreter shutdown. Surfacing it through the project logger
    immediately makes the regression visible.
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("Workflow background task ended with unhandled exception: %r", exc)


# ``extract_identity`` moved to ``cfdb.workflows.keys`` — re-exported below
# for backward compatibility with existing importers in this module.
extract_identity = key_utils.extract_identity
