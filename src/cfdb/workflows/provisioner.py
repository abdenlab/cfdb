"""ECS on-demand worker provisioning via the ``RunTask`` API.

``EcsProvisioner`` wraps a single boto3 ``RunTask`` call with the cluster,
task-definition family, and awsvpc networking configuration the workflow
subsystem needs. The same code targets LocalStack in development and real
AWS in production — only ``AWS_ENDPOINT_URL`` differs at the boto3 client.

Concurrent ``request`` calls sharing the same ``dedup_key`` (typically
the workflow key) attach to a single in-flight ``RunTask`` task and
observe the same outcome. ``asyncio.shield`` insulates each caller's
cancellation from the others, so a request abandoned by one caller does
not poison the result for the rest — and again around the dedup-slot
cleanup so cancellation cannot leak a stale entry.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
import time
from collections.abc import Iterable
from typing import Any, Literal, Optional, get_args

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)


#: Acceptable values for ``RunTask`` ``assignPublicIp`` — ECS rejects
#: anything else. The runtime-validation set is derived from the
#: ``Literal`` so the two definitions can't drift.
AssignPublicIp = Literal["ENABLED", "DISABLED"]
_ASSIGN_PUBLIC_IP_VALUES = frozenset(get_args(AssignPublicIp))


#: How long a just-issued ``RunTask`` is counted as an in-flight worker
#: launch on top of what ``list_tasks`` reports. ECS ``list_tasks`` is
#: eventually consistent — a freshly launched task is not visible for a few
#: seconds — so a simultaneous burst would otherwise see a stale count and
#: every spawn would slip under the cap. Counting recent launches for this
#: window closes that race; it MUST exceed the real ``list_tasks`` visibility
#: lag (err high — over-counting is conservative, only briefly delaying a
#: spawn, whereas under-counting lets the fleet overshoot the cap).
_LAUNCH_VISIBILITY_WINDOW_S = 60.0


class _SubmittedRunTask:
    """Bookkeeping slot for an in-flight ``RunTask`` future.

    Tracks the underlying ``concurrent.futures.Future`` and whether the
    awaiting coroutine successfully consumed the result so a late-
    arriving boto thread can be classified as orphan (caller cancelled,
    ARN unreachable) vs claimed (caller observed the ARN normally).
    """

    __slots__ = ("future", "claimed")

    def __init__(self) -> None:
        self.future: Optional[concurrent.futures.Future[dict[str, Any]]] = None
        self.claimed: bool = False


class RetryableProvisionerError(RuntimeError):
    """Provisioner failure the caller should resubmit later.

    Covers both ECS-side capacity / ENI exhaustion and transport-level
    transients (connection timeouts, endpoint unavailability,
    throttling). The executor's response is identical for both — mark
    the workflow ``FAILED`` with a retryable error string — so they
    share one exception type.
    """


class EcsProvisioner:
    """Launch ephemeral worker tasks on ECS Fargate.

    ``_in_flight`` has two cleanup paths: ``_run_task_owned``'s
    ``finally`` pops the slot under normal completion; ``request``'s
    self-heal pops a stale slot whose owning task is already ``done``
    when the next ``request`` for the same key arrives. The self-heal
    is the safety net for "the finally pop was skipped" cases —
    cancellation during event-loop teardown can re-raise out of the
    awaited shielded pop before the inner coroutine runs.

    Args:
        cluster: ECS cluster name or ARN.
        task_definition: Task definition family (or family:revision)
            for the worker container.
        subnets: Awsvpc subnet IDs to place worker ENIs into.
        security_groups: Awsvpc security group IDs to attach.
        assign_public_ip: ``"ENABLED"`` or ``"DISABLED"`` — whether the
            ENI gets a public IP. Production should usually leave this
            disabled and rely on VPC endpoints; LocalStack accepts
            either value.
        client: Optional pre-built boto3 ``ecs`` client. When omitted,
            one is constructed via :func:`build_ecs_client` with the
            ``endpoint_url`` / ``region_name`` kwargs threaded through.
        endpoint_url: Boto3 ``endpoint_url``. Passed to
            :func:`build_ecs_client` when ``client`` is omitted. The
            lifespan plumbs :data:`cfdb.api.AWS_ENDPOINT_URL` here.
        region_name: Boto3 ``region_name``. Plumbed analogously.
        max_in_flight: Soft cap on concurrent ``RunTask`` calls. ECS's
            ``RunTask`` API is rate-limited to ~20 req/s per account;
            this guard keeps us well under it.
        task_family: Task-definition family used to count the current
            worker fleet via ``list_tasks`` for the ``max_workers`` cap.
            Defaults to ``task_definition`` with any ``:revision`` suffix
            stripped.
        max_workers: Cap on concurrently-running worker tasks. Before each
            ``RunTask`` the provisioner counts the ECS-visible fleet plus its
            own recently-issued launches (which ``list_tasks`` may not
            reflect yet) under a lock, and skips the spawn when already at
            this cap, so the worker fleet is bounded while excess jobs stay
            queued (the durable scheduler dispatches them as workers free up
            — no shedding). ``0`` disables the cap (rely on the Fargate vCPU
            quota). Counting in-flight launches is what bounds a simultaneous
            cold-start burst; counting only ``list_tasks`` lets a concurrent
            burst see a stale count and every spawn slip under the cap.
            Bounded per API task: a single API task (the default) is held to
            the cap; a multi-task API would each track only its own launches
            and need a shared lease to bound the total.
    """

    def __init__(
        self,
        *,
        cluster: str,
        task_definition: str,
        subnets: Iterable[str],
        security_groups: Iterable[str] = (),
        assign_public_ip: AssignPublicIp = "DISABLED",
        client: Optional[Any] = None,
        endpoint_url: Optional[str] = None,
        region_name: Optional[str] = None,
        max_in_flight: int = 16,
        task_family: Optional[str] = None,
        max_workers: int = 0,
    ) -> None:
        if not cluster:
            raise ValueError("EcsProvisioner requires a cluster name")
        if not task_definition:
            raise ValueError("EcsProvisioner requires a task_definition")
        if max_workers < 0:
            raise ValueError(f"max_workers must be >= 0; got {max_workers}")
        subnet_list = list(subnets)
        if not subnet_list:
            raise ValueError("EcsProvisioner requires at least one subnet")
        if assign_public_ip not in _ASSIGN_PUBLIC_IP_VALUES:
            raise ValueError(
                f"assign_public_ip must be one of {sorted(_ASSIGN_PUBLIC_IP_VALUES)}; "
                f"got {assign_public_ip!r}"
            )
        if client is not None and (endpoint_url or region_name):
            raise ValueError(
                "EcsProvisioner: pass either client or endpoint_url/region_name, "
                "not both — the boto kwargs are silently ignored when client is set"
            )

        self._cluster = cluster
        self._task_definition = task_definition
        # Family used to count the running fleet via ``list_tasks``; strip
        # any ``:revision`` so a pinned task-def revision still matches.
        self._task_family = task_family or task_definition.split(":", 1)[0]
        self._max_workers = max_workers
        self._subnets = subnet_list
        self._security_groups = list(security_groups)
        self._assign_public_ip = assign_public_ip
        self._client = (
            client
            if client is not None
            else build_ecs_client(endpoint_url=endpoint_url, region_name=region_name)
        )
        self._semaphore = asyncio.Semaphore(max_in_flight)
        # Concurrent ``request`` calls sharing a key attach to the
        # same in-flight Task. ``_run_task_owned``'s ``finally`` block
        # always pops the entry on completion (success or failure) so
        # a fresh request after the previous one finishes spawns a new
        # launch.
        self._in_flight: dict[str, asyncio.Task[list[str]]] = {}
        self._in_flight_lock = asyncio.Lock()
        # Owned threadpool for ``RunTask`` so ``aclose`` can drain
        # in-flight boto calls deterministically. ``asyncio.to_thread``
        # submits to the loop's default executor, which we cannot
        # drain on shutdown — a cancelled awaiter would leave the
        # boto thread untracked and any ARNs it produced as silent
        # orphans. Tracking the underlying ``concurrent.futures.Future``
        # lets the done-callback observe late-arriving ARNs and log
        # them at WARNING for an external reaper.
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_in_flight,
            thread_name_prefix="ecs-runtask",
        )
        self._pending_lock = threading.Lock()
        self._pending: set[_SubmittedRunTask] = set()
        # Worker-fleet cap accounting. ``_recent_launches`` holds the
        # monotonic timestamps of RunTask launches this provisioner issued
        # but that ``list_tasks`` may not reflect yet; the cap counts them on
        # top of the ECS-visible fleet so a concurrent burst cannot all slip
        # under a stale count. ``_cap_lock`` serializes the count-and-reserve
        # so concurrent deciders observe each other's reservations.
        self._cap_lock = asyncio.Lock()
        self._recent_launches: list[float] = []

    async def request(self, *, dedup_key: str) -> list[str]:
        """Launch a worker task, returning its ARN(s).

        Concurrent callers sharing ``dedup_key`` share one ``RunTask``:
        only the first launches; the rest await the same result.

        Args:
            dedup_key: Typically the workflow mutex key. Two callers
                holding the same workflow-level mutex should not
                independently launch two workers.

        Returns:
            List of task ARNs corresponding to the launched worker(s).

        Raises:
            RetryableProvisionerError: Capacity / ENI exhaustion,
                throttling, or connection-level transients. Callers
                should mark the workflow as failed with a retryable
                status so the client can resubmit.
        """
        if not dedup_key:
            raise ValueError("EcsProvisioner.request requires a non-empty dedup_key")

        async with self._in_flight_lock:
            existing = self._in_flight.get(dedup_key)
            # Self-heal: if a previous task finished but its dedup-slot
            # release was skipped (e.g. event-loop teardown re-raised
            # ``CancelledError`` out of the awaited shielded pop
            # before the inner coroutine ran), treat the stale entry
            # as absent and launch a fresh task. Collapses every
            # "the finally pop never ran" race into one self-healing
            # read at registration time.
            if existing is not None and existing.done():
                existing = None
                self._in_flight.pop(dedup_key, None)
            if existing is None:
                existing = asyncio.create_task(
                    self._run_task_owned(dedup_key),
                    name=f"ecs-runtask-{dedup_key}",
                )
                # Retrieve the eventual exception/result so a Task
                # abandoned by every caller (universal cancellation)
                # doesn't trigger asyncio's "Task exception was never
                # retrieved" warning when it's garbage-collected.
                existing.add_done_callback(_consume_task_outcome)
                self._in_flight[dedup_key] = existing

        # Shield protects concurrent callers sharing this dedup_key
        # from each other's cancellation: if one caller is cancelled
        # mid-await, only that caller sees CancelledError; the
        # underlying task continues and other callers still observe
        # the real result.
        return await asyncio.shield(existing)

    async def _run_task_owned(self, dedup_key: str) -> list[str]:
        """Body of the dedup-protected RunTask call.

        Split out from ``request`` so the dedup-registration lock is
        not held across the boto3 thread-pool round-trip. The
        ``finally`` always pops the in-flight slot so the next
        ``request`` for the same key can launch a fresh worker. The
        pop is shielded so a CancelledError delivered to the task
        itself (event-loop teardown, explicit cancel) cannot skip the
        cleanup and leak a stale dedup entry pointing at a cancelled
        task.

        The release captures ``my_task`` and only pops the slot if the
        current map entry is still its own task. Today the self-heal in
        ``request`` would already discard a stale entry pointing at a
        cancelled task, but the identity check makes the invariant
        local — a future maintainer who inserts into ``_in_flight``
        directly cannot have their entry nuked by a late release.
        """
        my_task = asyncio.current_task()
        async def _release_dedup_slot() -> None:
            async with self._in_flight_lock:
                if self._in_flight.get(dedup_key) is my_task:
                    self._in_flight.pop(dedup_key, None)

        try:
            # Worker-fleet cap: reserve a slot under the cap before launching.
            # When the fleet is full, return an empty ARN list rather than
            # raise — the caller (``_handle_overflow``) reschedules the job,
            # so it stays queued and runs when an existing worker frees up.
            # This bounds the worker-container count while preserving the
            # queue (the admission ceiling, not this cap, bounds the queue).
            token: Optional[float] = None
            if self._max_workers > 0:
                token = await self._reserve_worker_slot(dedup_key)
                if token is None:
                    return []
            try:
                async with self._semaphore:
                    return await self._run_task()
            except BaseException:
                # The launch did not produce a worker; release the reserved
                # slot so the in-flight count does not over-count a spawn
                # that never happened.
                if token is not None:
                    await self._release_worker_slot(token)
                raise
        finally:
            await asyncio.shield(_release_dedup_slot())

    async def _reserve_worker_slot(self, dedup_key: str) -> Optional[float]:
        """Reserve a worker slot under the cap, or return None when full.

        Counts the ECS-visible fleet PLUS this provisioner's own recent
        launches (``_recent_launches``) that ``list_tasks`` may not reflect
        yet, all under ``_cap_lock`` so concurrent burst deciders observe
        each other's reservations. This is what bounds a simultaneous
        cold-start burst: counting only ``list_tasks`` lets every decider see
        a stale ~0 and all spawn. Returns the launch token (a monotonic
        timestamp) on success, or None when already at the cap.
        """
        async with self._cap_lock:
            now = time.monotonic()
            self._recent_launches = [
                t
                for t in self._recent_launches
                if now - t < _LAUNCH_VISIBILITY_WINDOW_S
            ]
            ecs_visible = await self._current_worker_count()
            effective = ecs_visible + len(self._recent_launches)
            if effective >= self._max_workers:
                logger.info(
                    "Worker fleet at capacity (%d running + %d in-flight "
                    ">= %d); not spawning for %s — job stays queued until a "
                    "worker frees",
                    ecs_visible,
                    len(self._recent_launches),
                    self._max_workers,
                    dedup_key,
                )
                return None
            self._recent_launches.append(now)
            return now

    async def _release_worker_slot(self, token: float) -> None:
        """Drop a reservation whose RunTask did not produce a worker."""
        async with self._cap_lock:
            try:
                self._recent_launches.remove(token)
            except ValueError:
                pass  # already aged out of the visibility window

    async def _current_worker_count(self) -> int:
        """Count worker tasks whose desired status is RUNNING.

        ``list_tasks`` with ``desiredStatus="RUNNING"`` returns tasks still
        starting (PROVISIONING / PENDING / ACTIVATING) as well as those
        already running, so the count reflects the workers that exist or are
        coming up — the right denominator for the fleet cap. A ``list_tasks``
        failure raises :class:`RetryableProvisionerError` so the caller
        queues the job and retries on the next tick rather than spawning
        blind past the cap.

        Read-only and cheap, so it runs on the default executor rather than
        the owned RunTask pool — ``aclose``'s deterministic drain only needs
        to cover the billable RunTask launches. The cap is small (well under
        the first ``list_tasks`` page of 100), so the first page is the whole
        fleet and pagination is unnecessary to decide whether it is reached.
        """
        try:
            response = await asyncio.to_thread(
                self._client.list_tasks,
                cluster=self._cluster,
                family=self._task_family,
                desiredStatus="RUNNING",
            )
        except (ClientError, BotoCoreError) as exc:
            raise RetryableProvisionerError(
                f"list_tasks failed: {type(exc).__name__}: {exc}"
            ) from exc
        return len(response.get("taskArns") or [])

    async def _run_task(self) -> list[str]:
        """Single ``RunTask`` invocation translated to a list of task ARNs.

        ``count`` is hardcoded to 1 because the dedup contract is one
        worker per workflow key: two concurrent ``request(dedup_key=K)``
        calls share the same task, and once that task finishes a fresh
        request for the same key launches a fresh worker.

        Three response shapes from ECS:

        * ``failures`` only — no task launched; raise
          :class:`RetryableProvisionerError` for retryable reasons,
          :class:`RuntimeError` otherwise.
        * ``tasks`` only — happy path; return the ARN list.
        * ``tasks`` + ``failures`` together (rare; ECS occasionally
          surfaces a secondary placement warning alongside a
          successfully launched task) — log the failures at WARNING
          and return the ARNs rather than discard a billable worker.
        """
        kwargs: dict[str, Any] = {
            "cluster": self._cluster,
            "taskDefinition": self._task_definition,
            "launchType": "FARGATE",
            "count": 1,
            "networkConfiguration": {
                "awsvpcConfiguration": {
                    "subnets": list(self._subnets),
                    "securityGroups": list(self._security_groups),
                    "assignPublicIp": self._assign_public_ip,
                }
            },
        }

        # Capacity / ENI exhaustion / throttling / boto transport
        # transients; see the class docstring for the ``capacity:``
        # prefix contract callers parse off the persisted error.
        try:
            response = await self._submit_run_task(kwargs)
        except (ClientError, BotoCoreError) as exc:
            if _is_retryable_error(exc):
                raise RetryableProvisionerError(f"{type(exc).__name__}: {exc}") from exc
            raise

        failures = response.get("failures") or []
        tasks = response.get("tasks") or []
        arns = [t["taskArn"] for t in tasks if t.get("taskArn")]

        if failures and not arns:
            # No ARN to fall back on — the launch did not happen.
            reasons = ", ".join(f.get("reason", "?") for f in failures)
            if any(_is_retryable_failure(f) for f in failures):
                raise RetryableProvisionerError(f"ECS RunTask failures: {reasons}")
            raise RuntimeError(f"ECS RunTask failures: {reasons}")

        if failures and arns:
            # Partial-success edge: ECS occasionally surfaces a
            # secondary placement warning alongside a successfully
            # launched task. Log and keep the ARN rather than
            # discarding a worker that's already running (and
            # billing) to chase the warning.
            reasons = ", ".join(f.get("reason", "?") for f in failures)
            logger.warning(
                "ECS RunTask succeeded with %d task(s) but reported failures: %s",
                len(arns),
                reasons,
            )

        if not arns:
            # ECS returned neither a launched task nor a failure entry.
            # Treat as a retryable transient — the caller has nothing
            # to dispatch to and the executor's failed-with-retryable
            # path is the right response.
            raise RetryableProvisionerError("ECS RunTask returned no tasks and no failures")
        logger.info(
            "ECS RunTask launched %d task(s) on cluster=%s family=%s",
            len(arns),
            self._cluster,
            self._task_definition,
        )
        return arns

    async def _submit_run_task(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        """Run ``RunTask`` in the owned executor and track the future.

        The future is recorded in ``_pending`` so ``aclose`` can drain
        threads mid-flight. A done-callback logs any ARNs the boto
        thread produced *after* the awaiting coroutine was cancelled —
        these are orphan workers that no caller can claim. Left unused,
        they self-reap once continuously idle beyond
        ``CFDB_WORKER_IDLE_TIMEOUT_SECONDS`` (minutes), with the
        ``CFDB_WORKER_MAX_LIFETIME_SECONDS`` ceiling as the outer
        bound. Surfacing them at WARNING still gives operators a
        chance to reap manually first.
        """
        slot = _SubmittedRunTask()
        future = self._executor.submit(self._client.run_task, **kwargs)
        slot.future = future
        loop = asyncio.get_running_loop()
        with self._pending_lock:
            self._pending.add(slot)
        future.add_done_callback(
            lambda f, _slot=slot, _loop=loop: self._on_runtask_done(_slot, _loop)
        )
        try:
            result = await asyncio.wrap_future(future)
        except BaseException:
            # Awaiter cancelled or wrap_future surfaced a failure; the
            # done-callback owns the orphan check. Slot stays in
            # ``_pending`` until the callback fires.
            raise
        slot.claimed = True
        return result

    def _on_runtask_done(
        self, slot: _SubmittedRunTask, loop: asyncio.AbstractEventLoop
    ) -> None:
        """Schedule the orphan check on the loop thread.

        ``add_done_callback`` fires from the boto worker thread (or
        synchronously from the submitter if the future is already done).
        Defer the check via ``call_soon_threadsafe`` so the awaiting
        coroutine has a chance to flip ``slot.claimed`` first — without
        the deferral every successful launch would race-classify as
        orphan because the callback runs before the wrap_future
        resumption.
        """
        try:
            loop.call_soon_threadsafe(self._check_orphan, slot)
        except RuntimeError:
            # Loop already closed (interpreter shutdown); inline the
            # check so the WARNING still surfaces.
            self._check_orphan(slot)

    def _check_orphan(self, slot: _SubmittedRunTask) -> None:
        with self._pending_lock:
            self._pending.discard(slot)
        future = slot.future
        if future is None or slot.claimed:
            return
        if future.cancelled() or future.exception() is not None:
            return
        response = future.result()
        tasks = response.get("tasks") or []
        arns = [t["taskArn"] for t in tasks if t.get("taskArn")]
        if arns:
            logger.warning(
                "ECS RunTask launched %d task(s) after the awaiting "
                "caller was cancelled; ARN(s) %s are orphaned. The "
                "worker's max-lifetime ceiling caps the cost-leak "
                "window; reap manually for faster reclaim.",
                len(arns),
                arns,
            )

    async def aclose(self) -> None:
        """Cancel every in-flight ``RunTask`` and drain the executor.

        Called from the API lifespan's ``finally`` so a shutdown while
        a ``RunTask`` round-trip is mid-flight doesn't leave the task
        unrequested-but-billed. ``gather(return_exceptions=True)``
        absorbs the ``CancelledError`` and any ``RetryableProvisionerError``
        already in flight. The done-callback that ``request`` attaches
        separately suppresses "Task exception was never retrieved"
        warnings for tasks that complete without an awaiter; ``aclose``
        itself awaits everything it cancels.

        After cancelling the awaiting tasks, drain the owned
        ``ThreadPoolExecutor`` via ``shutdown(wait=True)`` so any boto
        thread still on the wire finishes before the client tears down.
        ``shutdown`` runs in a worker thread so the event loop is not
        held; orphan ARNs land via the done-callback's WARNING.
        """
        async with self._in_flight_lock:
            tasks = list(self._in_flight.values())
            # Clearing inside the lock makes any concurrent ``request``
            # arriving after ``aclose`` get a fresh Task rather than
            # attach to a doomed in-flight.
            self._in_flight.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.to_thread(self._executor.shutdown, wait=True)


def build_ecs_client(
    *, endpoint_url: Optional[str] = None, region_name: Optional[str] = None
) -> Any:
    """Construct a boto3 ``ecs`` client with explicit endpoint/region.

    The caller (typically :class:`EcsProvisioner`, :class:`EcsDiscovery`,
    or the API lifespan) is the single source of truth for
    ``endpoint_url`` and ``region_name``; this function does not
    consult :mod:`cfdb.api` for fallback values. Leave both ``None``
    to let boto3's default session resolver chain pick them up from
    the environment.
    """
    return boto3.client(
        "ecs",
        endpoint_url=endpoint_url,
        region_name=region_name,
    )


# Codes ECS returns as ``ClientError.response["Error"]["Code"]`` for a
# ``RunTask`` failure that a retry can plausibly fix. Capacity-style
# placement reasons (``RESOURCE:ENI`` / ``RESOURCE:CPU`` /
# ``RESOURCE:MEMORY`` / ``AWS.ECS.PlacementError``) are NOT in this
# set because ECS surfaces them via ``failures[].reason``, not as a
# top-level error code; ``_RETRYABLE_REASON_TOKENS`` catches them on
# the failure-payload path.
_RETRYABLE_ERROR_CODES = frozenset({
    "Capacity",
    "CapacityProviderException",
    "ClusterCapacityProviderException",
    "ThrottlingException",
    "Throttling",
    "RequestLimitExceeded",
    "ServerException",
    "ServiceUnavailableException",
    "ServiceUnavailable",
})
# Substring match (``token in reason``). ``THROTTL`` is deliberately
# truncated so it covers any throttling-derived reason — ``THROTTLED``,
# ``THROTTLING``, ``THROTTLINGEXCEPTION`` — without enumerating every
# variant AWS might emit.
_RETRYABLE_REASON_TOKENS = ("CAPACITY", "RESOURCE:", "THROTTL")


def _is_retryable_error(exc: BaseException) -> bool:
    """Return True when an ECS exception is a retryable transient.

    Handles both ``ClientError`` (with a structured response dict) and
    ``BotoCoreError`` subclasses (transport-level failures without a
    response) — connection timeouts, endpoint unavailability, and
    other transport transients are exactly the cases the caller should
    resubmit.
    """
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = (response.get("Error") or {}).get("Code")
        if code in _RETRYABLE_ERROR_CODES:
            return True
    # BotoCoreError subclasses (EndpointConnectionError,
    # ConnectTimeoutError, ReadTimeoutError, HTTPClientError, etc.)
    # carry no .response; classify the whole family as retryable.
    return isinstance(exc, BotoCoreError)


def _is_retryable_failure(failure: dict[str, Any]) -> bool:
    """Return True when a ``RunTask`` ``failures`` entry is retryable."""
    reason = (failure.get("reason") or "").upper()
    return any(token in reason for token in _RETRYABLE_REASON_TOKENS)


def _consume_task_outcome(task: asyncio.Task[Any]) -> None:
    """Mark a Task's outcome retrieved so asyncio doesn't warn.

    Attached as a done-callback on the in-flight provisioner Task so
    universal cancellation (every caller cancels before the Task
    completes) doesn't surface as a noisy "Task exception was never
    retrieved" warning at GC time. The outcome itself isn't logged
    here — ``_run_task`` already emits a success line on launch and
    the caller surfaces failures via the awaiting coroutine.
    """
    if task.cancelled():
        return
    task.exception()
