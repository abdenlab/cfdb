"""Worker discovery driven by the ECS control plane.

ECS already owns the lifecycle of every worker task — registration, IP,
status, health — so we use it as the discovery substrate directly rather
than maintaining a parallel registry. ``EcsDiscovery`` implements Wool's
``DiscoveryLike`` protocol with a poll-and-diff loop:

1. ``ecs.list_tasks`` enumerates every task in the cluster matching the
   worker task family with ``desiredStatus="RUNNING"``.
2. ``ecs.describe_tasks`` (batched 100 ARNs at a time) hydrates each
   into its full state, including ``healthStatus`` and ``attachments``.
3. We filter for ``lastStatus == "RUNNING"`` and (where reported)
   ``healthStatus == "HEALTHY"`` — tasks whose container definition
   omits a ``healthCheck`` are accepted as healthy so a deployment that
   hasn't yet wired up health checks still surfaces workers.
4. The poller diffs the resolved set against the previous one and
   publishes ``worker-added`` / ``worker-dropped`` events to a Wool
   ``DiscoveryPublisherLike``.

The worker side does nothing for discovery — no heartbeat thread, no
Mongo write, no registration RPC. ECS reports ``healthStatus: HEALTHY``
once the container's health check passes, which is the "ready to
dispatch" signal the discovery filters on. The worker task definition
MUST declare a ``healthCheck`` against the gRPC port; without one
ECS reports ``healthStatus: UNKNOWN`` indefinitely and the worker is
never advertised.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator, Iterable
from typing import Any, Optional

from wool.runtime.discovery.base import (
    Discovery,
    DiscoveryEvent,
    DiscoveryEventType,
    DiscoveryPublisherLike,
    DiscoverySubscriberLike,
    PredicateFunction,
    WorkerMetadata,
)

from cfdb.workflows.constants import DEFAULT_WORKER_PORT
from cfdb.workflows.provisioner import build_ecs_client

logger = logging.getLogger(__name__)

#: ECS ``DescribeTasks`` accepts up to 100 ARNs per call.
_DESCRIBE_BATCH_SIZE = 100

#: Default poll cadence. ``ListTasks`` and ``DescribeTasks`` are
#: subject to the ECS cluster-read API quota (see AWS Service Quotas
#: page "Rate of cluster resource read API calls"). 5s leaves ample
#: headroom for many concurrent clusters.
DEFAULT_POLL_INTERVAL_SECONDS = 5.0


class EcsDiscovery(Discovery):
    """Wool-compatible discovery service backed by ECS list/describe.

    Args:
        cluster: ECS cluster name or ARN to poll.
        task_definition_family: Worker task definition family (sans
            revision). Used as the ``family`` filter on ``ListTasks``.
        client: Optional pre-built boto3 ``ecs`` client. When omitted,
            one is constructed via :func:`build_ecs_client` with the
            ``endpoint_url`` / ``region_name`` kwargs threaded through.
        endpoint_url: Boto3 ``endpoint_url``. Passed to
            :func:`build_ecs_client` when ``client`` is omitted. The
            API lifespan threads its configured AWS endpoint here.
        region_name: Boto3 ``region_name``. Plumbed analogously.
        poll_interval: Seconds between successive ``ListTasks`` polls.
        worker_port: gRPC port the worker binds — used to construct
            the address string passed to Wool. Shares the worker_main
            default so a deployment that changes one changes both.
        version: Wool worker version string passed through into
            ``WorkerMetadata``. Free-form; useful for filtering.

    Lifecycle: enter ``async with EcsDiscovery(...)`` to start the
    background poller. Exiting the context cancels the poller and
    releases the publisher.
    """

    def __init__(
        self,
        *,
        cluster: str,
        task_definition_family: str,
        client: Optional[Any] = None,
        endpoint_url: Optional[str] = None,
        region_name: Optional[str] = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
        worker_port: int = DEFAULT_WORKER_PORT,
        version: str = "0",
    ) -> None:
        if not cluster:
            raise ValueError("EcsDiscovery requires a cluster name")
        if not task_definition_family:
            raise ValueError("EcsDiscovery requires a task_definition_family")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if client is not None and (endpoint_url or region_name):
            raise ValueError(
                "EcsDiscovery: pass either client or endpoint_url/region_name, "
                "not both — the boto kwargs are silently ignored when client is set"
            )

        self._cluster = cluster
        self._task_definition_family = task_definition_family
        #: Retained so ``__setstate__`` can rebuild the boto3 client on the
        #: worker targeting the same endpoint/region as the API process.
        #: Mirrors ``S3Cache._endpoint_url`` / ``S3Cache._region_name``.
        self._endpoint_url = endpoint_url
        self._region_name = region_name
        self._client = (
            client
            if client is not None
            else build_ecs_client(endpoint_url=endpoint_url, region_name=region_name)
        )
        self._poll_interval = poll_interval
        self._worker_port = worker_port
        self._version = version

        self._subscribers: list[_EcsSubscriber] = []
        self._known: dict[str, WorkerMetadata] = {}
        self._poll_task: Optional[asyncio.Task[None]] = None
        #: Guards ``_subscribers`` / ``_known`` / ``_closed`` and the
        #: sentinel-push fan-out during ``__aexit__``. Held only
        #: across in-memory mutations, never across AWS round-trips.
        self._state_lock = asyncio.Lock()
        #: Set inside ``__aexit__`` under ``_state_lock`` so late
        #: registrations (a subscriber whose ``_register_subscriber``
        #: was parked on the lock while ``__aexit__`` held it, or a
        #: fresh registration arriving after exit) receive the
        #: shutdown sentinel rather than hanging on ``queue.get()``
        #: forever.
        self._closed = False
        #: Serializes the entire ``poll_once`` body so two concurrent
        #: callers (test fixture + background loop, mostly) cannot
        #: interleave their list/describe snapshots into the diff and
        #: regress ``_known``. Separate from ``_state_lock`` so
        #: subscriber registration is not blocked on the AWS
        #: round-trip held by an in-flight poll.
        self._serialize_polls = asyncio.Lock()

    def __getstate__(self) -> dict[str, Any]:
        """Strip non-picklable runtime state for cloudpickle.

        ``EcsDiscovery`` is dragged across the cloudpickle boundary into
        the Wool worker process by ``WorkerProxy.__wool_reduce__`` (wool's
        internal reduce hook, which serializes the caller's ``discovery``;
        the proxy itself is reduced only via wool's own pickler, never a
        vanilla ``cloudpickle.dumps(proxy)``). The live boto3 ECS
        client holds an ``ssl.SSLContext`` via its urllib3 connection
        pools, which cloudpickle cannot serialize — the same problem
        ``S3Cache`` already solves for its boto3 ``s3`` client. We
        mirror it here: null ``_client`` (``__setstate__`` rebuilds it
        via :func:`build_ecs_client`) and also drop the loop-bound
        ``asyncio.Lock`` instances, the background ``_poll_task``, and
        the live subscriber/known set — none of which mean anything on
        the worker (it never polls or fans out events) and the locks
        are bound to the API's event loop. ``__setstate__`` recreates
        fresh transient state so the unpickled object is a valid,
        inert discovery handle.
        """
        state = self.__dict__.copy()
        state["_client"] = None
        # Loop-bound / live-runtime fields — never carry across the
        # boundary. Recreated fresh in ``__setstate__``. ``_closed`` is
        # stripped too, for symmetry with the other reset transients —
        # ``__setstate__`` unconditionally resets it to False, so a
        # stale True must not ride along in the pickled state.
        for transient in (
            "_state_lock",
            "_serialize_polls",
            "_poll_task",
            "_subscribers",
            "_known",
            "_closed",
        ):
            state.pop(transient, None)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restore state, rebuild the boto3 client, and reset runtime fields.

        Threads the originally-supplied ``endpoint_url`` / ``region_name``
        back into :func:`build_ecs_client` so the worker targets the same
        ECS control plane as the API. Transient fields stripped by
        ``__getstate__`` (locks, poll task, subscriber/known set) are
        recreated empty: the worker never enters the discovery context,
        so these stay inert, but they must exist for the object to be
        well-formed. The client rebuild is guarded on ``_client is None``
        (mirroring ``S3Cache``); the pickle protocol always nulls it, so
        the guard is a no-op there but keeps restoration idempotent.
        """
        self.__dict__.update(state)
        # Guard mirrors ``S3Cache.__setstate__``: rebuild only when the
        # client was stripped. Via the pickle protocol that is always the
        # case (``__getstate__`` nulls ``_client``), so this is the normal
        # path; the guard keeps ``__setstate__`` idempotent and avoids
        # clobbering a client a non-pickle caller may have already placed
        # in ``state``.
        if self._client is None:
            self._client = build_ecs_client(
                endpoint_url=self._endpoint_url,
                region_name=self._region_name,
            )
        self._subscribers = []
        self._known = {}
        self._poll_task = None
        self._closed = False
        self._state_lock = asyncio.Lock()
        self._serialize_polls = asyncio.Lock()

    async def __aenter__(self) -> "EcsDiscovery":
        # Re-entry would orphan the prior poll task and diff against
        # stale ``_known`` state. ``__aexit__`` nulls ``_poll_task``;
        # the assert pairs with that to make the misuse loud.
        assert self._poll_task is None, (
            "EcsDiscovery context entered twice; create a fresh instance"
        )
        # Run an immediate scan so subscribers attached right after
        # __aenter__ see the initial set of workers without waiting a
        # full poll interval.
        await self.poll_once()
        self._poll_task = asyncio.create_task(self._poll_loop())
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        # Wake any consumer parked on ``await self._queue.get()`` so
        # ``async for event in subscriber:`` exits cleanly rather than
        # blocking forever on a queue nothing will publish to again.
        # ``asyncio.shield`` keeps the cleanup body running even if
        # ``__aexit__`` itself is cancelled mid-await — without it, a
        # cancelled exit could skip the sentinel push and leave every
        # existing subscriber hanging on ``queue.get()``.
        await asyncio.shield(self._cleanup_subscribers())

    async def _cleanup_subscribers(self) -> None:
        """Push the shutdown sentinel into every subscriber's queue.

        The sentinel ``None`` is pushed to each subscriber's queue;
        ``_iter`` breaks out of its loop when it dequeues ``None``.
        ``_known`` is cleared so a stray re-subscribe after exit
        doesn't replay stale workers, and ``_closed`` is set so late
        registrations route via the sentinel path in
        ``_register_subscriber`` instead of appending.

        Snapshots the subscriber list under the state lock and pushes
        the sentinels *outside* the lock. ``Queue.put`` on an
        unbounded queue does not block in practice, but pushing under
        the lock would still couple the per-subscriber put to lock
        hold time and order — surfacing the snapshot-then-publish
        contract structurally avoids that coupling.
        """
        async with self._state_lock:
            self._closed = True
            subs = list(self._subscribers)
            self._known.clear()
            self._subscribers.clear()
        for sub in subs:
            await sub._queue.put(None)

    @property
    def publisher(self) -> DiscoveryPublisherLike:
        """``EcsDiscovery`` is read-only — workers register implicitly via ECS.

        Returns a guard-rail publisher that raises on ``publish``;
        production code should never reach for this property.
        """
        return _RaisingPublisher()

    @property
    def subscriber(self) -> DiscoverySubscriberLike:
        """A subscriber with no filter (sees every healthy worker)."""
        return self.subscribe()

    def subscribe(
        self, filter: Optional[PredicateFunction] = None
    ) -> DiscoverySubscriberLike:
        """Create a subscriber, optionally filtered by a worker predicate.

        The filter predicate runs inline on the dispatch path while the
        registration lock is held — keep it cheap and non-blocking.
        Slow predicates stall delivery to every other subscriber and
        block concurrent registrations.
        """
        return _EcsSubscriber(self, filter)

    async def poll_once(self) -> tuple[list[DiscoveryEvent], dict[str, WorkerMetadata]]:
        """Run one list/describe cycle and emit diff events.

        Returns the events emitted in this cycle and the resolved set of
        currently-known healthy workers (keyed by UUID hex string).
        Callers do not normally need to use the return value — it
        exists for tests so that polling can be exercised step-by-step
        without the background loop.

        ``self._serialize_polls`` serializes the entire body so two concurrent
        callers (test fixture + background loop) cannot interleave
        their list/describe snapshots into the diff and regress
        ``_known``. ``self._state_lock`` separately serializes the
        diff/mutate/dispatch tail against subscriber registration; see
        the inner ``async with self._state_lock:`` below and
        ``_register_subscriber`` for the matching critical section.

        On ``list_tasks`` / ``describe_tasks`` failure the previous
        ``_known`` snapshot is retained — workers added during a multi-
        cycle ECS outage are invisible until recovery, and workers
        terminated during the outage continue to be advertised as
        healthy until the next successful poll. Acceptable degradation
        next to the cold-start budget; preferable to flapping the whole
        fleet on a single transient failure.
        """
        async with self._serialize_polls:
            try:
                arns = await asyncio.to_thread(self._list_task_arns)
            except Exception:
                logger.exception(
                    "EcsDiscovery: list_tasks failed for cluster=%s",
                    self._cluster,
                )
                async with self._state_lock:
                    return [], dict(self._known)

            resolved: dict[str, WorkerMetadata] = {}
            if arns:
                try:
                    tasks = await asyncio.to_thread(self._describe_tasks_batched, arns)
                except Exception:
                    logger.exception(
                        "EcsDiscovery: describe_tasks failed for cluster=%s",
                        self._cluster,
                    )
                    async with self._state_lock:
                        return [], dict(self._known)
                for task in tasks:
                    metadata = self._task_to_metadata(task)
                    if metadata is not None:
                        resolved[str(metadata.uid)] = metadata

            # Diff, mutate, and dispatch under ``self._state_lock`` so a
            # concurrent ``_register_subscriber`` cannot replay a
            # snapshot that already contains a freshly-added worker AND
            # then receive the worker-added event for it from this
            # dispatch (duplicate delivery). Either the registration
            # runs entirely before this mutation (replay sees the OLD
            # known, this dispatch sees the new subscriber) or entirely
            # after (replay sees the NEW known, this dispatch's events
            # were already delivered).
            async with self._state_lock:
                events = list(_diff(self._known, resolved))
                self._known = resolved
                if events:
                    for sub in self._subscribers:
                        for event in events:
                            # A subscriber filter that raises must not
                            # poison delivery to siblings. ``_known``
                            # has already been mutated, so a re-throw
                            # here would permanently drop these events
                            # for every later subscriber.
                            try:
                                sub._publish(event)
                            except Exception:
                                logger.warning(
                                    "EcsDiscovery subscriber filter raised "
                                    "for event %s; dropping for that "
                                    "subscriber and continuing",
                                    event.type,
                                    exc_info=True,
                                )

            return events, dict(resolved)

    async def _poll_loop(self) -> None:
        """Background poll loop. Cancellation-safe."""
        while True:
            try:
                await asyncio.sleep(self._poll_interval)
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("EcsDiscovery poll loop iteration failed")

    def _list_task_arns(self) -> list[str]:
        """Page through ``ListTasks`` and return every task ARN."""
        arns: list[str] = []
        paginator_kwargs = {
            "cluster": self._cluster,
            "family": self._task_definition_family,
            "desiredStatus": "RUNNING",
        }
        next_token: Optional[str] = None
        while True:
            kwargs = dict(paginator_kwargs)
            if next_token:
                kwargs["nextToken"] = next_token
            response = self._client.list_tasks(**kwargs)
            arns.extend(response.get("taskArns") or [])
            next_token = response.get("nextToken")
            if not next_token:
                break
        return arns

    def _describe_tasks_batched(self, arns: list[str]) -> list[dict[str, Any]]:
        """Call ``DescribeTasks`` in batches of ``_DESCRIBE_BATCH_SIZE``."""
        out: list[dict[str, Any]] = []
        for i in range(0, len(arns), _DESCRIBE_BATCH_SIZE):
            batch = arns[i : i + _DESCRIBE_BATCH_SIZE]
            response = self._client.describe_tasks(
                cluster=self._cluster,
                tasks=batch,
            )
            out.extend(response.get("tasks") or [])
        return out

    def _task_to_metadata(self, task: dict[str, Any]) -> Optional[WorkerMetadata]:
        """Convert an ECS task description to a Wool ``WorkerMetadata``.

        Returns None when the task is not RUNNING + HEALTHY or we
        cannot extract a usable IP address. Worker task definitions
        MUST declare a ``healthCheck``; tasks without one surface as
        ``healthStatus: UNKNOWN`` and are filtered out, since their
        gRPC readiness is unknowable. Tasks whose ``healthStatus`` is
        absent from the describe-tasks response are also filtered —
        same reason.
        """
        if task.get("lastStatus") != "RUNNING":
            return None
        if task.get("healthStatus") != "HEALTHY":
            return None

        ip = _extract_eni_ip(task)
        if ip is None:
            return None

        # ECS task ARNs end in ``/<task-id>`` where ``<task-id>`` is a
        # 32-char hex string. We use it as a stable UUID for the
        # worker so successive polls produce identical metadata
        # (otherwise diff would emit add+drop on every cycle). A
        # missing ARN means we cannot identify the task — drop it
        # rather than collide every ARN-less task on the same UUID.
        task_arn = task.get("taskArn") or ""
        if not task_arn:
            return None
        task_id = task_arn.rsplit("/", 1)[-1]
        try:
            uid = uuid.UUID(task_id)
        except (ValueError, AttributeError):
            uid = uuid.uuid5(uuid.NAMESPACE_URL, task_arn)

        return WorkerMetadata(
            uid=uid,
            address=f"{ip}:{self._worker_port}",
            pid=0,
            version=self._version,
        )

    async def _register_subscriber(self, sub: "_EcsSubscriber") -> None:
        """Append a subscriber and prime its queue with the current snapshot.

        Both steps happen under ``self._state_lock``, paired with
        ``poll_once`` mutating ``_known`` and dispatching under the
        same lock: a concurrent poll either runs entirely before the
        registration (replay sees the OLD ``_known``, the poll's
        events go to the new subscriber via dispatch) or entirely
        after (replay sees the NEW ``_known``, no dispatch overlaps).
        No interleave produces a missed event or a duplicate ``worker-
        added`` for the same UID.

        Partial snapshots are possible if the consumer task is cancelled
        mid-replay: the subscriber is appended before the replay loop
        starts, so a cancel between append and end-of-replay delivers a
        truncated snapshot. From the consumer's perspective this is
        consistent with "iteration was cancelled mid-yield"; callers
        wrapping ``_iter`` in ``asyncio.shield`` should be aware that
        cancel-during-replay still drops events.
        """
        async with self._state_lock:
            if self._closed:
                # ``__aexit__`` already ran (or is currently running and
                # this registration was parked on the lock). Push the
                # shutdown sentinel so the consumer's ``_iter`` exits
                # cleanly rather than blocking on a queue nothing else
                # will publish to.
                await sub._queue.put(None)
                return
            self._subscribers.append(sub)
            for metadata in self._known.values():
                sub._publish(DiscoveryEvent("worker-added", metadata=metadata))

    async def _unregister_subscriber(self, sub: "_EcsSubscriber") -> None:
        """Remove ``sub`` from the subscriber list under the lock.

        Silently ignores already-removed subscribers so the iteration
        ``finally`` is idempotent.
        """
        async with self._state_lock:
            try:
                self._subscribers.remove(sub)
            except ValueError:
                pass


class _EcsSubscriber:
    """Discovery subscriber backed by an ``asyncio.Queue``.

    The discovery instance pushes events into the queue; consumers
    iterate via ``async for``. Subscribers are single-use — a second
    iteration on the same subscriber raises ``RuntimeError``;
    create a fresh subscriber via ``EcsDiscovery.subscribe()`` instead.
    """

    def __init__(
        self,
        owner: EcsDiscovery,
        filter: Optional[PredicateFunction],
    ) -> None:
        self._owner = owner
        self._filter = filter
        #: Discovery events, plus a single ``None`` sentinel that means
        #: shutdown; the sentinel is written only by
        #: ``EcsDiscovery.__aexit__`` to wake consumers parked on
        #: ``get()``.
        self._queue: asyncio.Queue[Optional[DiscoveryEvent]] = asyncio.Queue()
        self._exhausted = False

    def __aiter__(self) -> AsyncIterator[DiscoveryEvent]:
        return self._iter()

    def _publish(self, event: DiscoveryEvent) -> None:
        """Deliver ``event`` to the subscriber's queue, or drop it.

        Synchronous because the queue is unbounded and ``put_nowait``
        suffices — making the call ``await``-free removes a future-bug
        surface where a maintainer could otherwise inject a yield
        point into the dispatch loop and reorder events relative to
        subsequent state mutations under :data:`_state_lock`. The
        filter (if any) runs inline; events that don't pass it are
        dropped silently — the subscriber asked not to see this
        worker.
        """
        if self._filter is not None and not self._filter(event.metadata):
            return
        self._queue.put_nowait(event)

    async def _iter(self) -> AsyncIterator[DiscoveryEvent]:
        """Yield discovery events for the lifetime of this subscriber.

        Single-use: a second call after the iterator has been consumed
        raises ``RuntimeError``. ``EcsDiscovery.subscribe()`` returns
        a fresh subscriber per call, so multiple independent consumers
        are supported by creating multiple subscribers.

        Registers with the owning discovery on the first iteration so
        the snapshot of currently-known workers is replayed before
        any new poll events arrive. The ``finally`` always
        unregisters — ``_unregister_subscriber`` silently ignores
        already-removed entries, so a partial-registration failure
        does not leak a stale subscriber.

        ``self._exhausted`` flips synchronously (no ``await`` before
        line 513), so any subsequent ``__anext__()`` on a second
        iterator created from the same subscriber observes the flag
        and raises before re-registering. Covers the framework-
        introspection case where ``aiter()`` is called before any
        actual iteration.
        """
        if self._exhausted:
            raise RuntimeError(
                "EcsDiscovery subscriber already iterated; "
                "call EcsDiscovery.subscribe() for a fresh one"
            )
        self._exhausted = True
        try:
            await self._owner._register_subscriber(self)
            while True:
                event = await self._queue.get()
                if event is None:
                    # Sentinel from ``EcsDiscovery.__aexit__``; exit
                    # cleanly so the consumer's ``async for`` ends.
                    break
                yield event
        finally:
            await self._owner._unregister_subscriber(self)


class _RaisingPublisher:
    """Guard-rail publisher returned by ``EcsDiscovery.publisher``.

    ECS owns worker lifecycle, so nothing in cfdb has cause to call
    ``publish``; raising here surfaces the misuse loudly rather than
    silently no-op'ing.
    """

    #: Required by ``DiscoveryPublisherLike`` (wool >=0.9.2): the host a
    #: pool spawning workers for this discovery would bind them to. Purely
    #: declarative here — ``EcsDiscovery`` never spawns or publishes, so
    #: wool never reads it. ``0.0.0.0`` matches what the ECS worker
    #: entrypoint actually binds (``worker_main`` -> ``LocalWorker(host=
    #: "0.0.0.0")``), keeping the value truthful rather than arbitrary.
    bind_host: str = "0.0.0.0"

    async def publish(
        self, type: DiscoveryEventType, metadata: WorkerMetadata
    ) -> None:
        raise RuntimeError(
            "EcsDiscovery is read-only — workers register implicitly via ECS"
        )


def _extract_eni_ip(task: dict[str, Any]) -> Optional[str]:
    """Return the awsvpc private IPv4 from an ECS task description.

    ECS attaches one ENI per Fargate awsvpc task; its ``details`` list
    carries a ``privateIPv4Address`` entry. Older API responses use
    ``networkInterfaces`` on each container instead — we honor either.
    When both forms are populated the ``attachments`` value wins; for
    Fargate awsvpc both report the same IP, so the precedence is
    moot in practice.
    """
    for attachment in task.get("attachments") or []:
        if attachment.get("type") not in (None, "ElasticNetworkInterface"):
            continue
        for detail in attachment.get("details") or []:
            if detail.get("name") == "privateIPv4Address" and detail.get("value"):
                return detail["value"]
    for container in task.get("containers") or []:
        for nic in container.get("networkInterfaces") or []:
            ipv4 = nic.get("privateIpv4Address")
            if ipv4:
                return ipv4
    return None


def _diff(
    cached: dict[str, WorkerMetadata],
    discovered: dict[str, WorkerMetadata],
) -> Iterable[DiscoveryEvent]:
    """Yield Wool events describing the cached→discovered transition.

    The ``worker-updated`` branch below is dead code on ECS Fargate
    today: the UID is derived from the task ARN suffix (unique per
    task instance), and an ECS task's IP cannot change during its
    lifetime. The branch is preserved for parity with Wool's
    ``LocalDiscovery`` so the diff contract stays uniform across
    backends — a future ECS update that re-uses ARNs or supports
    IP migration would hit this path and we want the dispatch
    semantics to already be correct.
    """
    for uid, metadata in discovered.items():
        if uid not in cached:
            yield DiscoveryEvent("worker-added", metadata=metadata)
        elif cached[uid].address != metadata.address:
            # Protocol-parity guard: not reachable on ECS Fargate
            # today; see module-level note in _diff's docstring.
            yield DiscoveryEvent("worker-updated", metadata=metadata)
    for uid, metadata in cached.items():
        if uid not in discovered:
            yield DiscoveryEvent("worker-dropped", metadata=metadata)
