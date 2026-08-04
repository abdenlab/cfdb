"""Priority (leaky-bucket) load balancer for the wool worker pool (issue #45).

Unlike wool's round-robin balancer, this one always offers a task to the
discovered workers in the same stable order (sorted by ``WorkerMetadata.uid``)
on every dispatch. The order is arbitrary but reproducible — ``uid`` is a
per-worker UUID, not a seniority or cost ranking; what matters is only that
the same workers are offered work first on each dispatch. Combined with
per-worker backpressure (one task per worker), load concentrates on the
lowest-ordered workers and higher-ordered workers drain to idle — so an
over-provisioned fleet sheds idle capacity (workers self-terminate on their
max-lifetime) instead of every worker carrying a thin, perpetual slice of
traffic. This is the "leaky bucket": fill the priority workers first and let
the overflow spill to the next.

Routing policy only. Under :class:`wool.LoadBalancerLike` the balancer is an
async generator that *selects* worker uids; ``WorkerProxy`` owns the dispatch
loop, the transient/non-transient error classification, and eviction. So a
backpressure ``RESOURCE_EXHAUSTED`` rejection reaches this module as a plain
``athrow`` — indistinguishable from any other dispatch failure, and correctly
so, because the proxy has already decided whether the worker survives it. The
generator is driven by three signals: ``anext`` requests the next candidate,
``athrow`` reports that the previous candidate's dispatch failed, and a resume
value reports the outcome — a non-``None`` uid means success (the generator
MUST then terminate) while ``None`` means the candidate had left the pool and
was skipped, which is not a success and must advance to the next candidate.
When the generator ends the proxy raises ``NoWorkersAvailable``; the executor
treats that as the signal to add a worker and re-queue the job.

Conformance to :class:`wool.LoadBalancerLike` is structural — this class
deliberately does NOT inherit the protocol, mirroring wool's own
``RoundRobinLoadBalancer``. Subclassing a ``@runtime_checkable`` Protocol is an
active hazard: when the protocol gains a method you silently inherit a ``...``
stub that satisfies ``isinstance`` while returning ``None`` at runtime, which
is exactly how the 0.9.x ``dispatch`` contract broke on the upgrade to the
delegate protocol (issue #84).

Stateless by design: the stable order is recomputed per call, so there is no
per-context index, no lock, and nothing that resists pickling.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING
from typing import AsyncGenerator

from wool import LoadBalancerContextView

if TYPE_CHECKING:
    from wool.runtime.routine.task import Task

logger = logging.getLogger(__name__)


class PriorityLoadBalancer:
    """Offer each task to workers in a stable priority order.

    See the module docstring for the leaky-bucket rationale and the
    delegate-generator contract this honors.
    """

    def __reduce__(self):
        # Stateless — reconstruct via the bare constructor so the balancer
        # is trivially picklable (mirrors RoundRobinLoadBalancer, which
        # uses __reduce__ to shed its lock/index state).
        return (self.__class__, ())

    async def delegate(
        self,
        task: Task,
        *,
        context: LoadBalancerContextView,
    ) -> AsyncGenerator[uuid.UUID, uuid.UUID | None]:
        """Yield worker candidates in stable priority order.

        :param task:
            The :class:`wool.Task` being routed. Accepted for protocol
            conformance and ignored: priority order is task-agnostic.
        :param context:
            Read-only view of the worker pool. Only ``context.workers`` is
            read — eviction is the proxy's responsibility.
        :yields:
            Worker uids, lowest ``str(uid)`` first. The generator is driven
            by the proxy via ``anext``/``athrow``/``asend``.
        """
        # ``workers`` is a live view (see LoadBalancerContextView), so
        # re-reading the property per candidate would buy nothing.
        workers = context.workers
        # Snapshot a stable, deterministic order up front. Sorting by uid
        # makes "priority" reproducible across dispatches, so the same
        # low-order workers stay saturated. ``str(uid)`` guarantees an
        # orderable key regardless of the uid's concrete type. Iterating a
        # snapshot keeps the pass well-defined even as the proxy evicts
        # workers from the live map mid-loop.
        for candidate in sorted(workers, key=str):
            if candidate not in workers:
                # Left the pool since the snapshot was taken. The proxy would
                # skip it anyway; not yielding saves the round trip.
                continue
            try:
                resumed = yield candidate
            except Exception as exc:
                # The proxy reported this candidate's dispatch failed and has
                # already classified it — evicting the worker on a
                # non-transient error, leaving it in the pool on a transient
                # one. Either way the balancer's only move is to advance.
                # Note this catches Exception, not BaseException, so
                # GeneratorExit and CancelledError still propagate and
                # aclose()/cancellation keep working.
                logger.debug("Advancing past worker %s: %s", candidate, exc)
                continue
            if resumed is None:
                # The candidate left the pool between the yield and the
                # proxy's resolution, so it was skipped rather than
                # dispatched to. Advance — treating this as a success would
                # silently drop the task.
                continue
            # Non-None resume: the dispatch succeeded. The protocol requires
            # the generator to terminate here; yielding again is a violation
            # the proxy raises RuntimeError on.
            return
