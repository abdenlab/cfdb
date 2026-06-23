"""Priority (leaky-bucket) load balancer for the wool worker pool (issue #45).

Unlike wool's round-robin balancer, this one always offers a task to the
discovered workers in the same stable order (sorted by
``WorkerMetadata.uid``) on every dispatch. The order is arbitrary but
reproducible — ``uid`` is a per-worker UUID, not a seniority or cost
ranking; what matters is only that the same workers are offered work first
on each dispatch. Combined with per-worker backpressure (one task per
worker), load concentrates on the lowest-ordered workers and
higher-ordered workers drain to idle — so an over-provisioned fleet sheds
idle capacity (workers self-terminate on their max-lifetime) instead of
every worker carrying a thin, perpetual slice of traffic. This is the
"leaky bucket": fill the priority workers first and let the overflow spill
to the next.

It honors wool's load-balancer worker-health contract (see
:class:`wool.LoadBalancerLike`): rotate to the next worker on
``TransientRpcError`` — which includes a backpressure ``RESOURCE_EXHAUSTED``
rejection — evict the worker on a non-transient ``RpcError``, and let any
other exception propagate untouched. When no worker accepts the task (the
pool is empty, or every worker rejected or was evicted in one pass) it
raises ``NoWorkersAvailable``; the executor treats that as the signal to
add a worker and re-queue the job.

Stateless by design: the stable order is recomputed per call, so there is
no per-context index, no lock, and nothing that resists pickling.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from typing import Any
from typing import AsyncIterator

from wool import LoadBalancerContextLike
from wool import LoadBalancerLike
from wool import NoWorkersAvailable
from wool import RpcError
from wool import TransientRpcError

if TYPE_CHECKING:
    from wool.runtime.routine.task import Task

logger = logging.getLogger(__name__)


class PriorityLoadBalancer(LoadBalancerLike):
    """Offer each task to workers in a stable priority order.

    See the module docstring for the leaky-bucket rationale and the
    worker-health contract this honors.
    """

    def __reduce__(self):
        # Stateless — reconstruct via the bare constructor so the balancer
        # is trivially picklable (mirrors RoundRobinLoadBalancer, which
        # uses __reduce__ to shed its lock/index state).
        return (self.__class__, ())

    async def dispatch(
        self,
        task: Task,
        *,
        context: LoadBalancerContextLike,
        timeout: float | None = None,
    ) -> AsyncIterator[Any]:
        """Dispatch *task* to the first worker, in priority order, that accepts.

        :param task:
            The :class:`wool.Task` to dispatch.
        :param context:
            The :class:`wool.LoadBalancerContextLike` whose ``workers`` map
            names the candidates and through which ``remove_worker`` evicts
            unhealthy peers.
        :param timeout:
            Per-attempt timeout in seconds against a single worker.
        :returns:
            The worker's response stream (an async iterator).
        :raises NoWorkersAvailable:
            When the pool is empty or no worker accepts the task across one
            stable-order pass (all rejected transiently or were evicted).
        """
        # Snapshot a stable, deterministic order up front. Sorting by uid
        # makes "priority" reproducible across dispatches, so the same
        # low-order workers stay saturated. ``str(uid)`` guarantees an
        # orderable key regardless of the uid's concrete type. Iterating a
        # snapshot keeps the pass well-defined even when ``remove_worker``
        # mutates the live context mid-loop on eviction.
        candidates = sorted(
            context.workers.items(), key=lambda item: str(item[0].uid)
        )

        for metadata, connection in candidates:
            try:
                stream = await connection.dispatch(task, timeout=timeout)
            except TransientRpcError as exc:
                logger.debug(
                    "Skipping worker %s on transient error: %s", metadata.uid, exc
                )
                continue
            except RpcError as exc:
                logger.warning(
                    "Evicting worker %s after non-transient RPC error: %s",
                    metadata.uid,
                    exc,
                )
                context.remove_worker(metadata)
                continue
            else:
                return stream

        raise NoWorkersAvailable("No healthy workers available for dispatch")
