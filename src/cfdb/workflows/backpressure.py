"""Per-worker backpressure hook for the wool worker pool (issue #45).

A worker rejects an incoming dispatch once it already has a configured
number of tasks in flight, serializing the subprocess pipelines
(samtools/bgzip/tabix/sort/…) the routine shells out to so a single
1-vCPU/2-GiB worker stops oversubscribing. wool surfaces the rejection to
the dispatcher as gRPC ``RESOURCE_EXHAUSTED`` — a transient error the load
balancer rotates past to the next worker.

The hook is a plain, picklable object holding only an ``int`` because
``wool.LocalWorker`` serializes ``backpressure`` into its worker
subprocess. Keep it free of async/runtime state (no locks, queues, or
event-loop references) so it survives that serialization boundary cleanly.
"""

from __future__ import annotations

import wool


class TaskCountBackpressure:
    """Reject a dispatch when the worker already has ``threshold`` tasks.

    Implements :class:`wool.BackpressureLike`: returns ``True`` (reject)
    when the worker's in-flight task count has reached ``threshold`` and
    ``False`` (accept) otherwise. With ``threshold == 1`` each worker runs
    at most one routine at a time, serializing its CPU/IO-heavy subprocess
    pipeline. The "disable backpressure" case (threshold 0) is handled by
    the wiring, which passes ``backpressure=None`` rather than this hook.
    """

    def __init__(self, threshold: int) -> None:
        if threshold < 1:
            raise ValueError(
                f"TaskCountBackpressure threshold must be >= 1; got {threshold} "
                "(pass backpressure=None to disable backpressure entirely)"
            )
        self.threshold = threshold

    def __call__(self, ctx: wool.BackpressureContext) -> bool:
        """Return True to reject the incoming task, False to accept it."""
        return ctx.active_task_count >= self.threshold

    def __repr__(self) -> str:
        return f"{type(self).__name__}(threshold={self.threshold})"


def backpressure_for(threshold: int) -> TaskCountBackpressure | None:
    """Build the per-worker backpressure hook for ``threshold``, or None.

    ``threshold == 0`` disables backpressure: the worker wiring passes
    ``backpressure=None`` to wool, restoring unbounded admission (the prior
    behavior). Any positive value yields a :class:`TaskCountBackpressure`
    that rejects once the worker has that many tasks in flight. A negative
    threshold is rejected rather than silently disabling backpressure, so
    the helper's contract is total.
    """
    if threshold < 0:
        raise ValueError(
            f"backpressure threshold must be >= 0; got {threshold} "
            "(0 disables backpressure, a positive value sets the limit)"
        )
    return TaskCountBackpressure(threshold) if threshold > 0 else None
