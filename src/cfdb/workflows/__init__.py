"""On-demand preprocessing and indexing workflow subsystem.

Provides a pluggable pipeline that transforms files sourced from DCCs into
Gosling-ready artifacts (sorted/indexed BAM, bgzipped/tabix-indexed text
intervals). A single workflow services both `/data` and `/index` requests
for a given source file via a per-source mutex, writes outputs to a
pluggable cache, and is dispatched on-demand when a cache miss occurs.

Environment-variable knobs
--------------------------

Memory caps (passed to external tools — they spill to disk when exceeded):

- ``CFDB_SORT_MEMORY_CAP`` — GNU ``sort -S`` value. Defaults to ``256M``.
- ``CFDB_SAMTOOLS_MEMORY_CAP_PER_THREAD`` — ``samtools sort -m`` value,
  applied **per thread**. With ``CFDB_SAMTOOLS_THREADS=N``, total RSS is
  bounded by ``N × this value``. Defaults to ``256M``.

CPU / thread caps (avoid over-subscription under WorkerPool concurrency):

- ``CFDB_SAMTOOLS_THREADS`` — threads passed via ``samtools sort -@`` and
  ``samtools index -@``. Default ``1``.
- ``CFDB_SORT_PARALLEL`` — threads passed via GNU ``sort --parallel=N``.
  Default ``2``.

Runtime / lifecycle (consumed by the executor and lock modules):

- ``CFDB_WORKFLOW_DURATION_CAP_S`` — per-workflow wall-clock cap, enforced
  via ``asyncio.timeout`` on the API side while consuming the routine's
  event stream. Default ``14400`` (4 h) — sized for multi-hour
  preprocessing runs (e.g., a ``samtools sort`` on a multi-GB BAM
  followed by ``samtools index``). Operators running on bounded fixtures
  in dev should lower this via env.
- ``CFDB_WORKFLOW_HEARTBEAT_INTERVAL_S`` — how often the routine emits a
  heartbeat event during quiet periods so the API can refresh
  ``updated_at`` on the JobRecord. Default ``300`` (5 min).
- ``CFDB_WORKFLOW_STALE_THRESHOLD_S`` — ``updated_at`` age beyond which a
  RUNNING/PENDING row is considered stale and reclaimable. Default ``900``
  (15 min) — sized as ``2 × heartbeat_interval + safety_margin`` so a
  single missed heartbeat does not falsely reclaim a healthy worker.

Concurrency / admission (bounded-concurrency control, issue #45):

- ``CFDB_WORKER_MAX_CONCURRENT_TASKS`` — per-worker backpressure
  threshold: a worker rejects a dispatch (gRPC ``RESOURCE_EXHAUSTED``,
  which the load balancer treats as transient and rotates past) once it
  already has this many tasks in flight. Default ``1`` (serialize the
  subprocess pipelines on a 1-vCPU worker); ``0`` disables backpressure
  (unbounded — the prior behavior). Enforced worker-side via a
  ``wool.BackpressureLike`` hook.
- ``CFDB_WORKFLOW_MAX_ACTIVE`` — admission ceiling on concurrently active
  workflows (``pending`` + ``running`` jobs). ``ensure_workflow`` returns
  ``429 Retry-After`` once this many are active, shedding load before
  claiming the per-file mutex. Default ``1024``. Soft cap
  (count-then-insert may briefly overshoot).
- ``CFDB_WORKFLOW_DISPATCH_DEADLINE_S`` — how long a job may wait for
  worker capacity (re-dispatched on the retry cadence below) before it is
  marked ``failed``. Default ``14400`` (4 h).
- ``CFDB_WORKFLOW_RETRY_INTERVAL_S`` — base cadence at which the durable
  retry scheduler re-attempts dispatch for a queued job awaiting
  capacity; a small random jitter is added per attempt. Default ``120``
  (2 min).
"""

from __future__ import annotations

import os
import re
from typing import Final

_MEMORY_CAP_RE = re.compile(r"^\d+[KMGkmg]?$")


def _validate_memory_cap(name: str, value: str) -> str:
    """Validate an env-var memory cap (e.g. ``"256M"``) at module import.

    A malformed value fails fast here rather than at first workflow
    dispatch with a cryptic shell error. Shape mirrors the syntax both
    ``sort -S`` and ``samtools sort -m`` accept.
    """
    if not _MEMORY_CAP_RE.fullmatch(value):
        raise ValueError(f"{name} must match {_MEMORY_CAP_RE.pattern!r}; got {value!r}")
    return value


def _positive_int(name: str, value: str, *, minimum: int = 0) -> int:
    """Parse an integer env-var, raising on malformed input or values below ``minimum``.

    Defaults to ``minimum=0`` (non-negative). Callers that need a strict
    positive lower bound (e.g., a runtime cap whose zero value would
    silently break a loop or timeout) pass ``minimum=1``.
    """
    try:
        parsed = int(value)
    except ValueError as e:
        raise ValueError(f"{name} must be an integer; got {value!r}") from e
    if parsed < minimum:
        raise ValueError(f"{name} must be >= {minimum}; got {parsed}")
    return parsed


SORT_MEMORY_CAP: Final = _validate_memory_cap(
    "CFDB_SORT_MEMORY_CAP",
    os.getenv("CFDB_SORT_MEMORY_CAP", "256M"),
)
if os.getenv("CFDB_SAMTOOLS_MEMORY_CAP") is not None:
    raise ValueError(
        "CFDB_SAMTOOLS_MEMORY_CAP has been renamed to "
        "CFDB_SAMTOOLS_MEMORY_CAP_PER_THREAD to make per-thread semantics "
        "explicit (samtools sort -m is per-thread, so total RSS is "
        "CFDB_SAMTOOLS_THREADS × this value). Update your deployment."
    )

SAMTOOLS_MEMORY_CAP: Final = _validate_memory_cap(
    "CFDB_SAMTOOLS_MEMORY_CAP_PER_THREAD",
    os.getenv("CFDB_SAMTOOLS_MEMORY_CAP_PER_THREAD", "256M"),
)

SAMTOOLS_THREADS: Final = _positive_int(
    "CFDB_SAMTOOLS_THREADS",
    os.getenv("CFDB_SAMTOOLS_THREADS", "1"),
)
SORT_PARALLEL: Final = _positive_int(
    "CFDB_SORT_PARALLEL",
    os.getenv("CFDB_SORT_PARALLEL", "2"),
)

# Runtime caps require ``minimum=1`` — zero values silently break the
# corresponding loop or timeout (``asyncio.timeout(0)`` fires immediately,
# ``HEARTBEAT_INTERVAL_S=0`` spins, ``STALE_THRESHOLD_S=0`` reclaims every
# active job on every check).
WORKFLOW_DURATION_CAP_S: Final = _positive_int(
    "CFDB_WORKFLOW_DURATION_CAP_S",
    os.getenv("CFDB_WORKFLOW_DURATION_CAP_S", "14400"),
    minimum=1,
)
# NOTE: the former ``CFDB_WORKFLOW_DISPATCH_WAIT_S`` (a single in-request
# cold-start wait) was removed in the #45 restructure. A dispatch that
# finds no capacity no longer blocks the request — the job queues durably
# and the retry scheduler re-attempts it on ``CFDB_WORKFLOW_RETRY_INTERVAL_S``
# until ``CFDB_WORKFLOW_DISPATCH_DEADLINE_S`` (both below).
WORKFLOW_HEARTBEAT_INTERVAL_S: Final = _positive_int(
    "CFDB_WORKFLOW_HEARTBEAT_INTERVAL_S",
    os.getenv("CFDB_WORKFLOW_HEARTBEAT_INTERVAL_S", "300"),
    minimum=1,
)
WORKFLOW_STALE_THRESHOLD_S: Final = _positive_int(
    "CFDB_WORKFLOW_STALE_THRESHOLD_S",
    os.getenv("CFDB_WORKFLOW_STALE_THRESHOLD_S", "900"),
    minimum=1,
)

# The stale-reclaim threshold MUST exceed two heartbeat intervals so a
# single missed heartbeat (e.g., a brief Mongo write delay) does not
# falsely reclaim a healthy worker. The default values (300 / 900)
# satisfy this with a 300s safety margin; an operator-tuned pair that
# violates the invariant is a configuration error.
if WORKFLOW_STALE_THRESHOLD_S < 2 * WORKFLOW_HEARTBEAT_INTERVAL_S:
    raise ValueError(
        f"CFDB_WORKFLOW_STALE_THRESHOLD_S ({WORKFLOW_STALE_THRESHOLD_S}s) "
        f"must be >= 2 * CFDB_WORKFLOW_HEARTBEAT_INTERVAL_S "
        f"({WORKFLOW_HEARTBEAT_INTERVAL_S}s) to avoid false stale-reclaim "
        "of healthy workers between heartbeats."
    )

# --- Bounded-concurrency control (issue #45) --------------------------------

# Per-worker backpressure threshold. ``minimum=0`` because 0 is a valid
# "disable backpressure" sentinel — the worker wiring passes
# ``backpressure=None`` when 0, restoring the unbounded prior behavior.
WORKER_MAX_CONCURRENT_TASKS: Final = _positive_int(
    "CFDB_WORKER_MAX_CONCURRENT_TASKS",
    os.getenv("CFDB_WORKER_MAX_CONCURRENT_TASKS", "1"),
    minimum=0,
)
# Admission ceiling on concurrently active (pending + running) workflows.
# ``ensure_workflow`` returns 429 once this many are active. ``minimum=1``
# because 0 would reject every request.
WORKFLOW_MAX_ACTIVE: Final = _positive_int(
    "CFDB_WORKFLOW_MAX_ACTIVE",
    os.getenv("CFDB_WORKFLOW_MAX_ACTIVE", "1024"),
    minimum=1,
)
# How long a job may wait for worker capacity before being failed.
# ``minimum=1`` — 0 would fail every queued job on its first attempt.
WORKFLOW_DISPATCH_DEADLINE_S: Final = _positive_int(
    "CFDB_WORKFLOW_DISPATCH_DEADLINE_S",
    os.getenv("CFDB_WORKFLOW_DISPATCH_DEADLINE_S", "14400"),
    minimum=1,
)
# Base cadence for the durable retry scheduler's re-dispatch attempts;
# per-attempt jitter is layered on top. ``minimum=1`` — 0 would busy-spin.
WORKFLOW_RETRY_INTERVAL_S: Final = _positive_int(
    "CFDB_WORKFLOW_RETRY_INTERVAL_S",
    os.getenv("CFDB_WORKFLOW_RETRY_INTERVAL_S", "120"),
    minimum=1,
)
