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
- ``CFDB_WORKFLOW_DISPATCH_WAIT_S`` — how long ``ensure_workflow`` waits
  for a wool worker to become available before giving up. Default ``240``
  (4 min) — sized for an ECS Fargate cold start (image pull + health
  check, typically 1-3 min). A smaller value risks exhausting the retry
  budget before a freshly-provisioned worker reports HEALTHY; lower it
  for fixture-bound dev where workers are already running.
- ``CFDB_WORKFLOW_HEARTBEAT_INTERVAL_S`` — how often the routine emits a
  heartbeat event during quiet periods so the API can refresh
  ``updated_at`` on the JobRecord. Default ``300`` (5 min).
- ``CFDB_WORKFLOW_STALE_THRESHOLD_S`` — ``updated_at`` age beyond which a
  RUNNING/PENDING row is considered stale and reclaimable. Default ``900``
  (15 min) — sized as ``2 × heartbeat_interval + safety_margin`` so a
  single missed heartbeat does not falsely reclaim a healthy worker.
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
# active job on every check, ``DISPATCH_WAIT_S=0`` makes every cold start
# look like a hard failure).
WORKFLOW_DURATION_CAP_S: Final = _positive_int(
    "CFDB_WORKFLOW_DURATION_CAP_S",
    os.getenv("CFDB_WORKFLOW_DURATION_CAP_S", "14400"),
    minimum=1,
)
# Default sized for a Fargate cold start (image pull + health check,
# ~1-3 min). With ``quorum=0`` a cold-start dispatch surfaces
# ``NoWorkersAvailable``, which the executor retries inside this budget;
# the old 60s default could expire before the just-launched worker
# reports HEALTHY, hard-failing the first request. 240s covers the
# cold-start window with headroom while staying env-overridable.
WORKFLOW_DISPATCH_WAIT_S: Final = _positive_int(
    "CFDB_WORKFLOW_DISPATCH_WAIT_S",
    os.getenv("CFDB_WORKFLOW_DISPATCH_WAIT_S", "240"),
    minimum=1,
)
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
