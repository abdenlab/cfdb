"""Typed events streamed from a processor to the executor.

Processors yield these objects (rather than raw ``{"event": ...}`` dicts)
so the processor-to-executor wire contract is explicit and type-checked,
and the executor matches on type instead of parsing stringly-typed dicts.

These cross the Wool cloudpickle boundary as the routine's stream values,
so they MUST live in this stable module — never in a ``test_*`` module —
for cloudpickle to resolve them by reference on the worker side (the same
constraint that keeps cross-boundary stubs in ``tests/integration/routines.py``).
Plain frozen dataclasses pickle by value without help.
"""

from __future__ import annotations

from dataclasses import dataclass

from cfdb.workflows.models import ArtifactKind


@dataclass(frozen=True)
class StageComplete:
    """A pipeline stage finished and committed its artifact to the cache.

    Emitted after each ``cache.put`` so the executor can persist
    ``stages_done`` / ``artifact_cache_keys`` incrementally. ``key`` is the
    cache key the processor wrote under — derived via the same
    ``Processor.cache_key_for`` the router probes with, so the two agree by
    construction.
    """

    kind: ArtifactKind
    key: str


@dataclass(frozen=True)
class Complete:
    """Terminal success: the full artifact-kind-value -> cache-key map."""

    artifacts: dict[str, str]


@dataclass(frozen=True)
class Progress:
    """Human-readable progress hint for ``JobRecord.progress``."""

    value: str


@dataclass(frozen=True)
class Heartbeat:
    """Liveness signal injected by the routine wrapper during quiet stages.

    Lets the API refresh ``JobRecord.updated_at`` without the worker
    touching Mongo. Never emitted by processors themselves.
    """


@dataclass(frozen=True)
class Error:
    """Terminal failure carrying the exception type name and message."""

    type: str
    error: str


#: Union of every event a processor stream (wrapped by the routine) yields.
WorkflowEvent = StageComplete | Complete | Progress | Heartbeat | Error
