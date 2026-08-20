"""Processor registry — maps file metadata to the processor that handles it."""

from __future__ import annotations

import unicodedata
from typing import Any

from cfdb.workflows.processors.base import Processor
from cfdb.workflows.processors.passthrough import PassthroughProcessor
from cfdb.workflows.processors.tools import format_name


def _identity_fold(processor_id: str) -> str:
    """Fold an identity to the form two cache entries would collide under.

    ``cache_key`` preserves an identity's case and Unicode spelling, but a
    cache backend need not: the identity segment becomes a directory name
    under ``LocalFsCache``, and a case-insensitive filesystem (APFS by
    default) or a normalizing one folds two spellings onto one directory.
    Comparing on the folded form is what makes the registry's uniqueness
    guard match the guarantee ``cache_key`` is relied on to provide.
    """
    return unicodedata.normalize("NFC", processor_id).casefold()


class ProcessorRegistry:
    """Resolve a ``Processor`` instance for a given file metadata document.

    Processors are registered in order of decreasing specificity. Lookup
    walks the registered list and returns the first processor whose
    ``supported_formats`` matches the file's format name. ``PassthroughProcessor``
    is typically registered FIRST with an explicit format set ({"CSV", "TSV",
    "bigWig"}) so Gosling-native formats short-circuit before the heavier
    BAM/tabix processors are consulted — it is not a generic catch-all.
    """

    def __init__(self) -> None:
        self._processors: list[Processor] = []

    def register(self, processor: Processor) -> None:
        """Register a processor. First registration for a given format wins.

        ``lookup_for`` walks the registered list in insertion order and
        returns the first processor whose ``supported_formats`` covers
        the file's format name, so callers should register more
        specific processors before more general ones.

        The guard's scope is **this registry instance**, not the process:
        uniqueness is a property of one wired deployment, and the single
        wiring site is ``cfdb.api.main``'s lifespan. A deployment that
        wires a second registry (a worker-side one, a batch tool) has to
        re-establish the invariant itself — it travels with the registry,
        not with the processor class.

        Identities are compared case- and Unicode-folded rather than by
        exact string equality. ``cache_key`` preserves the raw spelling,
        but a cache backend need not keep two spellings apart: on a
        case-insensitive filesystem ``BedProcessor`` and ``BEDProcessor``
        derive distinct keys that resolve to one directory, which is the
        aliasing this guard exists to prevent.

        Raises:
            ValueError: Another registered processor already claims this
                one's ``processor_id``. Cache keys are scoped by that
                identity (issue #109), so two processors sharing it would
                read back each other's artifacts as cache hits — a wrong
                answer rather than a miss. Rejecting at wiring time turns
                the property into an enforced invariant instead of a
                convention each new processor has to remember.
        """
        folded = _identity_fold(processor.processor_id)
        for registered in self._processors:
            if _identity_fold(registered.processor_id) != folded:
                continue
            collision = (
                "is already registered by"
                if registered.processor_id == processor.processor_id
                else "collides once case- and Unicode-folded with the one "
                "registered by"
            )
            raise ValueError(
                f"processor_id {processor.processor_id!r} {collision} "
                f"{type(registered).__name__} ({registered.processor_id!r}); "
                f"cache keys are scoped by this identity, so "
                f"{type(processor).__name__} would alias its artifacts"
            )
        self._processors.append(processor)

    def lookup_for(self, file_meta: dict[str, Any]) -> Processor | None:
        """Return the first registered processor applicable to ``file_meta``.

        Returns None when no registered processor lists the file's format
        in its ``supported_formats``.
        """
        fmt = format_name(file_meta)
        if fmt is None:
            return None
        for processor in self._processors:
            if fmt in processor.supported_formats:
                return processor
        return None


def default_registry() -> ProcessorRegistry:
    """Build the default registry with the passthrough processor pre-registered.

    Returns a registry containing only ``PassthroughProcessor``. The
    BAM and tabix processors are not registered here — ``cfdb.api.main``
    registers them explicitly during application startup so the wired
    registry is populated before any request is served. Callers that
    want a fully-wired registry outside the API lifespan must invoke
    ``register(BamIndexProcessor())`` and ``register(TabixIntervalProcessor())``
    themselves.
    """
    registry = ProcessorRegistry()
    registry.register(PassthroughProcessor())
    return registry
