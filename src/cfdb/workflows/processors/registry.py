"""Processor registry — maps file metadata to the processor that handles it."""

from __future__ import annotations

from typing import Any

from cfdb.workflows.processors.tools import format_name
from cfdb.workflows.processors.base import Processor
from cfdb.workflows.processors.passthrough import PassthroughProcessor


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
        """
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
