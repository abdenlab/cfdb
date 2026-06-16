"""Passthrough processor for formats that need no preprocessing."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from cfdb.workflows.cache import CacheBackend
from cfdb.workflows.events import Complete, WorkflowEvent
from cfdb.workflows.processors.base import Processor


class PassthroughProcessor(Processor):
    """Processor covering formats Gosling can consume without preprocessing.

    CSV, TSV, and bigWig are either Gosling-native (bigWig is self-indexed)
    or accepted as plain text (csv/tsv fetched wholesale). The router
    short-circuits the workflow for these formats — ``needs_processing``
    returns False and ``run`` is never invoked.
    """

    processor_version = 0
    supported_formats = frozenset({"CSV", "TSV", "bigWig"})
    artifact_kinds = ()

    def needs_processing(self, file_meta: dict[str, Any]) -> bool:
        """Always return False — the file is served straight from the DCC."""
        return False

    async def run(
        self,
        file_meta: dict[str, Any],
        workdir: Path,
        cache: CacheBackend,
    ) -> AsyncIterator[WorkflowEvent]:
        """Never invoked; included to satisfy the Processor ABC."""
        raise RuntimeError(
            "PassthroughProcessor.run must not be called; these formats "
            "are served directly by the /data router."
        )
        # Unreachable; kept so the function is syntactically an async
        # generator (matching the ABC contract).
        yield Complete(artifacts={})
