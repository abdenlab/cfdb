"""Processor abstract base class."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.tools import format_name


class Processor(ABC):
    """Base class for format-specific preprocessing pipelines.

    A processor declares the set of upstream formats it handles and the
    artifact kinds it produces. It runs stage-by-stage inside a Wool worker,
    writing each stage's output to the caller-provided ``cache`` before
    proceeding to the next stage. This enables partial-commit recovery: on
    retry, the processor consults the cache and skips any stage whose
    output is already present.

    ``run`` is an async generator that yields a typed event stream:

    - ``{"event": "stage_complete", "kind": ArtifactKind.<X>.value,
       "key": <cache_key>}`` — emit after each ``cache.put`` for a stage
       so the API process can persist ``stages_done`` /
       ``artifact_cache_keys`` incrementally, not all at the end.
    - ``{"event": "complete", "artifacts": {kind: key, ...}}`` — emit
       once at the end with the full artifact map. Subclasses MUST emit
       this as the last yield.

    The wool routine wrapper composes a heartbeat loop around the
    processor's stream, so the processor itself never has to think
    about liveness signalling.

    Subclasses SHOULD be stateless — all execution state lives on the
    stack or inside the workdir path — to keep them trivially picklable
    across the Wool boundary.
    """

    #: Class-level version. Bump when the processor's output-producing
    #: logic changes in any way that affects the artifact bytes. This is
    #: baked into cache keys so bumps naturally trigger reprocessing.
    processor_version: int = 0

    #: Upstream ``file_format.name`` values this processor handles.
    supported_formats: frozenset[str] = frozenset()

    #: Artifact kinds this processor produces, in the order it produces
    #: them. For most pipelines this is ``[DATA, INDEX]``.
    artifact_kinds: tuple[ArtifactKind, ...] = ()

    def artifact_kinds_produced(
        self, file_meta: dict[str, Any] | None = None
    ) -> tuple[ArtifactKind, ...]:
        """Return the artifact kinds this processor writes for ``file_meta``.

        Default: ``cls.artifact_kinds``, the static class-level tuple.
        Subclasses MAY override to advertise different kinds based on
        per-file conditions (e.g., a BAM that is already coordinate-sorted
        upstream needs only an INDEX artifact; the DATA can be streamed
        directly from the upstream URL via the router's fall-through).

        ``file_meta`` is optional so callers that don't have it on hand
        (e.g., generic introspection) still get a reasonable answer.
        """
        return self.artifact_kinds

    def needs_processing(self, file_meta: dict[str, Any]) -> bool:
        """Return True when this processor is applicable and has work to do.

        The default implementation returns True when the file's format
        matches one of the processor's ``supported_formats``. Subclasses
        MAY override to add DCC-specific predicates (e.g., skip files that
        already have a tabix index sidecar).
        """
        fmt = format_name(file_meta)
        if fmt is None:
            return False
        return fmt in self.supported_formats

    @abstractmethod
    def run(
        self,
        file_meta: dict[str, Any],
        workdir: Path,
        cache_root: Path,
    ) -> AsyncIterator[dict[str, Any]]:
        """Execute the pipeline end-to-end as an event stream.

        Args:
            file_meta: A plain-dict snapshot of the source file's metadata
                (``FileMetadataModel.model_dump()``).
            workdir: A per-job scratch directory. The processor MAY create
                files here freely; the caller cleans it up after the job.
            cache_root: Root directory of the configured ``LocalFsCache``.
                The processor writes its artifacts directly under keys
                derived from ``file_meta``.

        Yields:
            ``{"event": "stage_complete", "kind": <str>, "key": <str>}``
            after each stage's ``cache.put``; one final
            ``{"event": "complete", "artifacts": {kind: key}}`` event.
            Implementations MUST be ``async def`` functions that use
            ``yield`` (i.e., async generators).
        """
