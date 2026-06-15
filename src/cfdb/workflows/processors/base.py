"""Processor abstract base class."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from cfdb.workflows import keys as key_utils
from cfdb.workflows.cache import CacheBackend
from cfdb.workflows.events import WorkflowEvent
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

    ``run`` is an async generator that yields typed
    :mod:`cfdb.workflows.events` objects:

    - :class:`~cfdb.workflows.events.StageComplete` — emit after each
       ``cache.put`` for a stage so the API process can persist
       ``stages_done`` / ``artifact_cache_keys`` incrementally, not all
       at the end.
    - :class:`~cfdb.workflows.events.Complete` — emit once at the end with
       the full artifact map. Subclasses MUST emit this as the last yield.

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

    def cache_key_for(
        self, file_meta: dict[str, Any], artifact_kind: ArtifactKind
    ) -> str:
        """Return the cache key this processor writes ``artifact_kind`` under.

        The single authority for cache-key derivation. The router probes
        the cache with this key, the processor ``put``s under it, and the
        :class:`~cfdb.workflows.events.StageComplete` event carries it — so
        all three agree by construction rather than by three independent
        re-derivations that must be kept in sync. ``processor_version`` is
        baked in, so bumping it invalidates this processor's cached
        artifacts without disturbing other processors'.

        Raises ``ValueError`` (via :func:`extract_identity`) when
        ``file_meta`` is missing dcc / local_id / md5.
        """
        dcc, local_id, md5 = key_utils.extract_identity(file_meta)
        return key_utils.cache_key(
            dcc=dcc,
            local_id=local_id,
            artifact_kind=artifact_kind,
            md5=md5,
            processor_version=self.processor_version,
        )

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
        cache: CacheBackend,
    ) -> AsyncIterator[WorkflowEvent]:
        """Execute the pipeline end-to-end as an event stream.

        Args:
            file_meta: A plain-dict snapshot of the source file's metadata
                (``FileMetadataModel.model_dump()``).
            workdir: A per-job scratch directory. The processor MAY create
                files here freely; the caller cleans it up after the job.
            cache: The configured cache backend (``LocalFsCache`` or
                ``S3Cache``), handed across the Wool boundary so artifacts
                are persisted to the deployment's real backing store. The
                processor materialises each artifact locally under
                ``workdir`` and then ``cache.put``s it under a key derived
                from ``file_meta``. Reusing the injected backend (rather
                than constructing a ``LocalFsCache``) is what lets the
                S3/ECS profile actually persist artifacts to S3.

        Yields:
            A :class:`~cfdb.workflows.events.StageComplete` after each
            stage's ``cache.put``, then one final
            :class:`~cfdb.workflows.events.Complete`. Implementations MUST
            be ``async def`` functions that use ``yield`` (i.e., async
            generators).
        """
