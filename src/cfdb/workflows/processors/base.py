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

    #: Stable identity of this processor, baked into its cache keys so two
    #: processors claiming the same ``(file, artifact_kind)`` pair can never
    #: read back each other's artifacts (issue #109). Subclasses that do
    #: NOT declare one inherit their own class name via
    #: ``__init_subclass__`` — distinct by default, so forgetting is safe.
    #: Declare an explicit value when the identity should survive a class
    #: rename: changing this string invalidates every artifact the
    #: processor has cached.
    #:
    #: The declaration MUST sit in the processor's own class body. The
    #: default is applied from ``cls.__dict__``, not the MRO, so a value
    #: supplied by a mixin or a base class is discarded in favour of the
    #: class name — factoring a pinned identity out into a mixin would
    #: silently cold-cache every artifact keyed under it.
    processor_id: str = ""

    #: Class-level version. Bump when the processor's output-producing
    #: logic changes in any way that affects the artifact bytes. This is
    #: baked into cache keys so bumps naturally trigger reprocessing.
    processor_version: int = 0

    #: Upstream ``file_format.name`` values this processor handles.
    supported_formats: frozenset[str] = frozenset()

    #: Artifact kinds this processor produces, in the order it produces
    #: them. For most pipelines this is ``[DATA, INDEX]``.
    artifact_kinds: tuple[ArtifactKind, ...] = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Default ``processor_id`` to the subclass's own class name.

        Only a value declared in the subclass's **own body** counts — an
        inherited one is replaced. A subclass of a shipped processor
        produces potentially different bytes under the same
        ``processor_version``, so inheriting the parent's identity would
        recreate exactly the aliasing the identity exists to prevent. A
        declared-but-falsy value (``processor_id = ""``) is treated as no
        declaration at all and takes the class-name default, because an
        empty identity is the same failure as a missing one.

        The identity is validated here rather than at first use, so a
        malformed one (``"   "``, ``".."``, one colliding with an artifact
        kind) raises when the module is imported instead of surfacing
        per-request inside a worker, long after the class that caused it
        was written. The class-name default goes through the same
        normalizer as a declared value: ``is_legacy_cache_key``'s safety
        argument rests on no identity ever equalling an artifact kind, and
        a guarantee that held only for declared identities would leave
        ``class index(Processor)`` failing at derivation instead.

        A ``processor_id`` supplied by a **mixin** — a base that is not
        itself a ``Processor`` — raises rather than being silently
        discarded. Replacing it would be a lie the reader cannot see: the
        mixin's source shows a pinned identity and the runtime uses the
        class name, so factoring a pinned identity into a mixin would cold-
        cache everything keyed under it with no signal. Inheriting from
        another ``Processor`` is a different case and stays legal, because
        every level of such a chain correctly takes its own class name.
        """
        super().__init_subclass__(**kwargs)
        declared = cls.__dict__.get("processor_id")
        if not declared:
            cls._reject_mixin_supplied_identity()
        cls.processor_id = key_utils.normalize_processor_id(
            declared or cls.__name__
        )

    @classmethod
    def _reject_mixin_supplied_identity(cls) -> None:
        """Raise when a non-``Processor`` base declares ``processor_id``."""
        for base in cls.__mro__[1:]:
            if issubclass(base, Processor):
                continue
            supplied = base.__dict__.get("processor_id")
            if supplied:
                raise ValueError(
                    f"{cls.__name__} inherits processor_id {supplied!r} from "
                    f"mixin {base.__name__}, which would be silently discarded "
                    f"in favour of the class name. Declare processor_id in "
                    f"{cls.__name__}'s own body instead."
                )

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
        re-derivations that must be kept in sync. Both ``processor_id`` and
        ``processor_version`` are baked in: the identity keeps this
        processor's artifacts disjoint from every other processor's even at
        an equal version, and bumping the version invalidates this
        processor's own cached artifacts without disturbing anyone else's.

        Raises ``ValueError`` (via :func:`extract_identity`) when
        ``file_meta`` is missing dcc / local_id / md5.
        """
        dcc, local_id, md5 = key_utils.extract_identity(file_meta)
        return key_utils.cache_key(
            dcc=dcc,
            local_id=local_id,
            artifact_kind=artifact_kind,
            md5=md5,
            processor_id=self.processor_id,
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
