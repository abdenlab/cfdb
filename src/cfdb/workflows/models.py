"""Pydantic models and enums for workflow job records.

`JobRecord` is the persistent Mongo-backed representation of a single
workflow execution. Its shape mirrors the existing `SyncTask` pattern in
`services/sync.py` — status enum, timestamps, error string, progress — with
workflow-specific fields for the mutex key, the per-artifact cache keys,
and the stages that have committed their outputs to cache.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator


class JobStatus(str, Enum):
    """Lifecycle state of a workflow job.

    Active states (``pending``, ``running``) participate in the partial
    unique index on ``workflow_key`` that enforces the per-source mutex.
    Terminal states (``completed``, ``failed``) are excluded from that
    index, freeing the workflow key for fresh submissions.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


#: Statuses considered "active" — counted as holding the per-source mutex.
#: MUST stay in sync with the ``status`` literal list in
#: ``scripts/create-indexes.js``'s ``partialFilterExpression`` for the
#: ``jobs.workflow_key`` partial-unique index, otherwise the in-memory
#: predicate and the database-enforced mutex disagree.
ACTIVE_STATUSES: tuple[JobStatus, ...] = (JobStatus.PENDING, JobStatus.RUNNING)


class ArtifactKind(str, Enum):
    """Kind of artifact a processor writes to the cache.

    A single workflow may write multiple artifact kinds (e.g., a sorted
    BAM and its BAI). Each lands under its own cache key.
    """

    DATA = "data"
    INDEX = "index"


class JobRecord(BaseModel):
    """Mongo document representing a workflow job.

    Attributes:
        job_id: Opaque unique identifier for this job.
        workflow_key: Per-source mutex key; see ``workflows.keys.workflow_key``.
        status: Lifecycle state.
        dcc: DCC abbreviation (normalized lowercase).
        local_id: Source file identifier within the DCC.
        md5: MD5 hex digest of the source bytes.
        pipeline_version: Pipeline version baked into the workflow key.
        submitted_at: Timestamp when the job was first claimed.
        updated_at: Timestamp of last state change.
        stages_done: Names of completed stages within the workflow. Allows
            partial-commit recovery: on retry the processor skips stages
            already present in this list.
        artifact_cache_keys: Mapping of artifact kind to cache key, written
            once each stage commits its artifact.
        progress: Optional free-form progress string emitted by long-running
            stages. Capped at 256 chars.
        error: Populated on ``FAILED`` status with a human-readable reason.
            Capped at 1024 chars after path-scrubbing in
            ``lock.release_workflow`` so absolute filesystem paths from
            tool stderr don't leak via ``/jobs/{id}``.
        superseded_by: When this row is stale-reclaimed by a fresh claim,
            the winner's ``job_id`` is recorded here so clients polling
            this (now-FAILED) job can follow the chain to the live one.
        file_meta_snapshot: Plain-dict snapshot of the source file's
            metadata as it was at claim time. Persisted on insert so a
            re-run / observer can reconstruct the dispatch context
            without re-reading the (possibly mutated) ``files`` document.
        next_dispatch_at: When the durable retry scheduler should next
            attempt to dispatch this (still-PENDING) job to a worker. Set
            on claim and pushed forward each time an attempt finds no
            capacity; ``None`` once the job is running or terminal. Drives
            the scheduler's "due for dispatch" query.
        dispatch_attempts: How many times dispatch has been deferred for
            lack of worker capacity. Observability only — the failure
            deadline is measured from ``submitted_at``, not this count.
    """

    model_config = ConfigDict(
        # ``ignore`` rather than ``forbid`` so future ops/index fields
        # (e.g. Mongo-managed TTL housekeeping) don't fail validation.
        # Field-level constraints on ``error``/``progress``/``superseded_by``
        # carry the per-field length-cap defense.
        extra="ignore",
        str_max_length=4096,
    )

    job_id: Annotated[str, StringConstraints(min_length=1)]
    workflow_key: Annotated[str, StringConstraints(min_length=1)]
    status: JobStatus = JobStatus.PENDING
    dcc: Annotated[str, StringConstraints(min_length=1)]
    local_id: Annotated[str, StringConstraints(min_length=1)]
    md5: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{32}$")]
    pipeline_version: int = Field(ge=0)
    submitted_at: datetime
    updated_at: datetime
    stages_done: list[str] = Field(default_factory=list)
    artifact_cache_keys: dict[str, str] = Field(default_factory=dict)
    progress: Annotated[str, StringConstraints(max_length=256)] | None = None
    error: Annotated[str, StringConstraints(max_length=1024)] | None = None
    superseded_by: Annotated[str, StringConstraints(max_length=64)] | None = None
    file_meta_snapshot: dict[str, Any] | None = None
    next_dispatch_at: datetime | None = None
    dispatch_attempts: int = Field(default=0, ge=0)

    @field_validator("submitted_at", "updated_at", "next_dispatch_at")
    @classmethod
    def _require_aware_datetime(cls, value: datetime | None) -> datetime | None:
        """Reject naive datetimes.

        All internal producers go through ``lock._utcnow()`` which returns
        an aware UTC datetime; a naive value would either be a refactor
        regression or a malformed document. Caught here so the stale
        cutoff comparison in ``claim_workflow`` (and the scheduler's
        ``next_dispatch_at`` comparison) can never raise ``TypeError`` on a
        mismatched-naivety operand. ``next_dispatch_at`` is nullable, so a
        ``None`` passes through untouched.
        """
        if value is None:
            return value
        if value.tzinfo is None:
            raise ValueError(
                "JobRecord datetimes must be timezone-aware "
                "(JobRecord uses aware UTC throughout)"
            )
        return value

    def to_mongo(self) -> dict[str, Any]:
        """Serialize to a plain dict suitable for Mongo insertion.

        Hand-rolled (rather than ``model_dump(mode="python")``) so the
        wire shape is explicit and adding a model field requires a
        deliberate update here — guards against accidentally persisting
        a future field without thinking about its index/query semantics.
        """
        return {
            "job_id": self.job_id,
            "workflow_key": self.workflow_key,
            "status": self.status.value,
            # ``active`` is a pure DB projection of ``status`` (active ==
            # status in ACTIVE_STATUSES). It backs the partial-filter
            # predicates on the ``jobs`` indexes: Amazon DocumentDB rejects
            # ``$in`` inside a partialFilterExpression, so the mutex /
            # TTL indexes filter on this boolean with implicit equality
            # ({"active": true}/{"active": false}) instead of a status
            # ``$in`` list. ``lock.py`` keeps it in lockstep on every
            # status-changing write.
            "active": self.status in ACTIVE_STATUSES,
            "dcc": self.dcc,
            "local_id": self.local_id,
            "md5": self.md5,
            "pipeline_version": self.pipeline_version,
            "submitted_at": self.submitted_at,
            "updated_at": self.updated_at,
            "stages_done": list(self.stages_done),
            "artifact_cache_keys": dict(self.artifact_cache_keys),
            "progress": self.progress,
            "error": self.error,
            "superseded_by": self.superseded_by,
            "file_meta_snapshot": (
                dict(self.file_meta_snapshot)
                if self.file_meta_snapshot is not None
                else None
            ),
            "next_dispatch_at": self.next_dispatch_at,
            "dispatch_attempts": self.dispatch_attempts,
        }
