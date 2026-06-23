"""Workflow mutex — atomic claim and release via a Mongo ``jobs`` collection.

The mutex is enforced by a partial unique index on ``workflow_key`` filtered
to active statuses (pending/running). When two concurrent ``/data`` and
``/index`` requests converge on the same source file, one ``insert_one``
succeeds and dispatches the workflow; the other raises
``DuplicateKeyError`` and attaches to the in-flight job via a re-read.

This mirrors the atomic-upsert idiom used by ``services.locks`` for the
sync and cutover locks — see ``services/locks.py:24-81`` — but uses the
partial unique index instead of a scalar ``active`` flag so that many
concurrent workflows (one per source file) may run in parallel.
"""

from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError
from pymongo.write_concern import WriteConcern

from cfdb.workflows import WORKFLOW_STALE_THRESHOLD_S
from cfdb.workflows.models import ACTIVE_STATUSES, ArtifactKind, JobRecord, JobStatus

#: Write concern applied to every write against the ``jobs`` collection.
#: The per-source mutex depends on ``insert_one`` durability surviving
#: primary failover — under a non-majority default an acked insert can
#: be rolled back and admit a duplicate workflow. ``w="majority"`` is
#: supported on both vanilla MongoDB and AWS DocumentDB; ``j=True`` is
#: deliberately NOT set since DocumentDB rejects journal acks.
_JOBS_WRITE_CONCERN = WriteConcern(w="majority")


def _jobs(db) -> Any:
    """Return the ``jobs`` collection handle with majority write concern.

    The ``with_options`` call falls back gracefully when the driver
    doesn't expose it (some test doubles, or older mongomock variants
    that return a non-async collection from ``with_options``). The
    write-concern pin is a production durability hardening; tests that
    don't model write-concern semantics can run against the bare
    collection.
    """
    coll = db[JOBS_COLLECTION]
    with_options = getattr(coll, "with_options", None)
    if with_options is None:
        return coll
    try:
        configured = with_options(write_concern=_JOBS_WRITE_CONCERN)
    except Exception:
        return coll
    # Some test doubles return a sync stub from ``with_options`` even
    # though the original is async. Detect and fall back.
    if not hasattr(configured, "find_one_and_update"):
        return coll
    if not callable(getattr(configured, "find_one_and_update", None)):
        return coll
    # Heuristic: real motor collections return a coroutine from these
    # methods. mongomock-motor's `with_options` historically degrades to
    # the sync collection — detect that by sniffing one method.
    import inspect

    sample = getattr(configured, "find_one_and_update", None)
    if sample is not None and not inspect.iscoroutinefunction(sample):
        return coll
    return configured

logger = logging.getLogger(__name__)

#: Collection name for workflow job records.
JOBS_COLLECTION = "jobs"

#: A workflow whose ``updated_at`` is older than this is considered stale —
#: i.e., the worker crashed without releasing OR the API process died
#: between the last heartbeat and a clean release_workflow. Driven by the
#: ``CFDB_WORKFLOW_STALE_THRESHOLD_S`` env var; the default is sized as
#: ``2 × CFDB_WORKFLOW_HEARTBEAT_INTERVAL_S + safety_margin`` so a single
#: missed heartbeat does not falsely reclaim a healthy worker.
STALE_WORKFLOW_THRESHOLD = timedelta(seconds=WORKFLOW_STALE_THRESHOLD_S)

_CLAIM_MAX_ATTEMPTS = 3

_ACTIVE_STATUS_VALUES: tuple[str, ...] = tuple(s.value for s in ACTIVE_STATUSES)

#: Absolute filesystem path pattern. Stripped from error text before
#: persisting so tool stderr (samtools/tabix) doesn't leak workdir paths
#: or container layout via the unauthenticated ``/jobs/{id}`` endpoint.
#:
#: Anchored to multi-segment paths starting with a letter so legitimate
#: tokens are preserved in diagnostic output:
#:   stripped: ``/tmp/foo/bar``, ``/app/src/cfdb``, ``/home/app/job_xyz``
#:   preserved: ``HTTP/1.1``, ``45/1000``, ``signal/SIGTERM``, ``v1/2/3``
_ABSOLUTE_PATH_RE = re.compile(
    r"(?<![A-Za-z0-9_])/[A-Za-z](?:[A-Za-z0-9_.\\-]*/)+[A-Za-z0-9_.\\-]*"
)

#: Cap persisted error strings. Matches the Pydantic constraint on
#: ``JobRecord.error`` (1024 chars) so the model never has to reject a
#: pre-scrubbed value.
_MAX_ERROR_LEN = 1024


def _scrub_error_text(text: str | None) -> str | None:
    """Strip absolute paths and cap length before persisting.

    Best-effort: relative paths, library version strings, and process
    memory addresses can still ride through. The length cap is the
    backstop for unbounded stderr from a misbehaving subprocess.
    """
    if text is None:
        return None
    cleaned = _ABSOLUTE_PATH_RE.sub("<path>", text)
    return cleaned[:_MAX_ERROR_LEN]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _record_from_mongo(doc: dict[str, Any]) -> JobRecord:
    """Coerce a Mongo document to a JobRecord, dropping the ``_id`` field.

    BSON Date round-trips through some Motor / mongomock-motor variants
    as a naive ``datetime`` (the BSON Date type itself has no timezone
    field; servers render UTC by convention). The application-level
    ``JobRecord`` validator rejects naive datetimes — that guard
    catches buggy in-process producers, but Mongo writes are trusted to
    be UTC. Re-attach ``tzinfo=UTC`` to ``submitted_at`` / ``updated_at``
    here so the round-trip survives.
    """
    sanitized = {k: v for k, v in doc.items() if k != "_id"}
    for field in ("submitted_at", "updated_at", "next_dispatch_at"):
        value = sanitized.get(field)
        if isinstance(value, datetime) and value.tzinfo is None:
            sanitized[field] = value.replace(tzinfo=timezone.utc)
    return JobRecord.model_validate(sanitized)


async def claim_workflow(
    db,
    workflow_key: str,
    *,
    dcc: str,
    local_id: str,
    md5: str,
    pipeline_version: int,
    file_meta_snapshot: Optional[dict[str, Any]] = None,
) -> tuple[JobRecord, bool]:
    """Atomically claim or attach to the workflow for this source file.

    Args:
        db: Motor database handle.
        workflow_key: The mutex key.
        dcc: Normalized DCC abbreviation.
        local_id: Source file identifier within the DCC.
        md5: MD5 hex digest of the source file.
        pipeline_version: Current workflow pipeline version.
        file_meta_snapshot: Optional plain-dict snapshot of the source
            file's metadata as it was at dispatch time. Persisted on the
            insert so observers/replays can reconstruct the dispatch
            context without re-reading a possibly-mutated ``files`` doc.

    Returns:
        A tuple ``(job, is_fresh)`` where ``is_fresh`` is True when this
        caller inserted the job record (and must dispatch the workflow)
        and False when attaching to an already-running job.

    Raises:
        RuntimeError: If the claim cannot be resolved after
            ``_CLAIM_MAX_ATTEMPTS`` attempts. This indicates a pathological
            churn pattern on the same workflow key and is exceedingly
            unlikely in practice.
    """
    jobs = _jobs(db)

    for attempt in range(_CLAIM_MAX_ATTEMPTS):
        now = _utcnow()
        fresh = JobRecord(
            job_id=str(uuid.uuid4()),
            workflow_key=workflow_key,
            status=JobStatus.PENDING,
            dcc=dcc,
            local_id=local_id,
            md5=md5,
            pipeline_version=pipeline_version,
            submitted_at=now,
            updated_at=now,
            file_meta_snapshot=file_meta_snapshot,
            # ``next_dispatch_at`` is intentionally left unset (None) on a
            # fresh claim. ``ensure_workflow`` dispatches the first attempt
            # inline; only if that attempt overflows (no worker capacity)
            # does it set ``next_dispatch_at``, handing the job to the
            # durable retry scheduler. A None value is never "due", so the
            # scheduler can't double-dispatch a job whose inline attempt is
            # still in flight.
        )

        # Attempt the insert first. If the partial-unique index admits it,
        # no stale row is blocking and there is nothing to reclaim. The
        # superseded_by chain is only annotated AFTER a successful insert
        # so the field always points at a record that exists in the DB —
        # a non-DuplicateKeyError failure of insert_one cannot strand a
        # stale row with a phantom successor.
        try:
            await jobs.insert_one(fresh.to_mongo())
            return fresh, True
        except DuplicateKeyError:
            pass

        # An active row blocks the insert. Try to reclaim it if stale —
        # mark FAILED without superseded_by; the chain is filled in below
        # only if we manage to insert a fresh row in this attempt.
        stale_cutoff = now - STALE_WORKFLOW_THRESHOLD
        reclaimed = await jobs.find_one_and_update(
            {
                "workflow_key": workflow_key,
                "status": {"$in": _ACTIVE_STATUS_VALUES},
                "updated_at": {"$lt": stale_cutoff},
            },
            {
                "$set": {
                    "status": JobStatus.FAILED.value,
                    # Clear the mutex discriminator in lockstep with the
                    # terminal status so the row leaves the partial-unique
                    # index and a fresh claim can succeed.
                    "active": False,
                    "error": "stale — reclaimed by later request",
                    "updated_at": now,
                }
            },
        )

        if reclaimed is None:
            # Not stale — a legitimate active worker holds the mutex. Attach.
            existing = await jobs.find_one(
                {
                    "workflow_key": workflow_key,
                    "status": {"$in": _ACTIVE_STATUS_VALUES},
                }
            )
            if existing is not None:
                return _record_from_mongo(existing), False
            # Active job terminated between our failed insert and the
            # re-read. Try once more.
            logger.debug(
                "Lost claim race on %s then terminal state observed — retrying "
                "(attempt %d/%d)",
                workflow_key,
                attempt + 1,
                _CLAIM_MAX_ATTEMPTS,
            )
            continue

        reclaimed_id = reclaimed.get("job_id")
        logger.warning(
            "Reclaimed stale workflow %s (job %s)",
            workflow_key,
            reclaimed_id,
        )

        # Stale row cleared. Retry the insert. On success, annotate the
        # reclaimed row's superseded_by so polling clients can follow the
        # chain to us.
        try:
            await jobs.insert_one(fresh.to_mongo())
        except DuplicateKeyError:
            # Another claimer raced us into the slot we cleared. Attach to
            # them and point the reclaimed row at the actual winner so the
            # chain remains correct.
            existing = await jobs.find_one(
                {
                    "workflow_key": workflow_key,
                    "status": {"$in": _ACTIVE_STATUS_VALUES},
                }
            )
            if existing is not None:
                winner = _record_from_mongo(existing)
                if reclaimed_id:
                    await jobs.update_one(
                        {"job_id": reclaimed_id},
                        {"$set": {"superseded_by": winner.job_id}},
                    )
                return winner, False
            # Winner terminated already — retry from scratch.
            continue

        if reclaimed_id:
            await jobs.update_one(
                {"job_id": reclaimed_id},
                {"$set": {"superseded_by": fresh.job_id}},
            )
        return fresh, True

    raise RuntimeError(
        f"Could not claim workflow {workflow_key} after {_CLAIM_MAX_ATTEMPTS} attempts"
    )


async def mark_running(db, job_id: str) -> None:
    """Transition a PENDING job to RUNNING.

    Raises ``RuntimeError`` if the row is no longer PENDING — typically
    because a parallel ``claim_workflow`` reclaimed it as stale and a
    successor caller now owns the workflow. The current task MUST treat
    this as a hand-off and bail without running the processor; otherwise
    it would race the successor on the cache write while doing useless
    duplicate work.
    """
    jobs = _jobs(db)
    now = _utcnow()
    result = await jobs.update_one(
        {"job_id": job_id, "status": JobStatus.PENDING.value},
        # PENDING→RUNNING stays active; re-assert ``active`` defensively so
        # the discriminator can never drift from the status.
        {"$set": {"status": JobStatus.RUNNING.value, "active": True, "updated_at": now}},
    )
    if getattr(result, "matched_count", 0) == 0:
        raise RuntimeError(
            f"mark_running rejected for job {job_id} — row no longer "
            f"PENDING (likely stale-reclaimed by a later request)"
        )


async def record_stage_complete(
    db,
    job_id: str,
    stage: str,
    artifact_kind: ArtifactKind,
    cache_key: str,
) -> None:
    """Append a completed stage and its artifact cache key to the job.

    Called by the executor after each stage commits its output to the
    cache. Enables partial-commit recovery: on a subsequent retry the
    processor skips stages already listed in ``stages_done``.

    Takes ``artifact_kind`` as the enum (not the string value) so the
    persisted ``artifact_cache_keys`` map cannot be silently keyed by an
    unexpected ``str(enum)`` form — convert at the call boundary.

    The update is fenced on an active status so that a late write from a
    worker whose job has already been stale-reclaimed by a new claimant
    cannot stomp on the successor's record.
    """
    jobs = _jobs(db)
    now = _utcnow()
    result = await jobs.update_one(
        {
            "job_id": job_id,
            "status": {"$in": _ACTIVE_STATUS_VALUES},
        },
        {
            "$addToSet": {"stages_done": stage},
            "$set": {
                f"artifact_cache_keys.{artifact_kind.value}": cache_key,
                "updated_at": now,
            },
        },
    )
    if getattr(result, "matched_count", 0) == 0:
        logger.warning(
            "record_stage_complete skipped for job %s — job no longer "
            "active (likely stale-reclaimed by a later request)",
            job_id,
        )


async def release_workflow(
    db,
    job_id: str,
    final_status: JobStatus,
    *,
    error: Optional[str] = None,
) -> None:
    """Transition a job to a terminal status, releasing the mutex.

    The transition is fenced on an active status so a worker that
    wakes up after its record has been stale-reclaimed cannot overwrite
    the successor's terminal state. A no-op release is logged.

    Args:
        db: Motor database handle.
        job_id: Job identifier.
        final_status: One of ``JobStatus.COMPLETED`` or ``JobStatus.FAILED``.
        error: Human-readable error message, included only for failures.
    """
    if final_status in ACTIVE_STATUSES:
        raise ValueError(
            f"release_workflow requires a terminal status, got {final_status}"
        )

    jobs = _jobs(db)
    now = _utcnow()
    update: dict[str, Any] = {
        "status": final_status.value,
        # Terminal status releases the mutex: drop the discriminator so the
        # row leaves the partial-unique index and is eligible for TTL reap.
        "active": False,
        "updated_at": now,
    }
    if error is not None:
        update["error"] = _scrub_error_text(error)
    result = await jobs.update_one(
        {
            "job_id": job_id,
            "status": {"$in": _ACTIVE_STATUS_VALUES},
        },
        {"$set": update},
    )
    if getattr(result, "matched_count", 0) == 0:
        logger.warning(
            "release_workflow no-op for job %s — job no longer active "
            "(likely stale-reclaimed by a later request)",
            job_id,
        )


async def get_job(db, job_id: str) -> Optional[JobRecord]:
    """Return the current persisted state of a job, or None if absent."""
    jobs = _jobs(db)
    doc = await jobs.find_one({"job_id": job_id})
    if doc is None:
        return None
    return _record_from_mongo(doc)


#: Cap a single progress-event value before persisting. Matches the
#: ``StringConstraints(max_length=256)`` on ``JobRecord.progress`` so the
#: model never has to reject a pre-clamped value.
_MAX_PROGRESS_LEN = 256


async def update_progress(db, job_id: str, value: str) -> None:
    """Set the free-form ``progress`` hint on an active job.

    Called by the executor's stream consumer in response to a
    :class:`~cfdb.workflows.events.Progress` event from the routine.
    No-op when the job is no longer active. The value is truncated to
    256 chars to match the JobRecord field cap.
    """
    jobs = _jobs(db)
    await jobs.update_one(
        {
            "job_id": job_id,
            "status": {"$in": _ACTIVE_STATUS_VALUES},
        },
        {
            "$set": {
                "progress": (value or "")[:_MAX_PROGRESS_LEN],
                "updated_at": _utcnow(),
            }
        },
    )


async def heartbeat_workflow(db, job_id: str) -> None:
    """Refresh ``updated_at`` on an active job to defer stale-reclaim.

    Called by the executor's stream consumer each time the routine emits
    a heartbeat event. No-op when the job is no longer active (the fence
    on ACTIVE_STATUSES drops the write if the row was stale-reclaimed or
    terminated underneath us).

    This is the primary liveness signal that lets ``STALE_WORKFLOW_THRESHOLD``
    drop from the original 1-hour conservative bound to a value driven by
    ``CFDB_WORKFLOW_HEARTBEAT_INTERVAL_S`` — a healthy long-running
    workflow keeps its row fresh without depending on stage transitions.
    """
    jobs = _jobs(db)
    await jobs.update_one(
        {
            "job_id": job_id,
            "status": {"$in": _ACTIVE_STATUS_VALUES},
        },
        {"$set": {"updated_at": _utcnow()}},
    )


# --- Bounded-concurrency admission + durable dispatch retry (issue #45) ------


async def count_active_workflows(db) -> int:
    """Count workflows currently holding the mutex (pending + running).

    Backs the admission ceiling in ``ensure_workflow``: once this reaches
    ``CFDB_WORKFLOW_MAX_ACTIVE`` new requests are shed with 429. Soft by
    nature — a count-then-insert race can briefly overshoot the cap, which
    is acceptable for a flood guard.

    Counts on the canonical ``active`` boolean discriminator (the single
    source of truth every other jobs read/write/index keys off), not the
    derived ``status $in ACTIVE_STATUSES`` view, so the ceiling and the
    mutex stay in lockstep even if the two ever drift.
    """
    jobs = _jobs(db)
    return await jobs.count_documents({"active": True})


async def reschedule_dispatch(db, job_id: str, *, next_at: datetime) -> None:
    """Defer a still-PENDING job's next dispatch attempt to ``next_at``.

    Called when a dispatch attempt finds no worker capacity: the job stays
    PENDING and the durable scheduler re-attempts it at ``next_at``.
    Fenced on PENDING so a job that has since gone RUNNING/terminal (a
    racing attempt won the worker, or it was stale-reclaimed) is not
    dragged back into the dispatch queue. Bumps ``dispatch_attempts`` for
    observability.
    """
    jobs = _jobs(db)
    result = await jobs.update_one(
        {"job_id": job_id, "status": JobStatus.PENDING.value},
        {
            "$set": {"next_dispatch_at": next_at, "updated_at": _utcnow()},
            "$inc": {"dispatch_attempts": 1},
        },
    )
    if getattr(result, "matched_count", 0) == 0:
        logger.debug(
            "reschedule_dispatch no-op for job %s — row no longer PENDING "
            "(won a worker, terminated, or was stale-reclaimed)",
            job_id,
        )


async def lease_due_dispatch(
    db, *, now: datetime, next_at: datetime
) -> Optional[JobRecord]:
    """Atomically claim one PENDING job whose dispatch is due.

    Selects a PENDING job with ``next_dispatch_at <= now`` and, in the same
    operation, pushes its ``next_dispatch_at`` forward to ``next_at`` — so a
    concurrent scheduler tick (or a second API replica) cannot lease the
    same job, and a crash mid-attempt still leaves the job scheduled for a
    later retry. Returns the leased job, or ``None`` when nothing is due.

    The caller runs a dispatch attempt for the returned job; that attempt
    either wins a worker (``mark_running``) or calls ``reschedule_dispatch``
    again on overflow. Jobs with ``next_dispatch_at`` unset (``None``) are
    never due, so they are not leased here.

    Due jobs are leased oldest-first (``next_dispatch_at`` ascending) so
    sustained overflow cannot starve the longest-waiting job toward its
    deadline. ``next_at`` MUST be strictly in the future relative to
    ``now``: the single-claim guarantee depends on the leased row's
    ``next_dispatch_at`` moving out of the ``<= now`` window, so an
    equal/past value would let a concurrent tick re-lease the same job
    before the attempt resolves.
    """
    if next_at <= now:
        raise ValueError(
            f"lease_due_dispatch requires next_at ({next_at}) strictly after "
            f"now ({now}); an equal/past value would let a concurrent tick "
            "immediately re-lease the same job"
        )
    jobs = _jobs(db)
    doc = await jobs.find_one_and_update(
        {
            "status": JobStatus.PENDING.value,
            "next_dispatch_at": {"$lte": now},
        },
        {"$set": {"next_dispatch_at": next_at, "updated_at": now}},
        sort=[("next_dispatch_at", 1)],
        return_document=ReturnDocument.AFTER,
    )
    return _record_from_mongo(doc) if doc is not None else None


async def requeue_orphaned_dispatch(
    db, *, now: datetime, stale_before: datetime
) -> int:
    """Re-queue jobs orphaned by an API/worker crash so they re-dispatch.

    The durable scheduler only leases ``PENDING`` jobs whose
    ``next_dispatch_at`` is set and due, so two states survive a restart
    that it would never pick up on its own:

    - ``RUNNING`` rows whose API consumer died mid-stream (no heartbeat
      since), and
    - ``PENDING`` rows with ``next_dispatch_at`` unset — a fresh claim whose
      inline first attempt never got to reschedule before the crash.

    Both are reset to ``PENDING`` with ``next_dispatch_at = now`` so the next
    scheduler tick leases them. Gated on ``updated_at < stale_before`` (the
    same staleness threshold :func:`claim_workflow` uses) so a healthy
    in-flight job — which keeps ``updated_at`` fresh via heartbeats — is
    never falsely reclaimed. Returns the number of rows requeued.

    Unlike ``claim_workflow``'s stale-reclaim, which fails the stale row so a
    *new* claimant can supersede it, this revives the same row: there is no
    new claimant, so the goal is to recover the in-flight work itself.
    Partial-commit recovery means the re-dispatched job reuses any stages
    already committed to cache.
    """
    jobs = _jobs(db)
    result = await jobs.update_many(
        {
            "active": True,
            "updated_at": {"$lt": stale_before},
            "$or": [
                {"status": JobStatus.RUNNING.value},
                {"status": JobStatus.PENDING.value, "next_dispatch_at": None},
            ],
        },
        {
            "$set": {
                "status": JobStatus.PENDING.value,
                # PENDING is active; re-assert defensively so the
                # discriminator can never drift from the status.
                "active": True,
                "next_dispatch_at": now,
                "updated_at": now,
            }
        },
    )
    return getattr(result, "modified_count", 0)
