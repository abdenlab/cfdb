"""Tests for the workflow mutex (claim/release and partial-unique semantics)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from cfdb.workflows import lock
from cfdb.workflows.models import ACTIVE_STATUSES, ArtifactKind, JobRecord, JobStatus
from tests.test_workflows import FIXTURE_MD5

PIPELINE_VERSION = 1


def _insert_job(
    mock_db,
    *,
    workflow_key: str,
    status: JobStatus,
    next_dispatch_at: datetime | None = None,
    dispatch_attempts: int = 0,
    updated_at: datetime | None = None,
) -> JobRecord:
    """Insert a crafted job doc into the fake collection and return it."""
    now = datetime.now(timezone.utc)
    record = JobRecord(
        job_id=f"job-{workflow_key}",
        workflow_key=workflow_key,
        status=status,
        dcc="encode",
        local_id="x",
        md5=FIXTURE_MD5,
        pipeline_version=PIPELINE_VERSION,
        submitted_at=now,
        updated_at=updated_at or now,
        next_dispatch_at=next_dispatch_at,
        dispatch_attempts=dispatch_attempts,
    )
    mock_db.jobs.docs.append(record.to_mongo())
    return record


def _install_jobs_index(mock_db) -> None:
    """Register the partial unique index on the FakeDB jobs collection."""
    mock_db.jobs.create_index(
        {"workflow_key": 1},
        unique=True,
        partialFilterExpression={
            "status": {"$in": [s.value for s in ACTIVE_STATUSES]}
        },
    )


class TestClaimWorkflow:
    @pytest.mark.asyncio
    async def test_claim_workflow_should_insert_and_return_fresh_record(
        self, mock_db
    ):
        """Test that the first claimant inserts a new active job.

        Given:
            An empty jobs collection with the partial unique index
            registered.
        When:
            claim_workflow is awaited for a new workflow_key.
        Then:
            It should return a JobRecord with status PENDING and a True
            fresh flag, and the doc should be persisted.
        """
        # Arrange
        _install_jobs_index(mock_db)

        # Act
        record, fresh = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )

        # Assert
        assert fresh is True
        assert record.status == JobStatus.PENDING
        assert record.workflow_key == "encode/x/aa/v1"
        assert len(mock_db.jobs.docs) == 1

    @pytest.mark.asyncio
    async def test_claim_workflow_should_attach_when_active_job_exists(
        self, mock_db
    ):
        """Test that a second claimant attaches rather than duplicating.

        Given:
            One active (PENDING) job for workflow_key W.
        When:
            claim_workflow is awaited again for W.
        Then:
            It should return the existing job_id with fresh=False and
            leave the collection size unchanged.
        """
        # Arrange
        _install_jobs_index(mock_db)
        first_record, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )

        # Act
        second_record, fresh = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )

        # Assert
        assert fresh is False
        assert second_record.job_id == first_record.job_id
        assert len(mock_db.jobs.docs) == 1

    @pytest.mark.asyncio
    async def test_claim_workflow_should_succeed_after_previous_completes(
        self, mock_db
    ):
        """Test that a new fresh claim follows release of the prior job.

        Given:
            One completed job for workflow_key W (status COMPLETED; outside
            the partial unique index's filter).
        When:
            claim_workflow is awaited for W.
        Then:
            It should insert a new active record since the completed one
            no longer participates in the unique index.
        """
        # Arrange
        _install_jobs_index(mock_db)
        first, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )
        await lock.release_workflow(mock_db, first.job_id, JobStatus.COMPLETED)

        # Act
        second, fresh = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )

        # Assert
        assert fresh is True
        assert second.job_id != first.job_id
        assert len(mock_db.jobs.docs) == 2

    @pytest.mark.asyncio
    async def test_claim_workflow_should_reclaim_stale_active_records(
        self, mock_db
    ):
        """Test that stale active records are flipped to FAILED and replaced.

        Given:
            An active job whose updated_at exceeds the stale threshold —
            simulating a crashed worker that never released the mutex.
        When:
            claim_workflow is awaited for the same workflow_key.
        Then:
            The stale record should be marked FAILED and a new fresh claim
            should succeed.
        """
        # Arrange
        _install_jobs_index(mock_db)
        first, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )
        stale_ts = (
            datetime.now(timezone.utc)
            - lock.STALE_WORKFLOW_THRESHOLD
            - timedelta(minutes=5)
        )
        for d in mock_db.jobs.docs:
            if d["job_id"] == first.job_id:
                d["updated_at"] = stale_ts

        # Act
        second, fresh = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )

        # Assert
        assert fresh is True
        statuses = {d["status"] for d in mock_db.jobs.docs}
        assert JobStatus.FAILED.value in statuses
        assert second.status == JobStatus.PENDING


class TestMarkRunning:
    @pytest.mark.asyncio
    async def test_mark_running_should_raise_when_row_no_longer_pending(
        self, mock_db
    ):
        """Test that mark_running rejects hand-off rows.

        Given:
            A job whose row has been flipped to FAILED out-of-band (e.g.,
            stale-reclaimed by a successor caller).
        When:
            mark_running is awaited with its job_id.
        Then:
            It should raise RuntimeError so the worker bails out instead
            of racing the successor on the cache write.
        """
        # Arrange
        _install_jobs_index(mock_db)
        record, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )
        # Simulate a successor reclaim flipping the row terminal.
        await lock.release_workflow(
            mock_db, record.job_id, JobStatus.FAILED, error="reclaimed"
        )

        # Act & assert
        with pytest.raises(RuntimeError, match="no longer"):
            await lock.mark_running(mock_db, record.job_id)

    @pytest.mark.asyncio
    async def test_mark_running_should_advance_pending_to_running(self, mock_db):
        """Test that mark_running transitions PENDING jobs to RUNNING.

        Given:
            A freshly-claimed PENDING job.
        When:
            mark_running is awaited with its job_id.
        Then:
            The stored status should become RUNNING.
        """
        # Arrange
        _install_jobs_index(mock_db)
        record, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )

        # Act
        await lock.mark_running(mock_db, record.job_id)

        # Assert
        after = await lock.get_job(mock_db, record.job_id)
        assert after is not None
        assert after.status == JobStatus.RUNNING


class TestRecordStageComplete:
    @pytest.mark.asyncio
    async def test_record_stage_complete_should_append_stage_and_cache_key(
        self, mock_db
    ):
        """Test that per-stage commits accumulate on the job record.

        Given:
            A RUNNING job.
        When:
            record_stage_complete is awaited for stage "sort_bam".
        Then:
            The job's stages_done should include "sort_bam" and
            artifact_cache_keys["data"] should map to the provided key.
        """
        # Arrange
        _install_jobs_index(mock_db)
        record, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )
        await lock.mark_running(mock_db, record.job_id)

        # Act
        await lock.record_stage_complete(
            mock_db,
            record.job_id,
            stage="sort_bam",
            artifact_kind=ArtifactKind.DATA,
            cache_key="encode/x/data/aa-v0",
        )

        # Assert
        persisted = await lock.get_job(mock_db, record.job_id)
        assert persisted is not None
        assert "sort_bam" in persisted.stages_done
        assert persisted.artifact_cache_keys["data"] == "encode/x/data/aa-v0"


class TestReleaseWorkflow:
    @pytest.mark.asyncio
    async def test_release_workflow_should_transition_to_completed(self, mock_db):
        """Test that a successful release marks the job COMPLETED.

        Given:
            A running job.
        When:
            release_workflow is awaited with JobStatus.COMPLETED.
        Then:
            Status should become COMPLETED and the mutex be freed.
        """
        # Arrange
        _install_jobs_index(mock_db)
        record, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )

        # Act
        await lock.release_workflow(mock_db, record.job_id, JobStatus.COMPLETED)

        # Assert
        after = await lock.get_job(mock_db, record.job_id)
        assert after is not None
        assert after.status == JobStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_release_workflow_should_carry_error_on_failed_release(
        self, mock_db
    ):
        """Test that failure releases persist an error message.

        Given:
            A running job.
        When:
            release_workflow is awaited with JobStatus.FAILED and an error.
        Then:
            The persisted record should carry that error string.
        """
        # Arrange
        _install_jobs_index(mock_db)
        record, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )

        # Act
        await lock.release_workflow(
            mock_db, record.job_id, JobStatus.FAILED, error="samtools died"
        )

        # Assert
        after = await lock.get_job(mock_db, record.job_id)
        assert after is not None
        assert after.status == JobStatus.FAILED
        assert after.error == "samtools died"

    @pytest.mark.asyncio
    async def test_release_workflow_should_reject_active_final_status(
        self, mock_db
    ):
        """Test that release refuses to transition to an active state.

        Given:
            Any running job.
        When:
            release_workflow is awaited with JobStatus.PENDING.
        Then:
            It should raise ValueError because release only moves jobs to
            a terminal status.
        """
        # Arrange
        _install_jobs_index(mock_db)
        record, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )

        # Act & assert
        with pytest.raises(ValueError, match="terminal"):
            await lock.release_workflow(mock_db, record.job_id, JobStatus.PENDING)


class TestReleaseWorkflowAfterReclaim:
    @pytest.mark.asyncio
    async def test_release_workflow_should_noop_when_job_already_reclaimed(
        self, mock_db
    ):
        """Test that a late release does not stomp on a reclaimed job.

        Given:
            A job that has been stale-reclaimed and is already FAILED.
        When:
            release_workflow is awaited for that job_id with COMPLETED.
        Then:
            The persisted status should remain FAILED — the late writer
            cannot overwrite a successor claimant's view of the record.
        """
        # Arrange
        _install_jobs_index(mock_db)
        record, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )
        # Simulate the reclaim having already occurred.
        for d in mock_db.jobs.docs:
            if d["job_id"] == record.job_id:
                d["status"] = JobStatus.FAILED.value
                d["error"] = "stale — reclaimed by later request"

        # Act
        await lock.release_workflow(mock_db, record.job_id, JobStatus.COMPLETED)

        # Assert
        persisted = await lock.get_job(mock_db, record.job_id)
        assert persisted is not None
        assert persisted.status == JobStatus.FAILED
        assert persisted.error == "stale — reclaimed by later request"

    @pytest.mark.asyncio
    async def test_record_stage_complete_should_noop_when_job_already_reclaimed(
        self, mock_db
    ):
        """Test that a late stage commit cannot mutate a reclaimed job.

        Given:
            A job whose status was flipped to FAILED by reclaim logic.
        When:
            record_stage_complete is awaited for its job_id.
        Then:
            The job's stages_done and artifact_cache_keys should remain
            unchanged.
        """
        # Arrange
        _install_jobs_index(mock_db)
        record, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )
        for d in mock_db.jobs.docs:
            if d["job_id"] == record.job_id:
                d["status"] = JobStatus.FAILED.value

        # Act
        await lock.record_stage_complete(
            mock_db,
            record.job_id,
            stage="data",
            artifact_kind=ArtifactKind.DATA,
            cache_key="encode/x/data/aa-v0",
        )

        # Assert
        persisted = await lock.get_job(mock_db, record.job_id)
        assert persisted is not None
        assert persisted.stages_done == []
        assert persisted.artifact_cache_keys == {}


class TestScrubErrorText:
    def test_scrub_error_text_should_replace_absolute_path_with_placeholder(self):
        """Test that absolute filesystem paths are scrubbed.

        Given:
            An error string containing ``/tmp/jobs/abc/scratch/source.bam``.
        When:
            ``_scrub_error_text`` is called.
        Then:
            The returned string should replace the absolute path with
            ``<path>``.
        """
        # Arrange
        raw = "failed in /tmp/jobs/abc/scratch/source.bam during sort"

        # Act
        cleaned = lock._scrub_error_text(raw)

        # Assert
        assert "<path>" in cleaned
        assert "/tmp/jobs/abc/scratch/source.bam" not in cleaned

    def test_scrub_error_text_should_truncate_long_input(self):
        """Test that the length cap fires on oversized error text.

        Given:
            An error string of length 2048.
        When:
            ``_scrub_error_text`` is called.
        Then:
            The returned string should be at most 1024 chars.
        """
        # Arrange
        raw = "x" * 2048

        # Act
        cleaned = lock._scrub_error_text(raw)

        # Assert
        assert cleaned is not None
        assert len(cleaned) <= 1024

    def test_scrub_error_text_should_preserve_negative_case_tokens(self):
        """Test that the regex anchors preserve legitimate-looking tokens.

        Given:
            An error string containing ``HTTP/1.1`` and ``signal/SIGTERM``.
        When:
            ``_scrub_error_text`` is called.
        Then:
            Both tokens should appear verbatim in the result — the
            scrubber's regex is anchored to multi-segment paths starting
            with a letter so these tokens are not mistakenly stripped.
        """
        # Arrange
        raw = "request: HTTP/1.1; killed by signal/SIGTERM"

        # Act
        cleaned = lock._scrub_error_text(raw)

        # Assert
        assert "HTTP/1.1" in cleaned
        assert "signal/SIGTERM" in cleaned

    def test_scrub_error_text_should_return_none_when_input_none(self):
        """Test that None is passed through.

        Given:
            A None input.
        When:
            ``_scrub_error_text`` is called.
        Then:
            It should return None.
        """
        # Act
        result = lock._scrub_error_text(None)

        # Assert
        assert result is None


class TestUpdateProgress:
    @pytest.mark.asyncio
    async def test_update_progress_should_persist_value_on_active_job(self, mock_db):
        """Test that update_progress writes to an active row.

        Given:
            A claimed active job.
        When:
            ``update_progress(db, job_id, "merging chunks")`` is awaited.
        Then:
            The persisted record's ``progress`` should equal the supplied
            value.
        """
        # Arrange
        _install_jobs_index(mock_db)
        record, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )

        # Act
        await lock.update_progress(mock_db, record.job_id, "merging chunks")

        # Assert
        persisted = await lock.get_job(mock_db, record.job_id)
        assert persisted is not None
        assert persisted.progress == "merging chunks"

    @pytest.mark.asyncio
    async def test_update_progress_should_noop_on_failed_job(self, mock_db):
        """Test that progress writes are fenced on active status.

        Given:
            A job that has already reached FAILED.
        When:
            ``update_progress`` is awaited.
        Then:
            The persisted record's ``progress`` should remain unchanged —
            the ``$in: ACTIVE_STATUSES`` predicate drops the write.
        """
        # Arrange
        _install_jobs_index(mock_db)
        record, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )
        # Flip the row terminal out-of-band.
        for d in mock_db.jobs.docs:
            if d["job_id"] == record.job_id:
                d["status"] = JobStatus.FAILED.value
                d["progress"] = None

        # Act
        await lock.update_progress(mock_db, record.job_id, "should-not-land")

        # Assert
        persisted = await lock.get_job(mock_db, record.job_id)
        assert persisted is not None
        assert persisted.progress is None

    @pytest.mark.asyncio
    async def test_update_progress_should_truncate_long_value(self, mock_db):
        """Test that update_progress caps the persisted value at 256 chars.

        Given:
            An active job and a 512-char progress value.
        When:
            ``update_progress`` is awaited.
        Then:
            The persisted ``progress`` field should be truncated to 256
            chars to match the JobRecord field cap.
        """
        # Arrange
        _install_jobs_index(mock_db)
        record, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )
        long_value = "p" * 512

        # Act
        await lock.update_progress(mock_db, record.job_id, long_value)

        # Assert
        persisted = await lock.get_job(mock_db, record.job_id)
        assert persisted is not None
        assert persisted.progress is not None
        assert len(persisted.progress) == 256


class TestHeartbeatWorkflow:
    @pytest.mark.asyncio
    async def test_heartbeat_workflow_should_bump_updated_at_on_active_job(
        self, mock_db
    ):
        """Test that heartbeat refreshes ``updated_at`` on an active job.

        Given:
            A claimed active job.
        When:
            ``heartbeat_workflow`` is awaited.
        Then:
            The persisted record's ``updated_at`` should be bumped past
            its initial value.
        """
        # Arrange
        _install_jobs_index(mock_db)
        record, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )
        # Force an obviously-old updated_at so the heartbeat write is
        # detectably newer.
        stale_ts = datetime.now(timezone.utc) - timedelta(minutes=30)
        for d in mock_db.jobs.docs:
            if d["job_id"] == record.job_id:
                d["updated_at"] = stale_ts

        # Act
        await lock.heartbeat_workflow(mock_db, record.job_id)

        # Assert
        persisted = await lock.get_job(mock_db, record.job_id)
        assert persisted is not None
        assert persisted.updated_at > stale_ts

    @pytest.mark.asyncio
    async def test_heartbeat_workflow_should_noop_on_completed_job(self, mock_db):
        """Test that heartbeats do not refresh a terminal job.

        Given:
            A COMPLETED job.
        When:
            ``heartbeat_workflow`` is awaited.
        Then:
            The persisted ``updated_at`` should remain unchanged.
        """
        # Arrange
        _install_jobs_index(mock_db)
        record, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )
        await lock.release_workflow(mock_db, record.job_id, JobStatus.COMPLETED)
        persisted_before = await lock.get_job(mock_db, record.job_id)
        assert persisted_before is not None
        baseline = persisted_before.updated_at

        # Act
        await lock.heartbeat_workflow(mock_db, record.job_id)

        # Assert
        persisted_after = await lock.get_job(mock_db, record.job_id)
        assert persisted_after is not None
        assert persisted_after.updated_at == baseline


class TestRecordFromMongo:
    def test_record_from_mongo_should_attach_utc_to_naive_timestamps(self):
        """Test that naive datetimes are re-attached to UTC during hydration.

        Given:
            A Mongo doc whose ``submitted_at`` and ``updated_at`` are
            naive datetimes (the BSON Date round-trip on some Motor /
            mongomock-motor variants).
        When:
            ``_record_from_mongo`` is called.
        Then:
            The resulting JobRecord's datetimes should carry
            ``tzinfo=UTC``.
        """
        # Arrange
        naive = datetime(2026, 4, 21, 12, 0, 0)
        doc = {
            "_id": "discarded",
            "job_id": "job-abc",
            "workflow_key": f"encode/x/{FIXTURE_MD5}/v1",
            "status": JobStatus.RUNNING.value,
            "dcc": "encode",
            "local_id": "x",
            "md5": FIXTURE_MD5,
            "pipeline_version": 1,
            "submitted_at": naive,
            "updated_at": naive,
            "stages_done": [],
            "artifact_cache_keys": {},
        }

        # Act
        record = lock._record_from_mongo(doc)

        # Assert
        assert record.submitted_at.tzinfo is timezone.utc
        assert record.updated_at.tzinfo is timezone.utc


class TestClaimWorkflowSupersededByChain:
    @pytest.mark.asyncio
    async def test_claim_workflow_should_annotate_superseded_by_after_reclaim(
        self, mock_db
    ):
        """Test that a reclaimed row gets a pointer to the new winner.

        Given:
            A stale active row that is reclaimed and a fresh row inserted.
        When:
            ``claim_workflow`` is awaited.
        Then:
            The reclaimed row's ``superseded_by`` field should equal the
            new winner's ``job_id`` so polling clients can follow the
            chain.
        """
        # Arrange
        _install_jobs_index(mock_db)
        first, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )
        stale_ts = (
            datetime.now(timezone.utc)
            - lock.STALE_WORKFLOW_THRESHOLD
            - timedelta(minutes=5)
        )
        for d in mock_db.jobs.docs:
            if d["job_id"] == first.job_id:
                d["updated_at"] = stale_ts

        # Act
        winner, fresh = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )

        # Assert
        assert fresh is True
        reclaimed_doc = next(
            d for d in mock_db.jobs.docs if d["job_id"] == first.job_id
        )
        assert reclaimed_doc.get("superseded_by") == winner.job_id


class TestGetJob:
    @pytest.mark.asyncio
    async def test_get_job_should_return_none_when_job_absent(self, mock_db):
        """Test that unknown job ids resolve to None.

        Given:
            An empty jobs collection.
        When:
            get_job is awaited with an unknown id.
        Then:
            It should return None.
        """
        # Act & assert
        assert await lock.get_job(mock_db, "not-a-job") is None

    @pytest.mark.asyncio
    async def test_get_job_should_hydrate_persisted_record(self, mock_db):
        """Test that get_job reconstructs the JobRecord from the Mongo doc.

        Given:
            One persisted job.
        When:
            get_job is awaited with its id.
        Then:
            It should return a JobRecord whose core fields match.
        """
        # Arrange
        _install_jobs_index(mock_db)
        record, _ = await lock.claim_workflow(
            mock_db,
            "encode/x/aa/v1",
            dcc="encode",
            local_id="x",
            md5=FIXTURE_MD5,
            pipeline_version=PIPELINE_VERSION,
        )

        # Act
        hydrated = await lock.get_job(mock_db, record.job_id)

        # Assert
        assert hydrated is not None
        assert hydrated.job_id == record.job_id
        assert hydrated.workflow_key == record.workflow_key


class TestCountActiveWorkflows:
    @pytest.mark.asyncio
    async def test_count_active_workflows_should_count_pending_and_running(
        self, mock_db
    ):
        """Test that the admission count includes only active jobs.

        Given:
            Two pending, one running, and two terminal job rows.
        When:
            count_active_workflows is awaited.
        Then:
            It should return 3 — pending + running only, excluding the
            terminal rows.
        """
        # Arrange
        _insert_job(mock_db, workflow_key="a", status=JobStatus.PENDING)
        _insert_job(mock_db, workflow_key="b", status=JobStatus.PENDING)
        _insert_job(mock_db, workflow_key="c", status=JobStatus.RUNNING)
        _insert_job(mock_db, workflow_key="d", status=JobStatus.COMPLETED)
        _insert_job(mock_db, workflow_key="e", status=JobStatus.FAILED)

        # Act
        count = await lock.count_active_workflows(mock_db)

        # Assert
        assert count == 3

    @pytest.mark.asyncio
    async def test_count_active_workflows_should_return_zero_when_empty(self, mock_db):
        """Test that an empty collection counts as zero active workflows.

        Given:
            A jobs collection with no rows.
        When:
            count_active_workflows is awaited.
        Then:
            It should return 0.
        """
        # Act
        count = await lock.count_active_workflows(mock_db)

        # Assert
        assert count == 0


class TestRescheduleDispatch:
    @pytest.mark.asyncio
    async def test_reschedule_dispatch_should_defer_pending_and_bump_attempts(
        self, mock_db
    ):
        """Test that rescheduling a pending job advances its retry time.

        Given:
            A pending job with no next_dispatch_at and zero attempts.
        When:
            reschedule_dispatch is awaited with a future next_at.
        Then:
            It should set next_dispatch_at to that time and increment
            dispatch_attempts.
        """
        # Arrange
        _insert_job(mock_db, workflow_key="a", status=JobStatus.PENDING)
        next_at = datetime.now(timezone.utc) + timedelta(seconds=120)

        # Act
        await lock.reschedule_dispatch(mock_db, "job-a", next_at=next_at)

        # Assert
        doc = mock_db.jobs.docs[0]
        assert doc["next_dispatch_at"] == next_at
        assert doc["dispatch_attempts"] == 1

    @pytest.mark.asyncio
    async def test_reschedule_dispatch_should_be_noop_when_not_pending(self, mock_db):
        """Test that rescheduling a non-pending job changes nothing.

        Given:
            A running job (it already won a worker).
        When:
            reschedule_dispatch is awaited for it.
        Then:
            It should leave next_dispatch_at and dispatch_attempts untouched,
            because the update is fenced on PENDING.
        """
        # Arrange
        _insert_job(mock_db, workflow_key="a", status=JobStatus.RUNNING)
        next_at = datetime.now(timezone.utc) + timedelta(seconds=120)

        # Act
        await lock.reschedule_dispatch(mock_db, "job-a", next_at=next_at)

        # Assert
        doc = mock_db.jobs.docs[0]
        assert doc["next_dispatch_at"] is None
        assert doc["dispatch_attempts"] == 0


class TestLeaseDueDispatch:
    @pytest.mark.asyncio
    async def test_lease_due_dispatch_should_claim_due_job_and_push_forward(
        self, mock_db
    ):
        """Test that a due pending job is leased and its next attempt deferred.

        Given:
            A pending job whose next_dispatch_at is in the past.
        When:
            lease_due_dispatch is awaited with now past that time and a
            future next_at.
        Then:
            It should return the job and push its next_dispatch_at forward to
            next_at so a later tick won't re-lease it.
        """
        # Arrange
        now = datetime.now(timezone.utc)
        _insert_job(
            mock_db,
            workflow_key="a",
            status=JobStatus.PENDING,
            next_dispatch_at=now - timedelta(seconds=1),
        )
        next_at = now + timedelta(seconds=120)

        # Act
        leased = await lock.lease_due_dispatch(mock_db, now=now, next_at=next_at)

        # Assert
        assert leased is not None
        assert leased.workflow_key == "a"
        assert mock_db.jobs.docs[0]["next_dispatch_at"] == next_at

    @pytest.mark.asyncio
    async def test_lease_due_dispatch_should_not_re_lease_within_lease_window(
        self, mock_db
    ):
        """Test that a freshly-leased job is not immediately re-leasable.

        Given:
            One due pending job that has just been leased (its
            next_dispatch_at pushed into the future).
        When:
            lease_due_dispatch is awaited again at the same now.
        Then:
            It should return None — the single-claim guard, so two ticks or
            replicas cannot double-dispatch one job.
        """
        # Arrange
        now = datetime.now(timezone.utc)
        _insert_job(
            mock_db,
            workflow_key="a",
            status=JobStatus.PENDING,
            next_dispatch_at=now - timedelta(seconds=1),
        )
        next_at = now + timedelta(seconds=120)
        first = await lock.lease_due_dispatch(mock_db, now=now, next_at=next_at)

        # Act
        second = await lock.lease_due_dispatch(mock_db, now=now, next_at=next_at)

        # Assert
        assert first is not None
        assert second is None

    @pytest.mark.asyncio
    async def test_lease_due_dispatch_should_skip_unscheduled_job(self, mock_db):
        """Test that a job with next_dispatch_at unset is never leased.

        Given:
            A pending job whose next_dispatch_at is None (freshly claimed,
            not yet deferred by the scheduler).
        When:
            lease_due_dispatch is awaited.
        Then:
            It should return None — an unscheduled job is not due.
        """
        # Arrange
        now = datetime.now(timezone.utc)
        _insert_job(
            mock_db, workflow_key="a", status=JobStatus.PENDING, next_dispatch_at=None
        )

        # Act
        leased = await lock.lease_due_dispatch(
            mock_db, now=now, next_at=now + timedelta(seconds=120)
        )

        # Assert
        assert leased is None

    @pytest.mark.asyncio
    async def test_lease_due_dispatch_should_skip_future_and_running_jobs(
        self, mock_db
    ):
        """Test that not-yet-due and non-pending jobs are not leased.

        Given:
            A pending job due in the future and a running job already past
            its time.
        When:
            lease_due_dispatch is awaited.
        Then:
            It should return None — the future job is not due and the running
            job is not pending.
        """
        # Arrange
        now = datetime.now(timezone.utc)
        _insert_job(
            mock_db,
            workflow_key="future",
            status=JobStatus.PENDING,
            next_dispatch_at=now + timedelta(seconds=60),
        )
        _insert_job(
            mock_db,
            workflow_key="running",
            status=JobStatus.RUNNING,
            next_dispatch_at=now - timedelta(seconds=60),
        )

        # Act
        leased = await lock.lease_due_dispatch(
            mock_db, now=now, next_at=now + timedelta(seconds=120)
        )

        # Assert
        assert leased is None

    @pytest.mark.asyncio
    async def test_lease_due_dispatch_should_lease_oldest_due_job_first(self, mock_db):
        """Test that the longest-waiting due job is leased first.

        Given:
            Two due pending jobs with different next_dispatch_at times,
            inserted newest-first.
        When:
            lease_due_dispatch is awaited.
        Then:
            It should lease the one with the earliest next_dispatch_at, so
            sustained overflow cannot starve the oldest job.
        """
        # Arrange
        now = datetime.now(timezone.utc)
        _insert_job(
            mock_db,
            workflow_key="newer",
            status=JobStatus.PENDING,
            next_dispatch_at=now - timedelta(seconds=10),
        )
        _insert_job(
            mock_db,
            workflow_key="older",
            status=JobStatus.PENDING,
            next_dispatch_at=now - timedelta(seconds=300),
        )

        # Act
        leased = await lock.lease_due_dispatch(
            mock_db, now=now, next_at=now + timedelta(seconds=120)
        )

        # Assert
        assert leased is not None
        assert leased.workflow_key == "older"

    @pytest.mark.asyncio
    async def test_lease_due_dispatch_should_reject_next_at_not_in_future(
        self, mock_db
    ):
        """Test that a non-future next_at is rejected.

        Given:
            A now timestamp and a next_at equal to it.
        When:
            lease_due_dispatch is awaited.
        Then:
            It should raise ValueError — an equal/past next_at would let a
            concurrent tick immediately re-lease the same job.
        """
        # Arrange
        now = datetime.now(timezone.utc)

        # Act & assert
        with pytest.raises(ValueError, match="strictly after"):
            await lock.lease_due_dispatch(mock_db, now=now, next_at=now)


class TestRequeueOrphanedDispatch:
    @pytest.mark.asyncio
    async def test_should_requeue_a_stale_running_orphan(self, mock_db):
        """Test that a RUNNING job with no recent heartbeat is re-queued.

        Given:
            A RUNNING job whose `updated_at` is older than the stale
            threshold — an in-flight job orphaned when its API consumer
            died mid-stream.
        When:
            requeue_orphaned_dispatch runs with that staleness cutoff.
        Then:
            The job is reset to PENDING with `next_dispatch_at` set so the
            scheduler re-dispatches it, and the call reports one requeued
            row.
        """
        # Arrange
        now = datetime.now(timezone.utc)
        stale = now - timedelta(seconds=2000)
        _insert_job(
            mock_db, workflow_key="a", status=JobStatus.RUNNING, updated_at=stale
        )

        # Act
        count = await lock.requeue_orphaned_dispatch(
            mock_db, now=now, stale_before=now - timedelta(seconds=900)
        )

        # Assert
        assert count == 1
        job = await lock.get_job(mock_db, "job-a")
        assert job is not None
        assert job.status == JobStatus.PENDING
        assert job.next_dispatch_at == now

    @pytest.mark.asyncio
    async def test_should_requeue_a_stale_pending_job_with_no_next_dispatch(
        self, mock_db
    ):
        """Test that a stale PENDING job with unset next_dispatch_at is re-queued.

        Given:
            A PENDING job with `next_dispatch_at` unset (a fresh claim whose
            inline attempt never rescheduled) and a stale `updated_at`.
        When:
            requeue_orphaned_dispatch runs.
        Then:
            Its `next_dispatch_at` is set to now so the scheduler — which
            never leases a null-`next_dispatch_at` row — can pick it up.
        """
        # Arrange
        now = datetime.now(timezone.utc)
        _insert_job(
            mock_db,
            workflow_key="a",
            status=JobStatus.PENDING,
            next_dispatch_at=None,
            updated_at=now - timedelta(seconds=2000),
        )

        # Act
        count = await lock.requeue_orphaned_dispatch(
            mock_db, now=now, stale_before=now - timedelta(seconds=900)
        )

        # Assert
        assert count == 1
        job = await lock.get_job(mock_db, "job-a")
        assert job is not None
        assert job.status == JobStatus.PENDING
        assert job.next_dispatch_at == now

    @pytest.mark.asyncio
    async def test_should_not_touch_a_fresh_running_job(self, mock_db):
        """Test that a healthy, recently-heartbeating RUNNING job is left alone.

        Given:
            A RUNNING job whose `updated_at` is recent (within the stale
            threshold), i.e. a healthy in-flight job.
        When:
            requeue_orphaned_dispatch runs.
        Then:
            It is not touched — staleness gating prevents the sweep from
            yanking a live job away from its worker.
        """
        # Arrange
        now = datetime.now(timezone.utc)
        _insert_job(
            mock_db,
            workflow_key="a",
            status=JobStatus.RUNNING,
            updated_at=now - timedelta(seconds=10),
        )

        # Act
        count = await lock.requeue_orphaned_dispatch(
            mock_db, now=now, stale_before=now - timedelta(seconds=900)
        )

        # Assert
        assert count == 0
        job = await lock.get_job(mock_db, "job-a")
        assert job is not None
        assert job.status == JobStatus.RUNNING

    @pytest.mark.asyncio
    async def test_should_not_touch_an_already_queued_pending_job(self, mock_db):
        """Test that a stale PENDING job already carrying next_dispatch_at is left.

        Given:
            A PENDING job that already has `next_dispatch_at` set (it is
            queued and leasable by the scheduler), even with a stale
            `updated_at`.
        When:
            requeue_orphaned_dispatch runs.
        Then:
            It is not touched — the scheduler already handles it; the sweep
            targets only the non-leasable orphan states.
        """
        # Arrange
        now = datetime.now(timezone.utc)
        original = now - timedelta(seconds=50)
        _insert_job(
            mock_db,
            workflow_key="a",
            status=JobStatus.PENDING,
            next_dispatch_at=original,
            updated_at=now - timedelta(seconds=2000),
        )

        # Act
        count = await lock.requeue_orphaned_dispatch(
            mock_db, now=now, stale_before=now - timedelta(seconds=900)
        )

        # Assert
        assert count == 0
        job = await lock.get_job(mock_db, "job-a")
        assert job is not None
        assert job.next_dispatch_at == original

    @pytest.mark.asyncio
    async def test_should_not_touch_terminal_jobs(self, mock_db):
        """Test that terminal (completed/failed) jobs are never re-queued.

        Given:
            Stale COMPLETED and FAILED jobs.
        When:
            requeue_orphaned_dispatch runs.
        Then:
            Neither is touched — only active rows (the mutex holders) are
            candidates for recovery.
        """
        # Arrange
        now = datetime.now(timezone.utc)
        stale = now - timedelta(seconds=2000)
        _insert_job(
            mock_db, workflow_key="done", status=JobStatus.COMPLETED, updated_at=stale
        )
        _insert_job(
            mock_db, workflow_key="fail", status=JobStatus.FAILED, updated_at=stale
        )

        # Act
        count = await lock.requeue_orphaned_dispatch(
            mock_db, now=now, stale_before=now - timedelta(seconds=900)
        )

        # Assert
        assert count == 0
        assert (await lock.get_job(mock_db, "job-done")).status == JobStatus.COMPLETED
        assert (await lock.get_job(mock_db, "job-fail")).status == JobStatus.FAILED
