"""Tests for the workflow mutex (claim/release and partial-unique semantics)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from cfdb.workflows import lock
from cfdb.workflows.models import ACTIVE_STATUSES, ArtifactKind, JobStatus
from tests.test_workflows import FIXTURE_MD5

PIPELINE_VERSION = 1


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
