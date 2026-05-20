"""Tests for the /jobs router."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

from cfdb import api
from cfdb.api.routers.jobs import get_job_status
from cfdb.services import locks
from cfdb.workflows.lock import JOBS_COLLECTION
from cfdb.workflows.models import ArtifactKind, JobRecord, JobStatus
from tests.test_workflows import FIXTURE_MD5


def _seed_job(mock_db, **overrides) -> JobRecord:
    """Insert a synthetic JobRecord into the FakeDB jobs collection."""
    now = datetime.now(timezone.utc)
    base = dict(
        job_id="job-abc",
        workflow_key=f"encode/x/{FIXTURE_MD5}/v1",
        status=JobStatus.RUNNING,
        dcc="encode",
        local_id="x",
        md5=FIXTURE_MD5,
        pipeline_version=1,
        submitted_at=now,
        updated_at=now,
        stages_done=[ArtifactKind.DATA.value],
        artifact_cache_keys={
            ArtifactKind.DATA.value: f"encode/x/data/{FIXTURE_MD5}-v0"
        },
        progress="indexing",
    )
    base.update(overrides)
    record = JobRecord(**base)
    mock_db[JOBS_COLLECTION].docs.append(record.to_mongo())
    return record


class TestGetJobStatus:
    @pytest.mark.asyncio
    async def test_get_job_status_should_return_record_when_job_present(
        self, mock_db, mocker
    ):
        """Test that /jobs/{id} returns the persisted job state.

        Given:
            A JobRecord in the jobs collection.
        When:
            get_job_status is awaited with its job_id.
        Then:
            It should return a dict containing the status, stages_done,
            artifact_cache_keys, and progress field.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        record = _seed_job(mock_db)

        # Act
        result = await get_job_status(record.job_id)

        # Assert
        assert result.job_id == record.job_id
        assert result.status == JobStatus.RUNNING.value
        assert result.stages_done == [ArtifactKind.DATA.value]
        assert result.artifacts == {
            ArtifactKind.DATA.value: f"encode/x/data/{FIXTURE_MD5}-v0"
        }
        assert result.superseded_by is None
        assert result.progress == "indexing"
        assert result.error is None

    @pytest.mark.asyncio
    async def test_get_job_status_should_raise_404_when_job_absent(
        self, mock_db, mocker
    ):
        """Test that /jobs/{id} 404s on unknown job ids.

        Given:
            An empty jobs collection.
        When:
            get_job_status is awaited with a bogus id.
        Then:
            It should raise HTTPException(404).
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await get_job_status("no-such-job")
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_get_job_status_should_include_error_when_job_failed(
        self, mock_db, mocker
    ):
        """Test that failed jobs surface their error string.

        Given:
            A JobRecord with status FAILED and an error message.
        When:
            get_job_status is awaited.
        Then:
            The response should carry the error text verbatim.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        record = _seed_job(
            mock_db, status=JobStatus.FAILED, error="samtools exploded"
        )

        # Act
        result = await get_job_status(record.job_id)

        # Assert
        assert result.status == JobStatus.FAILED.value
        assert result.error == "samtools exploded"

    @pytest.mark.asyncio
    async def test_get_job_status_should_expose_superseded_by_pointer(
        self, mock_db, mocker
    ):
        """Test (JB-001) that /jobs/{id} exposes the supersedes chain.

        Given:
            A JobRecord with ``superseded_by="job-xyz"``.
        When:
            ``get_job_status`` is awaited.
        Then:
            The response's ``superseded_by`` field should equal
            ``"job-xyz"`` so clients can follow the chain.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        record = _seed_job(mock_db, superseded_by="job-xyz")

        # Act
        result = await get_job_status(record.job_id)

        # Assert
        assert result.superseded_by == "job-xyz"

    @pytest.mark.parametrize("status", list(JobStatus))
    @pytest.mark.asyncio
    async def test_get_job_status_should_serialize_each_job_status_round_trip(
        self, mock_db, mocker, status
    ):
        """Test (JB-002) that every JobStatus round-trips through /jobs.

        Given:
            A JobRecord at each lifecycle status.
        When:
            ``get_job_status`` is awaited.
        Then:
            The response's ``status`` should equal ``status.value``.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        # Use a unique job_id per parametric case to avoid collisions
        # within a single mock_db.
        record = _seed_job(
            mock_db,
            status=status,
            job_id=f"job-{status.value}",
            workflow_key=f"encode/x/{FIXTURE_MD5}/v{status.value}",
        )

        # Act
        result = await get_job_status(record.job_id)

        # Assert
        assert result.status == status.value

    @pytest.mark.asyncio
    async def test_get_job_status_should_raise_500_when_db_unwired(
        self, mocker
    ):
        """Test (JB-003) that an unwired ``api.db`` produces 500.

        Given:
            ``api.db`` is None.
        When:
            ``get_job_status`` is awaited.
        Then:
            It should raise ``HTTPException(500)`` matching "Database
            not available".
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mocker.patch.object(api, "db", None)

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await get_job_status("any")
        assert exc_info.value.status_code == 500
        assert "Database not available" in (exc_info.value.detail or "")

    @pytest.mark.asyncio
    async def test_get_job_status_should_surface_in_flight_progress(
        self, mock_db, mocker
    ):
        """Test (JB-004) that an in-flight progress string surfaces verbatim.

        Given:
            A JobRecord with ``progress="merging chunks"``.
        When:
            ``get_job_status`` is awaited.
        Then:
            The response's ``progress`` should match.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        record = _seed_job(mock_db, progress="merging chunks")

        # Act
        result = await get_job_status(record.job_id)

        # Assert
        assert result.progress == "merging chunks"
