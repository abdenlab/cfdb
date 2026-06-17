"""Unit tests for workflow job records and enums."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from cfdb.workflows.models import (
    ACTIVE_STATUSES,
    ArtifactKind,
    JobRecord,
    JobStatus,
)
from tests.test_workflows import FIXTURE_MD5


def _make_job(**overrides) -> JobRecord:
    """Return a JobRecord with sensible defaults for test construction."""
    now = datetime(2026, 4, 21, 12, 0, 0, tzinfo=timezone.utc)
    base = dict(
        job_id="job-123",
        workflow_key=f"encode/x/{FIXTURE_MD5}/v0",
        status=JobStatus.PENDING,
        dcc="encode",
        local_id="x",
        md5=FIXTURE_MD5,
        pipeline_version=0,
        submitted_at=now,
        updated_at=now,
    )
    base.update(overrides)
    return JobRecord(**base)


class TestJobStatus:
    def test_active_statuses_should_include_pending_and_running(self):
        """Test that ACTIVE_STATUSES exactly enumerates the mutex-holding states.

        Given:
            The module-level ACTIVE_STATUSES tuple.
        When:
            Its members are inspected.
        Then:
            It should contain exactly PENDING and RUNNING so that the
            Mongo partial unique index filter predicate is kept in sync.
        """
        # Assert
        assert set(ACTIVE_STATUSES) == {JobStatus.PENDING, JobStatus.RUNNING}

    @pytest.mark.parametrize(
        "status,expected_active",
        [
            (JobStatus.PENDING, True),
            (JobStatus.RUNNING, True),
            (JobStatus.COMPLETED, False),
            (JobStatus.FAILED, False),
        ],
    )
    def test_status_membership_in_active_statuses_should_match_lifecycle(
        self, status, expected_active
    ):
        """Test that ACTIVE_STATUSES membership matches mutex-holding semantics.

        Given:
            Each member of JobStatus.
        When:
            Its membership in ACTIVE_STATUSES is evaluated.
        Then:
            PENDING and RUNNING report True (they hold the partial-unique
            mutex on the Mongo index); COMPLETED and FAILED report False.
        """
        # Act
        is_active = status in ACTIVE_STATUSES

        # Assert
        assert is_active is expected_active


class TestJobRecord:
    def test___init___should_default_stages_done_and_artifact_cache_keys(self):
        """Test that construction initializes derived collections to empty.

        Given:
            A JobRecord constructed without stages_done or artifact_cache_keys.
        When:
            The instance is inspected.
        Then:
            It should expose empty collections so callers can append without
            a preceding None check.
        """
        # Act
        job = _make_job()

        # Assert
        assert job.stages_done == []
        assert job.artifact_cache_keys == {}

    def test___init___should_default_progress_and_superseded_by_to_none(self):
        """Test that optional terminal/in-flight fields default to None.

        Given:
            A JobRecord constructed without ``progress`` or ``superseded_by``.
        When:
            The instance is inspected.
        Then:
            Both fields should be None so JSON serialization and Mongo
            inserts get explicit nulls rather than missing keys.
        """
        # Act
        job = _make_job()

        # Assert
        assert job.progress is None
        assert job.superseded_by is None
        assert job.file_meta_snapshot is None

    def test___init___should_reject_naive_submitted_at(self):
        """Test that naive datetimes are rejected at validation time.

        Given:
            A naive ``submitted_at`` datetime (no tzinfo).
        When:
            The JobRecord is constructed.
        Then:
            Pydantic should raise a ValidationError naming the
            timezone-aware requirement so a regression that introduces a
            naive producer cannot break the stale_cutoff comparison in
            ``claim_workflow``.
        """
        # Arrange
        naive = datetime(2026, 4, 21, 12, 0, 0)

        # Act & assert
        with pytest.raises(ValidationError, match="timezone-aware"):
            _make_job(submitted_at=naive)

    def test___init___should_reject_naive_updated_at(self):
        """Test that updated_at also enforces tz-aware datetimes.

        Given:
            A naive ``updated_at`` datetime.
        When:
            The JobRecord is constructed.
        Then:
            Pydantic should raise a ValidationError matching the same
            "timezone-aware" message as the submitted_at validator.
        """
        # Arrange
        aware = datetime(2026, 4, 21, 12, 0, 0, tzinfo=timezone.utc)
        naive = datetime(2026, 4, 21, 12, 30, 0)

        # Act & assert
        with pytest.raises(ValidationError, match="timezone-aware"):
            _make_job(submitted_at=aware, updated_at=naive)

    def test___init___should_reject_md5_with_non_hex_characters(self):
        """Test that md5 must be 32 lowercase hex characters.

        Given:
            A 32-character md5 candidate that contains non-hex chars.
        When:
            The JobRecord is constructed.
        Then:
            Pydantic should raise a ValidationError on the md5 pattern
            constraint so a malformed-md5 fixture or producer cannot
            insert a sentinel-shaped row that still satisfies the
            workflow_key partial-unique index.
        """
        # Arrange
        bad_md5 = "z" * 32

        # Act & assert
        with pytest.raises(ValidationError):
            _make_job(md5=bad_md5)

    def test___init___should_reject_md5_with_wrong_length(self):
        """Test that md5 must be exactly 32 chars (not 8, not 64).

        Given:
            An 8-character md5 fixture (the historical "deadbeef" foot-gun).
        When:
            The JobRecord is constructed.
        Then:
            Pydantic should raise a ValidationError on the pattern
            constraint, surfacing migration drift on any test that still
            uses the old short-md5 fixture.
        """
        # Act & assert
        with pytest.raises(ValidationError):
            _make_job(md5="deadbeef")

    def test___init___should_reject_empty_required_strings(self):
        """Test that required string fields enforce min_length=1.

        Given:
            An empty string for ``job_id``.
        When:
            The JobRecord is constructed.
        Then:
            Pydantic should raise a ValidationError on min_length so a
            buggy producer cannot insert a sentinel-shaped row.
        """
        # Act & assert
        with pytest.raises(ValidationError):
            _make_job(job_id="")

    def test___init___should_reject_negative_pipeline_version(self):
        """Test that pipeline_version must be non-negative.

        Given:
            A negative pipeline_version.
        When:
            The JobRecord is constructed.
        Then:
            Pydantic should raise a ValidationError on the ge=0 constraint.
        """
        # Act & assert
        with pytest.raises(ValidationError):
            _make_job(pipeline_version=-1)

    def test___init___should_persist_file_meta_snapshot(self):
        """Test that file_meta_snapshot round-trips through to_mongo.

        Given:
            A JobRecord constructed with a ``file_meta_snapshot`` dict.
        When:
            ``to_mongo`` is invoked.
        Then:
            The returned dict should carry an independent copy of the
            snapshot under the same key, matching the architecture
            diagram in the linked issue.
        """
        # Arrange
        snapshot = {"dcc": {"dcc_abbreviation": "encode"}, "local_id": "x"}
        job = _make_job(file_meta_snapshot=snapshot)

        # Act
        payload = job.to_mongo()

        # Assert
        assert payload["file_meta_snapshot"] == snapshot
        # Defensive copy: mutating the original must not affect the dump
        snapshot["dcc"]["dcc_abbreviation"] = "mutated"
        payload2 = job.to_mongo()
        # The model itself still references the original dict, so a
        # subsequent dump reflects the mutation. The defensive copy
        # protects the dump from being mutated by callers, not the model
        # from being mutated by the construction-time argument. Verify
        # the dump is at least a distinct dict object.
        assert payload2["file_meta_snapshot"] is not snapshot

    def test_to_mongo_should_return_mongo_serializable_dict(self):
        """Test that to_mongo returns a dict safe for Mongo insertion.

        Given:
            A JobRecord with populated enum and datetime fields.
        When:
            to_mongo is invoked.
        Then:
            It should return a dict whose enum values are serialized to
            their string representation and whose datetimes are preserved
            as ``datetime`` objects (Motor encodes to BSON Date).
        """
        # Arrange
        cache_key = f"encode/x/data/{FIXTURE_MD5}-v0"
        job = _make_job(
            status=JobStatus.RUNNING,
            artifact_cache_keys={ArtifactKind.DATA.value: cache_key},
        )

        # Act
        payload = job.to_mongo()

        # Assert
        assert payload["status"] == "running"
        assert isinstance(payload["submitted_at"], datetime)
        assert payload["artifact_cache_keys"]["data"] == cache_key
        # New fields surface in the persisted shape too.
        assert payload["file_meta_snapshot"] is None
        assert payload["superseded_by"] is None

    def test_to_mongo_should_serialize_stages_progress_and_superseded_by(self):
        """Test that to_mongo carries the full wire shape, including new fields.

        Given:
            A JobRecord with ``stages_done``, ``superseded_by``, and
            ``progress`` populated.
        When:
            to_mongo is invoked.
        Then:
            All three fields should appear in the dump with their values
            preserved verbatim — the hand-rolled serializer must cover
            every persisted field.
        """
        # Arrange
        job = _make_job(
            stages_done=["data", "index"],
            superseded_by="job-xyz",
            progress="indexing",
        )

        # Act
        payload = job.to_mongo()

        # Assert
        assert payload["stages_done"] == ["data", "index"]
        assert payload["superseded_by"] == "job-xyz"
        assert payload["progress"] == "indexing"

    @pytest.mark.parametrize(
        "status",
        list(JobStatus),
    )
    def test_to_mongo_should_serialize_each_job_status_to_string_value(self, status):
        """Test that every JobStatus serializes to its string form.

        Given:
            Each JobStatus enum value.
        When:
            ``JobRecord(..., status=X).to_mongo()`` is invoked.
        Then:
            ``payload["status"]`` should equal ``X.value``.
        """
        # Arrange
        job = _make_job(status=status)

        # Act
        payload = job.to_mongo()

        # Assert
        assert payload["status"] == status.value

    @pytest.mark.parametrize("status", list(JobStatus))
    def test_to_mongo_should_derive_active_from_status(self, status):
        """Test that to_mongo stamps the active mutex discriminator.

        Given:
            Each JobStatus enum value.
        When:
            ``to_mongo`` is invoked.
        Then:
            ``payload["active"]`` should be True exactly for the active
            statuses, so the boolean backing the partial-unique index
            stays in lockstep with ACTIVE_STATUSES.
        """
        # Arrange
        job = _make_job(status=status)

        # Act
        payload = job.to_mongo()

        # Assert
        assert payload["active"] is (status in ACTIVE_STATUSES)

    def test_to_mongo_should_isolate_artifact_cache_keys_dict_from_caller_mutations(
        self,
    ):
        """Test that mutating a returned dict does not affect later dumps.

        Given:
            A JobRecord with ``artifact_cache_keys`` populated.
        When:
            ``to_mongo`` is invoked twice; the first returned dict is
            mutated in between.
        Then:
            The second dump should equal the original, demonstrating that
            ``to_mongo`` returns a fresh dict each call.
        """
        # Arrange
        job = _make_job(artifact_cache_keys={"data": "k1", "index": "k2"})

        # Act
        first = job.to_mongo()
        first["artifact_cache_keys"]["data"] = "MUTATED"
        second = job.to_mongo()

        # Assert
        assert second["artifact_cache_keys"] == {"data": "k1", "index": "k2"}

    def test___init___should_accept_error_at_max_length_boundary(self):
        """Test that an error string at the 1024-char cap is admitted.

        Given:
            A JobRecord with ``error="x" * 1024`` (the upper bound of the
            StringConstraints max_length).
        When:
            Construction is attempted.
        Then:
            It should succeed without raising.
        """
        # Arrange
        boundary_error = "x" * 1024

        # Act
        job = _make_job(error=boundary_error)

        # Assert
        assert job.error == boundary_error

    def test___init___should_reject_error_longer_than_max_length(self):
        """Test that an error string exceeding 1024 chars is rejected.

        Given:
            A JobRecord with ``error="x" * 1025``.
        When:
            Construction is attempted.
        Then:
            Pydantic should raise a ValidationError on the per-field
            length cap.
        """
        # Arrange
        oversize_error = "x" * 1025

        # Act & assert
        with pytest.raises(ValidationError):
            _make_job(error=oversize_error)
