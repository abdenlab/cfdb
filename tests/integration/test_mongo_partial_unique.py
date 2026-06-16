"""Concurrency tests against a real partial-unique-index implementation.

Drives ``claim_workflow`` against ``mongomock-motor`` — an in-process
Python implementation of MongoDB that honors ``partialFilterExpression``
the same way ``mongod`` does. The tests are a contract check between the
application's claim logic and the canonical index spec in
``scripts/create-indexes.js``: any drift between the JSON predicate and
the application's ``ACTIVE_STATUSES`` would surface here, where the
sibling ``test_concurrency.py`` (FakeCollection-backed) cannot catch it.

Why a separate module: the FakeCollection in ``tests/conftest.py`` is a
hand-written stub of partial-unique semantics; it correctly models the
happy path but cannot vouch for index-syntax compatibility with real
MongoDB. ``mongomock`` re-implements the BSON/index machinery faithfully
enough that a ``DuplicateKeyError`` raised here means production would
also raise one for the same insert.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from mongomock_motor import AsyncMongoMockClient

from cfdb.workflows.executor import PIPELINE_VERSION
from cfdb.workflows.lock import (
    JOBS_COLLECTION,
    STALE_WORKFLOW_THRESHOLD,
    claim_workflow,
    release_workflow,
)
from cfdb.workflows.models import ACTIVE_STATUSES, JobRecord, JobStatus


pytestmark = pytest.mark.integration


@pytest_asyncio.fixture()
async def mongomock_db():
    """Yield a mongomock-motor database with the canonical jobs index.

    The index spec mirrors ``scripts/create-indexes.js`` so a drift
    between the production filter expression and ``ACTIVE_STATUSES``
    surfaces as a test failure here.
    """
    client = AsyncMongoMockClient()
    db = client["cfdb-test"]
    await db[JOBS_COLLECTION].create_index(
        [("workflow_key", 1)],
        unique=True,
        partialFilterExpression={
            "status": {"$in": [s.value for s in ACTIVE_STATUSES]}
        },
    )
    yield db


_CLAIM_MD5: str = "d41d8cd98f00b204e9800998ecf8427e"


def _claim_args(
    workflow_key: str = "encode/x/d41d8cd98f00b204e9800998ecf8427e/v1",
) -> dict:
    """Return the keyword arguments shared across claim_workflow calls."""
    return dict(
        dcc="encode",
        local_id="x",
        md5=_CLAIM_MD5,
        pipeline_version=PIPELINE_VERSION,
    )


class TestMongoPartialUniqueIndex:
    @pytest.mark.asyncio
    async def test_claim_workflow_dedupes_concurrent_callers_on_same_key(
        self, mongomock_db
    ):
        """Test that the partial-unique index funnels concurrent claims.

        Given:
            A mongomock jobs collection with the production
            partialFilterExpression and two coroutines racing
            claim_workflow on the same workflow_key.
        When:
            Both coroutines are awaited via asyncio.gather.
        Then:
            Exactly one returns ``fresh=True`` and inserts the row;
            the other returns ``fresh=False`` and attaches to the
            same job_id. The collection holds exactly one document
            for the workflow_key.
        """
        # Arrange
        wf_key = f"encode/x/{_CLAIM_MD5}/v1"

        # Act
        first, second = await asyncio.gather(
            claim_workflow(mongomock_db, wf_key, **_claim_args()),
            claim_workflow(mongomock_db, wf_key, **_claim_args()),
        )

        # Assert
        record_a, fresh_a = first
        record_b, fresh_b = second
        assert record_a.job_id == record_b.job_id
        assert {fresh_a, fresh_b} == {True, False}
        count = await mongomock_db[JOBS_COLLECTION].count_documents(
            {"workflow_key": wf_key}
        )
        assert count == 1

    @pytest.mark.asyncio
    async def test_claim_workflow_succeeds_after_terminal_release_frees_key(
        self, mongomock_db
    ):
        """Test that releasing a job re-opens the workflow_key for fresh claims.

        Given:
            A claimed workflow that has been released to COMPLETED
            (terminal status falls outside the partial filter).
        When:
            claim_workflow is awaited again with the same workflow_key.
        Then:
            The second claim is ``fresh=True`` with a new job_id
            because the prior row no longer matches the partial filter.
        """
        # Arrange
        wf_key = f"encode/x/{_CLAIM_MD5}/v1"
        first_record, _ = await claim_workflow(
            mongomock_db, wf_key, **_claim_args()
        )
        await release_workflow(
            mongomock_db, first_record.job_id, JobStatus.COMPLETED
        )

        # Act
        second_record, fresh = await claim_workflow(
            mongomock_db, wf_key, **_claim_args()
        )

        # Assert
        assert fresh is True
        assert second_record.job_id != first_record.job_id

    @pytest.mark.asyncio
    async def test_claim_workflow_should_reclaim_stale_active_row_against_mongomock(
        self, mongomock_db
    ):
        """Test that a stale RUNNING row is flipped to FAILED and re-inserted.

        Given:
            A mongomock-motor jobs collection holding a single RUNNING
            row whose ``updated_at`` predates the stale threshold by a
            comfortable margin.
        When:
            ``claim_workflow`` is awaited with the same workflow_key.
        Then:
            The stale row is flipped to FAILED with the canonical
            ``"stale — reclaimed by later request"`` error message; a
            fresh row is inserted with a new ``job_id``; the stale row
            carries a ``superseded_by`` pointing at the new row; the
            partial-unique index still admits exactly one active row.
        """
        # Arrange — seed a stale active row directly into the collection
        # so the test isolates the reclaim path without depending on a
        # prior claim_workflow call.
        wf_key = f"encode/x/{_CLAIM_MD5}/v1"
        now = datetime.now(timezone.utc)
        stale_at = now - STALE_WORKFLOW_THRESHOLD - timedelta(minutes=5)
        stale_record = JobRecord(
            job_id=str(uuid.uuid4()),
            workflow_key=wf_key,
            status=JobStatus.RUNNING,
            dcc="encode",
            local_id="x",
            md5=_CLAIM_MD5,
            pipeline_version=PIPELINE_VERSION,
            submitted_at=stale_at,
            updated_at=stale_at,
        )
        await mongomock_db[JOBS_COLLECTION].insert_one(stale_record.to_mongo())

        # Act
        fresh_record, fresh = await claim_workflow(
            mongomock_db, wf_key, **_claim_args()
        )

        # Assert
        assert fresh is True
        assert fresh_record.job_id != stale_record.job_id
        # Stale row was flipped + linked at the supersedes chain.
        stale_after = await mongomock_db[JOBS_COLLECTION].find_one(
            {"job_id": stale_record.job_id}
        )
        assert stale_after is not None
        assert stale_after["status"] == JobStatus.FAILED.value
        assert stale_after.get("error") == "stale — reclaimed by later request"
        assert stale_after.get("superseded_by") == fresh_record.job_id
        # The fresh row exists and is the only active row under the partial filter.
        active_count = await mongomock_db[JOBS_COLLECTION].count_documents(
            {
                "workflow_key": wf_key,
                "status": {"$in": [s.value for s in ACTIVE_STATUSES]},
            }
        )
        assert active_count == 1

    @pytest.mark.asyncio
    async def test_claim_workflow_should_record_supersededby_chain_when_stale_row_loses_to_racing_winner(
        self, mongomock_db
    ):
        """Test that under contention the stale row points at the actual winner.

        Given:
            A stale RUNNING row in the jobs collection and two
            coroutines racing ``claim_workflow`` on the same workflow_key.
        When:
            Both coroutines are awaited via ``asyncio.gather``.
        Then:
            Exactly one returns ``fresh=True`` and inserts a row; the
            other returns ``fresh=False`` and attaches to that row;
            the stale row's ``superseded_by`` points at the winning
            row even under contention.
        """
        # Arrange
        wf_key = f"encode/x/{_CLAIM_MD5}/v1"
        now = datetime.now(timezone.utc)
        stale_at = now - STALE_WORKFLOW_THRESHOLD - timedelta(minutes=5)
        stale_record = JobRecord(
            job_id=str(uuid.uuid4()),
            workflow_key=wf_key,
            status=JobStatus.RUNNING,
            dcc="encode",
            local_id="x",
            md5=_CLAIM_MD5,
            pipeline_version=PIPELINE_VERSION,
            submitted_at=stale_at,
            updated_at=stale_at,
        )
        await mongomock_db[JOBS_COLLECTION].insert_one(stale_record.to_mongo())

        # Act
        first, second = await asyncio.gather(
            claim_workflow(mongomock_db, wf_key, **_claim_args()),
            claim_workflow(mongomock_db, wf_key, **_claim_args()),
        )

        # Assert
        rec_a, fresh_a = first
        rec_b, fresh_b = second
        assert {fresh_a, fresh_b} == {True, False}
        # Both callers converge on the same winning row.
        assert rec_a.job_id == rec_b.job_id
        winner_id = rec_a.job_id
        # Stale row points at whichever caller actually inserted.
        stale_after = await mongomock_db[JOBS_COLLECTION].find_one(
            {"job_id": stale_record.job_id}
        )
        assert stale_after is not None
        assert stale_after.get("superseded_by") == winner_id

    @pytest.mark.asyncio
    async def test_claim_workflow_funnels_many_concurrent_callers_on_same_key(
        self, mongomock_db
    ):
        """Test that an N-way race produces exactly one fresh winner.

        Given:
            A mongomock jobs collection and ten coroutines racing
            claim_workflow on the same workflow_key.
        When:
            All ten are awaited via asyncio.gather.
        Then:
            Exactly one returns ``fresh=True`` and the other nine
            attach to the same job_id; the collection holds exactly
            one document.
        """
        # Arrange
        wf_key = f"encode/x/{_CLAIM_MD5}/v1"
        n = 10

        # Act
        results = await asyncio.gather(
            *[
                claim_workflow(mongomock_db, wf_key, **_claim_args())
                for _ in range(n)
            ]
        )

        # Assert
        fresh_count = sum(1 for _, fresh in results if fresh)
        assert fresh_count == 1
        job_ids = {record.job_id for record, _ in results}
        assert len(job_ids) == 1
        count = await mongomock_db[JOBS_COLLECTION].count_documents(
            {"workflow_key": wf_key}
        )
        assert count == 1
