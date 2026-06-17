"""Tests for the lifespan's operational-index self-heal."""

from __future__ import annotations

import logging

import pytest

from cfdb import api
from cfdb.api import main


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lifespan_should_ensure_operational_indexes_without_workflows(mocker):
    """Test startup creates the operational indexes when workflows are off.

    Given:
        A mongomock-motor database and the workflow subsystem disabled
        (``WorkflowProfile.from_env`` returns None).
    When:
        The app lifespan starts up.
    Then:
        It should ensure the operational indexes — the workflow_key mutex
        and locks.active — so a fresh database boots clean rather than
        crash-looping, even with no workflow subsystem.
    """
    # Arrange
    from mongomock_motor import AsyncMongoMockClient

    client = AsyncMongoMockClient()
    mocker.patch.object(main, "create_mongodb_client", return_value=client)
    mocker.patch.object(main.WorkflowProfile, "from_env", return_value=None)

    # Act
    async with main.lifespan(main.app):
        db = client[api.DATABASE_NAME]
        jobs_info = await db.jobs.index_information()
        locks_info = await db.locks.index_information()

    # Assert
    assert "workflow_key_active_unique" in jobs_info
    assert "active_1" in locks_info


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lifespan_should_not_raise_when_mutex_index_present(mocker, caplog):
    """Test startup does not warn once the mutex index is ensured.

    Given:
        A mongomock-motor database and the workflow subsystem disabled.
    When:
        The lifespan starts up (which ensures the indexes, then runs the
        belt-and-suspenders desync check).
    Then:
        It should complete without raising and without logging the
        mutex-out-of-sync warning, since ensure created the index first.
    """
    # Arrange
    from mongomock_motor import AsyncMongoMockClient

    client = AsyncMongoMockClient()
    mocker.patch.object(main, "create_mongodb_client", return_value=client)
    mocker.patch.object(main.WorkflowProfile, "from_env", return_value=None)

    # Act
    with caplog.at_level(logging.WARNING):
        async with main.lifespan(main.app):
            pass

    # Assert
    assert "partial-unique index missing or out of sync" not in caplog.text
