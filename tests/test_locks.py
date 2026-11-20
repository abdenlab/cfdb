"""Tests for the MongoDB-backed sync and cutover locks."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from cfdb.services import locks


async def _seed_sync_lock(mock_db, **fields):
    """Write the sync lock document with the given fields."""
    await mock_db[locks.LOCKS_COLLECTION].update_one(
        {"_id": locks.SYNC_LOCK_ID}, {"$set": fields}, upsert=True
    )


def test_sync_lock_alive_at_should_prefer_the_heartbeat():
    """Test a heartbeat, when present, is the lock's last sign of life.

    Given:
        A lock with both a start time and a later heartbeat.
    When:
        sync_lock_alive_at reads it.
    Then:
        It should return the heartbeat.
    """
    # Arrange
    started = datetime(2026, 1, 1, 0, 0)
    heartbeat = datetime(2026, 1, 1, 0, 50)

    # Act
    alive_at = locks.sync_lock_alive_at(
        {"started_at": started, "heartbeat_at": heartbeat}
    )

    # Assert
    assert alive_at == heartbeat


def test_sync_lock_alive_at_should_fall_back_to_the_start_without_a_heartbeat():
    """Test a lock written before heartbeats existed still has an age.

    Given:
        A lock with a start time and no heartbeat.
    When:
        sync_lock_alive_at reads it.
    Then:
        It should return the start time.
    """
    # Arrange
    started = datetime(2026, 1, 1, 0, 0)

    # Act
    alive_at = locks.sync_lock_alive_at({"started_at": started})

    # Assert
    assert alive_at == started


@pytest.mark.asyncio
async def test_heartbeat_sync_lock_should_refresh_an_active_lock(mock_db):
    """Test a heartbeat restarts an active lock's stale clock.

    Given:
        An active sync lock last heartbeated an hour ago.
    When:
        heartbeat_sync_lock runs.
    Then:
        The lock's heartbeat should be moments old.
    """
    # Arrange
    await _seed_sync_lock(
        mock_db,
        active=True,
        task_id="t1",
        heartbeat_at=datetime.utcnow() - timedelta(hours=1),
    )

    # Act
    await locks.heartbeat_sync_lock("t1")

    # Assert
    lock = await mock_db[locks.LOCKS_COLLECTION].find_one({"_id": locks.SYNC_LOCK_ID})
    assert datetime.utcnow() - lock["heartbeat_at"] < timedelta(seconds=5)


@pytest.mark.asyncio
async def test_heartbeat_sync_lock_should_leave_a_released_lock_alone(mock_db):
    """Test a heartbeat never revives a released lock.

    Given:
        A released (inactive) sync lock with an old heartbeat.
    When:
        heartbeat_sync_lock runs.
    Then:
        The heartbeat should be unchanged.
    """
    # Arrange
    old = datetime.utcnow() - timedelta(hours=2)
    await _seed_sync_lock(mock_db, active=False, task_id="t1", heartbeat_at=old)

    # Act
    await locks.heartbeat_sync_lock("t1")

    # Assert
    lock = await mock_db[locks.LOCKS_COLLECTION].find_one({"_id": locks.SYNC_LOCK_ID})
    assert lock["heartbeat_at"] == old


@pytest.mark.asyncio
async def test_heartbeat_sync_lock_should_leave_another_tasks_lock_alone(mock_db):
    """Test a run whose lock was taken over cannot keep the new one alive.

    Given:
        An active sync lock held by task "second" with an old heartbeat.
    When:
        Task "first" heartbeats.
    Then:
        The heartbeat should be unchanged.
    """
    # Arrange
    old = datetime.utcnow() - timedelta(minutes=30)
    await _seed_sync_lock(mock_db, active=True, task_id="second", heartbeat_at=old)

    # Act
    await locks.heartbeat_sync_lock("first")

    # Assert
    lock = await mock_db[locks.LOCKS_COLLECTION].find_one({"_id": locks.SYNC_LOCK_ID})
    assert lock["heartbeat_at"] == old


@pytest.mark.asyncio
async def test_try_acquire_sync_lock_should_take_over_when_the_heartbeat_is_stale(
    mock_db,
):
    """Test a lock silent past the threshold is treated as abandoned.

    Given:
        An active sync lock whose last heartbeat is older than the stale
        threshold.
    When:
        A second sync tries to acquire the lock.
    Then:
        It should acquire it, stamping a fresh start and heartbeat.
    """
    # Arrange
    old = datetime.utcnow() - locks.STALE_LOCK_THRESHOLD - timedelta(minutes=1)
    await _seed_sync_lock(
        mock_db, active=True, task_id="first", started_at=old, heartbeat_at=old
    )

    # Act
    acquired = await locks.try_acquire_sync_lock("second", ["hubmap"])

    # Assert
    lock = await mock_db[locks.LOCKS_COLLECTION].find_one({"_id": locks.SYNC_LOCK_ID})
    assert acquired is True
    assert lock["task_id"] == "second"
    assert lock["heartbeat_at"] > old


@pytest.mark.asyncio
async def test_is_sync_running_should_judge_staleness_by_the_heartbeat(mock_db):
    """Test a long, heartbeating run still reports as running.

    Given:
        An active sync lock started two hours ago but heartbeated a minute
        ago.
    When:
        is_sync_running checks it.
    Then:
        It should report the sync as running.
    """
    # Arrange
    now = datetime.utcnow()
    await _seed_sync_lock(
        mock_db,
        active=True,
        started_at=now - timedelta(hours=2),
        heartbeat_at=now - timedelta(minutes=1),
    )

    # Act
    running = await locks.is_sync_running()

    # Assert
    assert running is True
