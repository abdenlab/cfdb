"""MongoDB-based distributed locking for sync operations."""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from cfdb import api

logger = logging.getLogger(__name__)

LOCKS_COLLECTION = "locks"
SYNC_LOCK_ID = "sync"
CUTOVER_LOCK_ID = "cutover"

# How long to wait for cutover to complete before timing out
CUTOVER_WAIT_TIMEOUT = 60.0  # seconds
CUTOVER_POLL_INTERVAL = 0.1  # seconds

# Stale lock threshold - if a lock has shown no sign of life for longer than
# this, consider it abandoned. "Sign of life" is its ``heartbeat_at``, which
# the sync refreshes at each DCC boundary (see ``heartbeat_sync_lock``); a
# lock written before heartbeats existed falls back to its ``started_at``.
STALE_LOCK_THRESHOLD = timedelta(hours=1)


async def try_acquire_sync_lock(task_id: str, dcc_names: list[str]) -> bool:
    """
    Attempt to acquire the sync lock.

    Uses atomic upsert to ensure only one sync can run at a time.

    Args:
        task_id: Unique identifier for this sync task.
        dcc_names: List of DCC names being synced.

    Returns:
        True if lock was acquired, False if another sync is running.
    """
    if api.db is None:
        raise RuntimeError("Database not initialized")

    now = datetime.utcnow()
    stale_threshold = now - STALE_LOCK_THRESHOLD

    # Try to acquire lock atomically
    # Only succeeds if no lock exists OR existing lock is stale
    result = await api.db[LOCKS_COLLECTION].find_one_and_update(
        {
            "_id": SYNC_LOCK_ID,
            "$or": [
                {"active": False},
                {"active": {"$exists": False}},
                {"heartbeat_at": {"$lt": stale_threshold}},
                {
                    "heartbeat_at": {"$exists": False},
                    "started_at": {"$lt": stale_threshold},
                },
            ],
        },
        {
            "$set": {
                "active": True,
                "task_id": task_id,
                "dcc_names": dcc_names,
                "started_at": now,
                "heartbeat_at": now,
                "updated_at": now,
            },
            "$unset": {"completed_at": ""},
        },
        upsert=True,
        return_document=True,
    )

    # Check if we got the lock (our task_id is set)
    if result and result.get("task_id") == task_id:
        logger.info(f"Acquired sync lock for task {task_id}")
        return True

    # Lock exists and belongs to another task - check if it's ours via separate query
    existing = await api.db[LOCKS_COLLECTION].find_one({"_id": SYNC_LOCK_ID})
    if existing and existing.get("task_id") == task_id:
        return True

    logger.info(
        f"Sync lock held by task {existing.get('task_id') if existing else 'unknown'}"
    )
    return False


async def release_sync_lock(task_id: str) -> None:
    """
    Release the sync lock.

    Args:
        task_id: The task ID that holds the lock.
    """
    if api.db is None:
        raise RuntimeError("Database not initialized")

    result = await api.db[LOCKS_COLLECTION].update_one(
        {"_id": SYNC_LOCK_ID, "task_id": task_id},
        {"$set": {"active": False, "completed_at": datetime.utcnow()}},
    )

    if result.modified_count > 0:
        logger.info(f"Released sync lock for task {task_id}")
    else:
        logger.warning(f"Could not release sync lock for task {task_id} - not held")


async def is_sync_running() -> bool:
    """Check if a sync is currently running."""
    if api.db is None:
        return False

    lock = await api.db[LOCKS_COLLECTION].find_one({"_id": SYNC_LOCK_ID})
    if not lock:
        return False

    if not lock.get("active"):
        return False

    # Check if lock is stale
    alive_at = sync_lock_alive_at(lock)
    if alive_at and datetime.utcnow() - alive_at > STALE_LOCK_THRESHOLD:
        logger.warning("Found stale sync lock, considering it inactive")
        return False

    return True


def sync_lock_alive_at(lock: dict) -> Optional[datetime]:
    """When a sync lock last showed it was alive: its heartbeat, else its start.

    Staleness is judged from this, so a long multi-DCC run that keeps
    heartbeating is never mistaken for an abandoned one, while a crashed run
    still goes stale ``STALE_LOCK_THRESHOLD`` after its last heartbeat.
    """
    return lock.get("heartbeat_at") or lock.get("started_at")


async def heartbeat_sync_lock(task_id: str) -> None:
    """Record that the running sync is alive, restarting its stale clock.

    Called at each DCC boundary. Without it a lock's staleness counted from
    the start of the whole run, so whichever DCC ran last inherited what the
    earlier ones left of the hour -- or none of it. Scoped to ``task_id``, so
    a run whose lock went stale and was taken over cannot keep its
    successor's lock alive.

    Args:
        task_id: The task ID that holds the lock.
    """
    if api.db is None:
        raise RuntimeError("Database not initialized")

    await api.db[LOCKS_COLLECTION].update_one(
        {"_id": SYNC_LOCK_ID, "task_id": task_id, "active": True},
        {"$set": {"heartbeat_at": datetime.utcnow()}},
    )


async def get_current_sync_task() -> Optional[dict]:
    """Get information about the currently running sync task."""
    if api.db is None:
        return None

    lock = await api.db[LOCKS_COLLECTION].find_one(
        {"_id": SYNC_LOCK_ID, "active": True}
    )
    return lock


async def get_sync_task(task_id: str) -> Optional[dict]:
    """Get information about a sync task by ID."""
    if api.db is None:
        return None

    lock = await api.db[LOCKS_COLLECTION].find_one(
        {"_id": SYNC_LOCK_ID, "task_id": task_id}
    )
    return lock


async def acquire_cutover_lock(dcc: str) -> None:
    """
    Acquire the cutover lock, blocking API requests.

    Args:
        dcc: The DCC being updated during cutover.
    """
    if api.db is None:
        raise RuntimeError("Database not initialized")

    await api.db[LOCKS_COLLECTION].update_one(
        {"_id": CUTOVER_LOCK_ID},
        {
            "$set": {
                "active": True,
                "dcc": dcc,
                "started_at": datetime.utcnow(),
            }
        },
        upsert=True,
    )
    logger.info(f"Acquired cutover lock for {dcc}")


async def release_cutover_lock() -> None:
    """Release the cutover lock, allowing API requests to proceed."""
    if api.db is None:
        raise RuntimeError("Database not initialized")

    await api.db[LOCKS_COLLECTION].update_one(
        {"_id": CUTOVER_LOCK_ID},
        {"$set": {"active": False, "completed_at": datetime.utcnow()}},
    )
    logger.info("Released cutover lock")


async def wait_for_cutover() -> None:
    """
    Wait for any active cutover to complete.

    Called by API endpoints to pause during database updates.
    Returns immediately if no cutover is in progress.

    Raises:
        TimeoutError: If cutover takes longer than CUTOVER_WAIT_TIMEOUT.
    """
    if api.db is None:
        return

    start_time = asyncio.get_event_loop().time()

    while True:
        lock = await api.db[LOCKS_COLLECTION].find_one({"_id": CUTOVER_LOCK_ID})

        if not lock or not lock.get("active"):
            return

        elapsed = asyncio.get_event_loop().time() - start_time
        if elapsed > CUTOVER_WAIT_TIMEOUT:
            raise TimeoutError(
                f"Cutover lock held for more than {CUTOVER_WAIT_TIMEOUT}s"
            )

        await asyncio.sleep(CUTOVER_POLL_INTERVAL)


class CutoverLock:
    """Async context manager for cutover lock."""

    def __init__(self, dcc: str):
        self.dcc = dcc

    async def __aenter__(self):
        await acquire_cutover_lock(self.dcc)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await release_cutover_lock()
        return False
