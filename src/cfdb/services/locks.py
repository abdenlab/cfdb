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

# Stale lock threshold - if a lock is older than this, consider it abandoned
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
                {"started_at": {"$lt": stale_threshold}},
            ],
        },
        {
            "$set": {
                "active": True,
                "task_id": task_id,
                "dcc_names": dcc_names,
                "started_at": now,
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
    started_at = lock.get("started_at")
    if started_at and datetime.utcnow() - started_at > STALE_LOCK_THRESHOLD:
        logger.warning("Found stale sync lock, considering it inactive")
        return False

    return True


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
