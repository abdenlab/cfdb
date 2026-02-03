"""Sync service for API-driven DCC metadata synchronization."""

import asyncio
import csv
import logging
import os
import shutil
import subprocess
from copy import copy
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

from cfdb import api
from cfdb.dcc_registry import (
    get_all_dcc_names,
    get_dcc_config,
    get_dcc_type,
    normalize_dcc_name,
)
from cfdb.downloader import cleanup_zip, download_file, extract_zip
from cfdb.services import locks

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000
DATA_DIR = os.getenv("SYNC_DATA_DIR", ".data")
MATERIALIZE_BIN = os.getenv("MATERIALIZE_BIN", "materialize")
DATABASE_URL = os.getenv("DATABASE_URL", "mongodb://localhost:27017")


class TaskStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class SyncTask:
    id: str
    dcc_names: list[str]
    status: TaskStatus = TaskStatus.RUNNING
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    progress: str = ""
    error: Optional[str] = None
    current_dcc: Optional[str] = None
    current_step: Optional[str] = None


async def is_sync_running() -> bool:
    """Check if a sync task is currently running (via DB lock)."""
    return await locks.is_sync_running()


async def start_sync(task_id: str, dcc_names: list[str]) -> SyncTask:
    """
    Start a new sync task.

    Args:
        task_id: Unique identifier for this task
        dcc_names: List of DCC names to sync (empty = all)

    Returns:
        The created SyncTask

    Raises:
        RuntimeError: If a sync is already running
    """
    # Validate DCC names first
    valid_dccs = get_all_dcc_names()
    if dcc_names:
        for dcc in dcc_names:
            normalized = normalize_dcc_name(dcc)
            if normalized not in valid_dccs:
                raise ValueError(
                    f"Unknown DCC '{dcc}'. Available: {', '.join(valid_dccs)}"
                )

    normalized_names = (
        [normalize_dcc_name(d) for d in dcc_names] if dcc_names else valid_dccs
    )

    # Try to acquire the sync lock (DB-based, works across workers)
    acquired = await locks.try_acquire_sync_lock(task_id, normalized_names)
    if not acquired:
        raise RuntimeError("A sync task is already running")

    task = SyncTask(id=task_id, dcc_names=normalized_names)

    # Run sync in background
    asyncio.create_task(_run_sync(task))

    return task


async def _run_sync(task: SyncTask) -> None:
    """Execute the sync task."""
    try:
        await _sync_dccs(task)
        task.status = TaskStatus.COMPLETED
        task.progress = "Sync completed successfully"
    except Exception as e:
        task.status = TaskStatus.FAILED
        task.error = str(e)
        logger.exception(f"Sync task {task.id} failed: {e}")
    finally:
        task.completed_at = datetime.utcnow()
        task.current_dcc = None
        task.current_step = None
        # Release the sync lock
        await locks.release_sync_lock(task.id)


async def _sync_dccs(task: SyncTask) -> None:
    """Core sync implementation for API."""
    if api.db is None:
        raise RuntimeError("Database not initialized")

    data_path = Path(DATA_DIR)
    data_path.mkdir(exist_ok=True)
    downloads_path = data_path / "downloads"
    downloads_path.mkdir(exist_ok=True)

    for dcc in task.dcc_names:
        task.current_dcc = dcc
        dcc_type = get_dcc_type(dcc)

        # Branch on DCC type
        if dcc_type == "rest_api" and dcc == "encode":
            await _sync_encode(task)
        else:
            await _sync_c2m2_zip(task, data_path, downloads_path)

        logger.info(f"{dcc.upper()} synced successfully")

    task.progress = "All DCCs synced successfully"
    logger.info(task.progress)


async def _sync_c2m2_zip(
    task: SyncTask, data_path: Path, downloads_path: Path
) -> None:
    """Sync a DCC using C2M2 ZIP datapackage."""
    dcc = task.current_dcc
    config = get_dcc_config(dcc)

    # Step 1: Download
    task.current_step = "downloading"
    task.progress = f"Downloading {dcc.upper()} datapackage..."
    logger.info(task.progress)

    url = config["latest_url"]
    zip_filename = Path(url).name
    zip_path = downloads_path / zip_filename

    await download_file(url, zip_path, show_progress=False)

    # Step 2: Extract
    task.current_step = "extracting"
    task.progress = f"Extracting {dcc.upper()} datapackage..."
    logger.info(task.progress)

    extract_dir = data_path / dcc
    extract_zip(zip_path, extract_dir)

    # Step 3 & 4: Clear + Load (CUTOVER - acquire DB lock)
    task.current_step = "cutover"
    task.progress = f"Performing database cutover for {dcc.upper()}..."
    logger.info(task.progress)

    async with locks.CutoverLock(dcc):
        await _clear_dcc_data_async(dcc)
        await _load_dataset_async(extract_dir, dcc)

    # Step 5: Cleanup
    task.current_step = "cleanup"
    task.progress = f"Cleaning up {dcc.upper()}..."
    logger.info(task.progress)

    cleanup_zip(zip_path)

    # Step 6: Materialize files collection for this DCC
    task.current_step = "materializing"
    task.progress = f"Materializing files for {dcc.upper()}..."
    logger.info(task.progress)

    await _materialize_files(dcc)


async def _sync_encode(task: SyncTask) -> None:
    """
    Sync ENCODE data from REST API.

    Unlike C2M2 ZIP sources, ENCODE data is fetched from the REST API
    and pre-materialized directly into the files collection.
    """
    from cfdb.services.encode import (
        build_encode_dcc_record,
        fetch_encode_metadata,
        transform_to_c2m2,
    )

    if api.db is None:
        raise RuntimeError("Database not initialized")

    # Step 1: Clear existing ENCODE data
    task.current_step = "clearing"
    task.progress = "Clearing existing ENCODE data..."
    logger.info(task.progress)

    async with locks.CutoverLock("encode"):
        await _clear_dcc_data_async("encode")

        # Step 2: Upsert DCC record
        task.current_step = "dcc_record"
        task.progress = "Creating ENCODE DCC record..."
        logger.info(task.progress)

        dcc_record = build_encode_dcc_record()
        await api.db.dcc.update_one(
            {"submission": "encode"},
            {"$set": dcc_record},
            upsert=True,
        )

        # Step 3: Fetch and transform files from ENCODE API
        task.current_step = "fetching"
        task.progress = "Fetching files from ENCODE API..."
        logger.info(task.progress)

        batch = []
        count = 0

        async for encode_file in fetch_encode_metadata():
            # Transform to C2M2 format
            doc = transform_to_c2m2(encode_file)
            if doc is None:
                continue

            batch.append(doc)
            count += 1

            # Insert in batches
            if len(batch) >= BATCH_SIZE:
                await api.db.files.insert_many(batch)
                task.progress = f"Inserted {count} ENCODE files..."
                logger.info(task.progress)
                batch.clear()

        # Insert remaining batch
        if batch:
            await api.db.files.insert_many(batch)
            logger.info(f"Inserted final batch, total: {count} ENCODE files")

    task.progress = f"ENCODE sync complete: {count} files"
    logger.info(task.progress)


async def _materialize_files(submission: str) -> None:
    """Run the Rust materializer for a specific DCC submission."""
    materialize_bin = shutil.which(MATERIALIZE_BIN)
    if not materialize_bin:
        logger.warning(
            f"Materialize binary not found ({MATERIALIZE_BIN}), skipping materialization"
        )
        return

    env = os.environ.copy()
    env["DATABASE_URL"] = DATABASE_URL

    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: subprocess.run(
                [materialize_bin, "--submission", submission],
                env=env,
                capture_output=True,
                text=True,
            ),
        )
        if result.returncode != 0:
            logger.error(f"Materialize failed: {result.stderr}")
            raise RuntimeError(f"Materialize failed for {submission}")
        logger.info(f"Materialized files for {submission}")
    except Exception as e:
        logger.error(f"Failed to run materializer: {e}")
        raise


async def _clear_dcc_data_async(submission: str) -> None:
    """Clear DCC data using async Motor client."""
    if api.db is None:
        raise RuntimeError("Database not initialized")

    collection_names = await api.db.list_collection_names()

    for collection_name in collection_names:
        try:
            result = await api.db[collection_name].delete_many(
                {"submission": submission}
            )
            if result.deleted_count > 0:
                logger.info(
                    f"Deleted {result.deleted_count} records from {collection_name}"
                )
        except Exception as e:
            logger.warning(f"Failed to delete from {collection_name}: {e}")


async def _load_dataset_async(directory: Path, submission: str) -> None:
    """Load CSV/TSV files into MongoDB using async Motor client."""
    if api.db is None:
        raise RuntimeError("Database not initialized")

    # Handle nested directories from ZIP extraction
    # Look for CSV/TSV files, checking subdirectories if needed
    files_to_load = [f for f in directory.iterdir() if f.suffix in (".csv", ".tsv")]

    # If no files found at top level, check first non-junk subdirectory
    if not files_to_load:
        for subdir in directory.iterdir():
            if subdir.is_dir() and not subdir.name.startswith("__"):
                files_to_load = [
                    f for f in subdir.iterdir() if f.suffix in (".csv", ".tsv")
                ]
                if files_to_load:
                    break
    logger.info(f"Loading {len(files_to_load)} files into database")

    for filepath in files_to_load:
        delimiter = "," if filepath.suffix == ".csv" else "\t"
        table = filepath.stem

        batch = []
        count = 0

        with open(filepath, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter=delimiter)

            for row in reader:
                count += 1
                record = {**row, "submission": submission, "table": table}

                # Mark 4DN files as public
                if submission == "4dn" and table == "file":
                    record["data_access_level"] = "public"

                batch.append(record)

                if len(batch) >= BATCH_SIZE:
                    await api.db[table].insert_many(copy(batch))
                    batch.clear()

            if batch:
                await api.db[table].insert_many(copy(batch))

        logger.info(f"Loaded {count} records into {table}")
