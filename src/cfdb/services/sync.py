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

    # Step 6: Pre-materialization enrichment
    if dcc == "4dn":
        task.current_step = "enriching_collections"
        task.progress = "Enriching 4DN collections from experiment API..."
        logger.info(task.progress)
        await _enrich_4dn_collections()
    elif dcc == "hubmap":
        task.current_step = "enriching_collections"
        task.progress = "Enriching HuBMAP collections and subjects from Search API..."
        logger.info(task.progress)
        await _enrich_hubmap_collections_and_subjects()

    # Step 7: Materialize files collection for this DCC
    task.current_step = "materializing"
    task.progress = f"Materializing files for {dcc.upper()}..."
    logger.info(task.progress)

    await _materialize_files(dcc)

    # Step 8: Post-materialization enrichment
    if dcc == "4dn":
        task.current_step = "enriching"
        task.progress = "Enriching 4DN files from portal API..."
        logger.info(task.progress)
        await _enrich_4dn_api_metadata()
    elif dcc == "hubmap":
        task.current_step = "enriching"
        task.progress = "Enriching HuBMAP files from Search API..."
        logger.info(task.progress)
        await _enrich_hubmap_files()


async def _enrich_4dn_api_metadata() -> None:
    """Enrich materialized 4DN files with metadata from the 4DN Search API."""
    from pymongo import UpdateOne

    from cfdb.services.fourdn import (
        extract_accession,
        fetch_biosource_tiers,
        fetch_file_metadata_bulk,
    )

    if api.db is None:
        raise RuntimeError("Database not initialized")

    # Fetch API data
    file_metadata = await fetch_file_metadata_bulk()
    biosource_tiers = await fetch_biosource_tiers()

    logger.info(
        f"4DN enrichment: {len(file_metadata)} file entries, "
        f"{len(biosource_tiers)} biosource tier entries"
    )

    # Build accession -> _id lookup from existing files (avoids $regex per update)
    accession_to_id: dict[str, object] = {}
    cursor = api.db.files.find(
        {"submission": "4dn"},
        {"_id": 1, "persistent_id": 1},
    )
    async for doc in cursor:
        acc = extract_accession(doc.get("persistent_id", ""))
        if acc:
            accession_to_id[acc] = doc["_id"]

    logger.info(f"4DN enrichment: {len(accession_to_id)} files in DB mapped by accession")

    # Build bulk update operations matched by _id
    operations = []
    for accession, meta in file_metadata.items():
        doc_id = accession_to_id.get(accession)
        if not doc_id:
            continue

        update: dict = {}

        for key in (
            "genome_assembly",
            "file_type",
            "file_type_detailed",
            "condition",
            "biosource_name",
            "dataset",
            "experiment_type",
            "assay_info",
            "replicate_info",
            "extra_files",
        ):
            val = meta.get(key)
            if val:
                update[f"extra.fourdn.{key}"] = val

        # Derive cell_line_tier from biosource_name
        biosource_name = meta.get("biosource_name")
        if biosource_name and biosource_name in biosource_tiers:
            update["extra.fourdn.cell_line_tier"] = biosource_tiers[biosource_name]

        if not update:
            continue

        operations.append(UpdateOne({"_id": doc_id}, {"$set": update}))

    if not operations:
        logger.warning("4DN enrichment: no updates to apply")
        return

    # Execute in batches
    total_modified = 0
    for i in range(0, len(operations), BATCH_SIZE):
        batch = operations[i : i + BATCH_SIZE]
        result = await api.db.files.bulk_write(batch, ordered=False)
        total_modified += result.modified_count

    logger.info(f"4DN enrichment: updated {total_modified} files")


async def _enrich_4dn_collections() -> None:
    """Enrich 4DN collection documents with experiment metadata from the 4DN Search API."""
    from pymongo import UpdateOne

    from cfdb.services.fourdn import (
        extract_experiment_accession,
        fetch_experiment_metadata_bulk,
    )

    if api.db is None:
        raise RuntimeError("Database not initialized")

    # Fetch experiment metadata from 4DN API
    experiment_metadata = await fetch_experiment_metadata_bulk()

    logger.info(f"4DN collection enrichment: {len(experiment_metadata)} experiment entries")

    # Build bulk updates: match collection docs by experiment accession in persistent_id
    operations = []
    cursor = api.db.collection.find(
        {"submission": "4dn"},
        {"_id": 1, "persistent_id": 1},
    )
    matched = 0
    async for doc in cursor:
        accession = extract_experiment_accession(doc.get("persistent_id", ""))
        if not accession:
            continue

        meta = experiment_metadata.get(accession)
        if not meta:
            continue

        matched += 1
        # Extract lab to top-level collection field; nest remaining under extra.fourdn
        update: dict = {"extra.fourdn": meta}
        lab = meta.get("lab")
        if lab:
            update["lab"] = lab
        operations.append(UpdateOne({"_id": doc["_id"]}, {"$set": update}))

    if not operations:
        logger.warning("4DN collection enrichment: no updates to apply")
        return

    # Execute in batches
    total_modified = 0
    for i in range(0, len(operations), BATCH_SIZE):
        batch = operations[i : i + BATCH_SIZE]
        result = await api.db.collection.bulk_write(batch, ordered=False)
        total_modified += result.modified_count

    logger.info(
        f"4DN collection enrichment: matched {matched} collections, "
        f"updated {total_modified}"
    )


async def _enrich_hubmap_collections_and_subjects() -> None:
    """Enrich HuBMAP collections and subjects with metadata from the Search API.

    Performs a single bulk fetch of all HuBMAP datasets, then:
    1. Stores dataset-level metadata on collection.extra.hubmap
    2. Stores donor demographics on subject.extra.hubmap
    """
    from pymongo import UpdateOne

    from cfdb.services.hubmap import fetch_dataset_metadata_bulk

    if api.db is None:
        raise RuntimeError("Database not initialized")

    # Single bulk fetch for all datasets
    dataset_metadata = await fetch_dataset_metadata_bulk()
    logger.info(f"HuBMAP enrichment: {len(dataset_metadata)} dataset entries fetched")

    # --- Collection enrichment ---
    collection_ops = []
    cursor = api.db.collection.find(
        {"submission": "hubmap"},
        {"_id": 1, "persistent_id": 1},
    )
    async for doc in cursor:
        persistent_id = doc.get("persistent_id", "")
        meta = dataset_metadata.get(persistent_id)
        if not meta:
            continue

        extra = {}
        for key in (
            "dataset_type",
            "pipeline",
            "processing",
            "group_name",
            "analyte_class",
            "visualization",
            "vitessce_hints",
            "metadata",
        ):
            val = meta.get(key)
            if val is not None:
                extra[key] = val

        if extra:
            collection_ops.append(
                UpdateOne({"_id": doc["_id"]}, {"$set": {"extra.hubmap": extra}})
            )

    if collection_ops:
        total_modified = 0
        for i in range(0, len(collection_ops), BATCH_SIZE):
            batch = collection_ops[i : i + BATCH_SIZE]
            result = await api.db.collection.bulk_write(batch, ordered=False)
            total_modified += result.modified_count
        logger.info(f"HuBMAP collection enrichment: updated {total_modified} collections")
    else:
        logger.warning("HuBMAP collection enrichment: no updates to apply")

    # --- Subject enrichment ---
    # Build donor_uuid -> donor_metadata lookup from the bulk fetch
    donor_lookup: dict[str, dict] = {}
    for meta in dataset_metadata.values():
        donor_uuid = meta.get("donor_uuid")
        donor_metadata = meta.get("donor_metadata")
        if donor_uuid and donor_metadata:
            donor_lookup[donor_uuid] = donor_metadata

    if not donor_lookup:
        logger.info("HuBMAP subject enrichment: no donor metadata available")
        return

    # Match subjects by local_id containing the donor UUID (HuBMAP C2M2 uses
    # donor UUID as part of the subject local_id)
    subject_ops = []
    cursor = api.db.subject.find(
        {"submission": "hubmap"},
        {"_id": 1, "local_id": 1},
    )
    async for doc in cursor:
        local_id = doc.get("local_id", "")
        # Try to match donor UUID in the subject local_id
        matched_donor = None
        for donor_uuid, donor_meta in donor_lookup.items():
            if donor_uuid in local_id:
                matched_donor = donor_meta
                break

        if not matched_donor:
            continue

        extra: dict = {}
        for key in (
            "age_value",
            "age_unit",
            "body_mass_index_value",
            "body_mass_index_unit",
            "cause_of_death",
            "death_event",
            "mechanism_of_injury",
            "height_value",
            "height_unit",
            "weight_value",
            "weight_unit",
        ):
            val = matched_donor.get(key)
            if val is not None:
                # HuBMAP returns single-element lists for some fields
                if isinstance(val, list) and len(val) == 1:
                    val = val[0]
                extra[key] = val

        # String list fields
        for key in ("sex", "race"):
            val = matched_donor.get(key)
            if val is not None:
                if isinstance(val, list) and len(val) == 1:
                    val = val[0]
                extra[key] = val

        for key in ("medical_history", "social_history"):
            val = matched_donor.get(key)
            if isinstance(val, list) and val:
                extra[key] = val

        if extra:
            subject_ops.append(
                UpdateOne({"_id": doc["_id"]}, {"$set": {"extra.hubmap": extra}})
            )

    if subject_ops:
        total_modified = 0
        for i in range(0, len(subject_ops), BATCH_SIZE):
            batch = subject_ops[i : i + BATCH_SIZE]
            result = await api.db.subject.bulk_write(batch, ordered=False)
            total_modified += result.modified_count
        logger.info(f"HuBMAP subject enrichment: updated {total_modified} subjects")
    else:
        logger.warning("HuBMAP subject enrichment: no updates to apply")


async def _enrich_hubmap_files() -> None:
    """Enrich materialized HuBMAP files with metadata from the Search API.

    Sets top-level ``data_access_level`` and stores ``genome_assembly``,
    ``is_data_product``, and ``rel_path`` on ``extra``.
    """
    from pymongo import UpdateOne

    from cfdb.services.hubmap import fetch_dataset_metadata_bulk

    if api.db is None:
        raise RuntimeError("Database not initialized")

    # Bulk fetch (should be fast from API cache if collections step already ran)
    dataset_metadata = await fetch_dataset_metadata_bulk()

    # Build DOI -> file-level lookup
    doi_file_info: dict[str, dict] = {}
    for doi_url, meta in dataset_metadata.items():
        info: dict = {}

        access_level = meta.get("data_access_level")
        if access_level:
            info["data_access_level"] = access_level

        genome_assembly = meta.get("genome_assembly")
        if genome_assembly:
            info["genome_assembly"] = genome_assembly

        # Build filename -> file metadata lookup from files array
        files_list = meta.get("files", [])
        file_lookup: dict[str, dict] = {}
        for f in files_list:
            rel_path = f.get("rel_path")
            if rel_path:
                entry = {"rel_path": rel_path}
                is_data_product = f.get("is_data_product")
                if is_data_product is not None:
                    entry["is_data_product"] = is_data_product
                # Use the basename as key for matching with C2M2 filename
                basename = rel_path.rsplit("/", 1)[-1] if "/" in rel_path else rel_path
                file_lookup[basename] = entry
        if file_lookup:
            info["file_lookup"] = file_lookup

        if info:
            doi_file_info[doi_url] = info

    if not doi_file_info:
        logger.warning("HuBMAP file enrichment: no dataset metadata available")
        return

    # Get DOI URLs for all HuBMAP collections (to map files to datasets)
    collection_doi_map: dict[str, str] = {}  # (id_namespace, local_id) key -> doi_url
    cursor = api.db.collection.find(
        {"submission": "hubmap"},
        {"_id": 1, "id_namespace": 1, "local_id": 1, "persistent_id": 1},
    )
    async for doc in cursor:
        persistent_id = doc.get("persistent_id", "")
        if persistent_id in doi_file_info:
            key = f"{doc.get('id_namespace', '')}|{doc.get('local_id', '')}"
            collection_doi_map[key] = persistent_id

    logger.info(
        f"HuBMAP file enrichment: {len(collection_doi_map)} collections matched to datasets"
    )

    # Iterate materialized files and build updates
    operations = []
    cursor = api.db.files.find(
        {"submission": "hubmap"},
        {"_id": 1, "filename": 1, "collections": 1},
    )
    async for doc in cursor:
        # Find matching dataset via collection DOI
        collections = doc.get("collections", [])
        matched_info = None
        for coll in collections:
            if isinstance(coll, dict):
                key = f"{coll.get('id_namespace', '')}|{coll.get('local_id', '')}"
            else:
                continue
            doi_url = collection_doi_map.get(key)
            if doi_url:
                matched_info = doi_file_info.get(doi_url)
                if matched_info:
                    break

        if not matched_info:
            continue

        update: dict = {}

        # Top-level field
        access_level = matched_info.get("data_access_level")
        if access_level:
            update["data_access_level"] = access_level

        # Extra fields
        genome_assembly = matched_info.get("genome_assembly")
        if genome_assembly:
            update["extra.hubmap.genome_assembly"] = genome_assembly

        # Try to match by filename to get per-file metadata
        filename = doc.get("filename", "")
        file_lookup = matched_info.get("file_lookup", {})
        if filename and filename in file_lookup:
            file_meta = file_lookup[filename]
            rel_path = file_meta.get("rel_path")
            if rel_path:
                update["extra.hubmap.rel_path"] = rel_path
            is_data_product = file_meta.get("is_data_product")
            if is_data_product is not None:
                update["extra.hubmap.is_data_product"] = is_data_product

        if update:
            operations.append(UpdateOne({"_id": doc["_id"]}, {"$set": update}))

    if not operations:
        logger.warning("HuBMAP file enrichment: no updates to apply")
        return

    total_modified = 0
    for i in range(0, len(operations), BATCH_SIZE):
        batch = operations[i : i + BATCH_SIZE]
        result = await api.db.files.bulk_write(batch, ordered=False)
        total_modified += result.modified_count

    logger.info(f"HuBMAP file enrichment: updated {total_modified} files")


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
