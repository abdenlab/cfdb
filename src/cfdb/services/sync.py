"""Sync service for API-driven DCC metadata synchronization."""

import asyncio
import csv
import logging
import os
import re
import shutil
import subprocess
from collections import Counter
from contextlib import aclosing
from copy import copy
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import partial
from pathlib import Path
from typing import Optional

from cfdb import api
from cfdb.accessions import normalize_accession
from cfdb.dcc_registry import (
    get_all_dcc_names,
    get_dcc_config,
    get_dcc_type,
    normalize_dcc_name,
)
from cfdb.downloader import cleanup_zip, download_file, extract_zip
from cfdb.indexes import (
    data_index_specs,
    ensure_indexes,
    materialized_files_index_specs,
)
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
    """Core sync implementation for API.

    Each DCC is isolated the same way ``_sync_encode`` isolates its phases:
    one that raises is logged and recorded, and the remaining DCCs still
    run. Letting the first failure escape meant every DCC ordered after it
    was skipped -- ``get_all_dcc_names`` sorts, so one failed ENCODE phase
    cost the whole HuBMAP sync -- along with the data-collection indexes
    built at the end. The run still fails once every DCC has been attempted,
    naming the ones that broke.
    """
    if api.db is None:
        raise RuntimeError("Database not initialized")

    data_path = Path(DATA_DIR)
    data_path.mkdir(exist_ok=True)
    downloads_path = data_path / "downloads"
    downloads_path.mkdir(exist_ok=True)

    failures: list[tuple[str, Exception]] = []

    for dcc in task.dcc_names:
        task.current_dcc = dcc
        dcc_type = get_dcc_type(dcc)

        try:
            # Branch on DCC type
            if dcc_type == "rest_api" and dcc == "encode":
                await _sync_encode(task)
            else:
                await _sync_c2m2_zip(task, data_path, downloads_path)
        except Exception as exc:
            # Narrower than BaseException on purpose, so a CancelledError
            # still cancels the run rather than being recorded as one DCC's
            # failure and followed by every remaining DCC.
            logger.exception(
                "%s sync failed; continuing with the remaining DCCs",
                dcc.upper(),
            )
            failures.append((dcc, exc))
            continue

        logger.info(f"{dcc.upper()} synced successfully")
        await _log_accession_coverage(dcc)

    # Ensure the data-collection query indexes now that data is loaded.
    # Idempotent: a no-op on subsequent syncs. Kept out of API startup
    # (operational indexes only) so we never build these against an
    # empty database on a cold start; likewise skip when no DCC was
    # actually synced so an empty request doesn't build them either.
    # Runs even when a DCC failed: whatever did load still has to be
    # queryable without a full collection scan.
    if task.dcc_names:
        task.current_step = "indexing"
        task.progress = "Ensuring data indexes..."
        logger.info(task.progress)
        await ensure_indexes(api.db, data_index_specs())

    if failures:
        failed_names = ", ".join(dcc for dcc, _ in failures)
        task.progress = (
            f"Sync incomplete: {len(task.dcc_names) - len(failures)} of "
            f"{len(task.dcc_names)} DCCs synced"
        )
        logger.info(task.progress)
        raise RuntimeError(
            f"Sync completed with {len(failures)} failed DCC(s) "
            f"({failed_names})"
        ) from failures[0][1]

    task.progress = "All DCCs synced successfully"
    logger.info(task.progress)


async def _log_accession_coverage(dcc: str) -> None:
    """Report how much of a DCC is queryable by accession after a sync.

    ``accession_id`` fails silently in both directions: a filter against an
    unpopulated corpus returns ``totalCount: 0`` with no error, which reads
    exactly like "no such accession", and a DCC that issues no accession at
    all looks identical. Neither the API nor the client can tell those
    apart, so this log is the only place the distinction is visible --
    which also makes it the signal that a standalone re-materialization
    dropped the 4DN file accessions.

    Advisory only: a coverage shortfall is not a sync failure, and this
    never raises into the sync path.
    """
    if api.db is None:
        return

    try:
        total = await api.db.files.count_documents({"submission": dcc})
        covered = await api.db.files.count_documents(
            {"submission": dcc, "accession_id": {"$ne": None}}
        )
    except Exception as exc:  # pragma: no cover - diagnostics must not break sync
        logger.warning(f"{dcc.upper()} accession coverage unavailable: {exc}")
        return

    if not total:
        return

    pct = 100.0 * covered / total
    message = (
        f"{dcc.upper()} accession coverage: {covered}/{total} files "
        f"carry accession_id ({pct:.1f}%)"
    )
    if covered:
        logger.info(message)
    else:
        logger.warning(
            f"{message}; accession filters will return no matches for this DCC"
        )


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
        await _stamp_4dn_file_accessions()
        await _enrich_4dn_collections()
    elif dcc == "hubmap":
        task.current_step = "enriching_collections"
        task.progress = "Enriching HuBMAP collections and subjects from Search API..."
        logger.info(task.progress)
        dataset_metadata = await _enrich_hubmap_collections_and_subjects()

        task.current_step = "pruning"
        task.progress = "Pruning non-public HuBMAP files..."
        logger.info(task.progress)
        await _prune_non_public_hubmap_raw_records(dataset_metadata)

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
        await _enrich_hubmap_files(dataset_metadata)


async def _set_accession_ids(collection, stamps: list, label: str) -> int:
    """Stamp ``accession_id`` on every document with a parsed accession.

    Kept separate from the Search API enrichment passes because it must
    not inherit their matching: those only update documents the API
    returned metadata for, whereas ``accession_id`` has to land on every
    document whose accession parsed, or the field is queryable for only
    part of the DCC.

    ``stamps`` is a list of ``(document _id, accession)`` pairs, one entry
    per document -- deliberately not the accession-keyed dict the callers
    also build for their Search API fetch. That dict is last-write-wins,
    so two documents resolving to one accession would collapse to a single
    entry and leave the loser unstamped despite having parsed cleanly,
    silently contradicting the guarantee above. Which one lost would
    depend on cursor order, and the end state -- a null ``accession_id``
    -- is indistinguishable from a DCC that issues no accession at all.

    Values are folded through
    :func:`~cfdb.accessions.normalize_accession` so what is stored matches
    what the GraphQL layer folds a filter value to.

    A failed batch is logged and skipped rather than raised. Stamping is
    now the first write of each enrichment pass, so an escaping
    ``BulkWriteError`` would abort the pass before any Search API
    enrichment ran and fail the whole sync -- costing more than the
    accessions it failed to write. The two are independent by design, so a
    partial stamp degrades the field rather than the sync.

    Returns the number of documents modified.
    """
    from pymongo import UpdateOne
    from pymongo.errors import BulkWriteError

    operations = [
        UpdateOne({"_id": doc_id}, {"$set": {"accession_id": normalize_accession(acc)}})
        for doc_id, acc in stamps
    ]
    if not operations:
        logger.warning(f"{label} accession_id: no documents to stamp")
        return 0

    total_modified = 0
    failed = 0
    for i in range(0, len(operations), BATCH_SIZE):
        batch = operations[i : i + BATCH_SIZE]
        try:
            result = await collection.bulk_write(batch, ordered=False)
        except BulkWriteError as exc:
            failed += len(batch)
            logger.error(
                f"{label} accession_id: batch of {len(batch)} failed to stamp; "
                f"continuing so enrichment still runs: {exc}"
            )
            continue
        total_modified += result.modified_count

    if failed:
        logger.error(
            f"{label} accession_id: {failed} of {len(operations)} documents were "
            "not stamped; those files are not queryable by accession"
        )
    logger.info(f"{label} accession_id: stamped {total_modified} documents")
    return total_modified


async def _stamp_4dn_file_accessions() -> None:
    """Stamp accession_id onto the raw 4DN file documents.

    Deliberately targets the raw ``file`` collection rather than the
    materialized ``files``, and therefore runs pre-materialization. The
    materializer rebuilds ``files`` from ``file`` on every run -- dropping
    the collection outright when invoked without a submission filter -- so
    a value written post-materialization survives only until the next
    ``make materialize-dcc`` or ``make materialize-files``, both of which
    are supported standalone operator commands. Stamping the raw document
    instead lets ``enrich_file``'s in-place mutation carry the value
    forward on every rebuild, which is what already makes the collection
    accession durable.

    The accession is parsed from ``persistent_id``, which the raw file rows
    already carry, so this needs nothing the Search API pass provides.
    """
    from cfdb.services.fourdn import extract_accession

    if api.db is None:
        raise RuntimeError("Database not initialized")

    stamps: list[tuple[object, str]] = []
    unparseable = 0
    cursor = api.db.file.find(
        {"submission": "4dn"},
        {"_id": 1, "persistent_id": 1},
    )
    async for doc in cursor:
        acc = extract_accession(doc.get("persistent_id", ""))
        if acc:
            stamps.append((doc["_id"], acc))
        else:
            unparseable += 1

    if unparseable:
        logger.warning(
            f"4DN file accession_id: {unparseable} files have no parseable "
            "accession in persistent_id; accession_id left null"
        )

    await _set_accession_ids(api.db.file, stamps, "4DN file")


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

    # Build accession -> _ids lookup from existing files (avoids $regex per
    # update) first, so enrichment metadata is fetched for exactly those
    # accessions. accession_id itself is stamped pre-materialization by
    # _stamp_4dn_file_accessions, so this pass no longer writes it.
    #
    # Every accession maps to a *list* of document ids. 4DN issues one
    # accession per file, but nothing here enforces that, and a plain
    # accession-keyed dict is last-write-wins: two files resolving to one
    # accession would silently leave the loser un-enriched, chosen by
    # cursor order, with no signal that it happened.
    accession_to_ids: dict[str, list] = {}
    cursor = api.db.files.find(
        {"submission": "4dn"},
        {"_id": 1, "persistent_id": 1},
    )
    async for doc in cursor:
        acc = extract_accession(doc.get("persistent_id", ""))
        if acc:
            accession_to_ids.setdefault(acc, []).append(doc["_id"])

    mapped = sum(len(ids) for ids in accession_to_ids.values())
    logger.info(f"4DN enrichment: {mapped} files in DB mapped by accession")
    if mapped > len(accession_to_ids):
        logger.warning(
            f"4DN enrichment: {mapped - len(accession_to_ids)} files share an "
            "accession with another file; all of them will be enriched alike"
        )

    # Fetch API data for exactly the accessions we hold. Batching the Search
    # API query by accession bypasses its 10k result-window cap, which a
    # blind deep-pagination scan would silently hit and truncate.
    file_metadata = await fetch_file_metadata_bulk(accession_to_ids.keys())
    biosource_tiers = await fetch_biosource_tiers()

    logger.info(
        f"4DN enrichment: {len(file_metadata)} file entries, "
        f"{len(biosource_tiers)} biosource tier entries"
    )

    # Build bulk update operations matched by _id
    operations = []
    for accession, meta in file_metadata.items():
        doc_ids = accession_to_ids.get(accession)
        if not doc_ids:
            continue

        update: dict = {}

        # Promoted to file top-level
        top_level_map = {
            "genome_assembly": "genome_assembly",
            "file_type": "output_type",
            "file_type_detailed": "output_type_detail",
            "condition": "condition",
            "assay_info": "assay_info",
        }
        for src_key, dest_key in top_level_map.items():
            val = meta.get(src_key)
            if val:
                update[dest_key] = val

        # Parse replicate_info into separate fields
        replicate_info = meta.get("replicate_info")
        if replicate_info:
            update["extra.replicate_info"] = replicate_info
            bio = re.search(r"Biorep\s+(\S+)", replicate_info)
            tech = re.search(r"Techrep\s+(\S+)", replicate_info)
            if bio:
                update["biological_replicates"] = bio.group(1)
            if tech:
                update["technical_replicates"] = tech.group(1)

        # Stay on extra (DCC-specific, no cross-DCC equivalent)
        for key in ("biosource_name", "dataset", "extra_files"):
            val = meta.get(key)
            if val:
                update[f"extra.fourdn.{key}"] = val

        # Derive cell_line_tier from biosource_name
        biosource_name = meta.get("biosource_name")
        if biosource_name and biosource_name in biosource_tiers:
            update["extra.fourdn.cell_line_tier"] = biosource_tiers[biosource_name]

        if not update:
            continue

        operations.extend(
            UpdateOne({"_id": doc_id}, {"$set": update}) for doc_id in doc_ids
        )

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

    # Scan and stamp before any network call. An empty API result already
    # left every collection queryable by accession, but a *raised* one did
    # not: fetch_experiment_metadata_bulk catches only aiohttp.ClientError,
    # so a TimeoutError from its 60s budget propagated and nothing was
    # stamped at all. Collecting first makes that guarantee structural
    # rather than a property of where the await happens to sit, and matches
    # the ordering the file pass already uses.
    cursor = api.db.collection.find(
        {"submission": "4dn"},
        {"_id": 1, "persistent_id": 1},
    )
    unparseable = 0
    stamps: list[tuple[object, str]] = []
    async for doc in cursor:
        accession = extract_experiment_accession(doc.get("persistent_id", ""))
        if not accession:
            unparseable += 1
            continue

        # One entry per document rather than an accession-keyed dict, so two
        # collections sharing an accession both get stamped instead of one
        # silently winning.
        stamps.append((doc["_id"], accession))

    if unparseable:
        logger.warning(
            f"4DN collection enrichment: {unparseable} collections have no "
            "parseable accession in persistent_id; accession_id left null"
        )

    # This runs pre-materialization, so the materializer embeds the value
    # into files.collections[].
    await _set_accession_ids(api.db.collection, stamps, "4DN collection")

    # Fetch experiment metadata from 4DN API
    experiment_metadata = await fetch_experiment_metadata_bulk()

    logger.info(f"4DN collection enrichment: {len(experiment_metadata)} experiment entries")

    # Build bulk updates from the accessions already collected above
    operations = []
    matched = 0
    for doc_id, accession in stamps:
        meta = experiment_metadata.get(accession)
        if not meta:
            continue

        matched += 1
        update: dict = {}

        # Promote selected fields to Collection top-level
        if meta.get("lab"):
            update["lab"] = meta["lab"]
        if meta.get("experiment_type"):
            update["experiment_type"] = meta["experiment_type"]

        # Nest remaining under extra.fourdn
        remaining = {k: v for k, v in meta.items() if k not in ("lab", "experiment_type")}
        if remaining:
            update["extra.fourdn"] = remaining

        if update:
            operations.append(UpdateOne({"_id": doc_id}, {"$set": update}))

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


async def _enrich_hubmap_collections_and_subjects() -> dict[str, dict]:
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

        update_set: dict = {}

        # Promoted to Collection top-level
        dataset_type = meta.get("dataset_type")
        if dataset_type is not None:
            update_set["experiment_type"] = dataset_type
        analyte_class = meta.get("analyte_class")
        if analyte_class is not None:
            update_set["analyte_class"] = analyte_class

        # HuBMAP-specific fields stay nested on extra.hubmap
        hubmap_extra: dict = {}
        for key in (
            "pipeline",
            "processing",
            "group_name",
            "visualization",
            "vitessce_hints",
            "metadata",
        ):
            val = meta.get(key)
            if val is not None:
                hubmap_extra[key] = val

        if hubmap_extra:
            update_set["extra.hubmap"] = hubmap_extra

        if update_set:
            collection_ops.append(
                UpdateOne({"_id": doc["_id"]}, {"$set": update_set})
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
        return dataset_metadata

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

    return dataset_metadata


async def _prune_non_public_hubmap_raw_records(
    dataset_metadata: dict[str, dict],
) -> None:
    """Remove non-public HuBMAP records from raw C2M2 tables before materialization.

    Deletes ``file_in_collection`` links and orphaned ``file`` rows for any
    dataset whose ``data_access_level`` is not ``"public"``.  Datasets with a
    missing access level are treated as non-public (conservative default).
    """
    if api.db is None:
        raise RuntimeError("Database not initialized")

    # 1. Identify non-public DOIs
    non_public_dois = [
        doi
        for doi, meta in dataset_metadata.items()
        if meta.get("data_access_level") != "public"
    ]

    if not non_public_dois:
        logger.info("HuBMAP pruning: all datasets are public, nothing to prune")
        return

    logger.info(
        f"HuBMAP pruning: {len(non_public_dois)} non-public datasets identified"
    )

    # 2. Find matching raw collection records
    collection_keys: list[dict] = []
    cursor = api.db.collection.find(
        {
            "submission": "hubmap",
            "persistent_id": {"$in": non_public_dois},
        },
        {"id_namespace": 1, "local_id": 1},
    )
    async for doc in cursor:
        collection_keys.append(
            {
                "id_namespace": doc["id_namespace"],
                "local_id": doc["local_id"],
            }
        )

    if not collection_keys:
        logger.info("HuBMAP pruning: no raw collection records match non-public DOIs")
        return

    # 3. Collect potentially-orphaned file keys from file_in_collection
    or_filter = [
        {
            "collection_id_namespace": ck["id_namespace"],
            "collection_local_id": ck["local_id"],
        }
        for ck in collection_keys
    ]

    candidate_file_keys: set[tuple[str, str]] = set()
    cursor = api.db.file_in_collection.find(
        {"$or": or_filter},
        {"file_id_namespace": 1, "file_local_id": 1},
    )
    async for doc in cursor:
        candidate_file_keys.add(
            (doc["file_id_namespace"], doc["file_local_id"])
        )

    # 4. Delete non-public file_in_collection links
    fic_result = await api.db.file_in_collection.delete_many({"$or": or_filter})
    logger.info(
        f"HuBMAP pruning: deleted {fic_result.deleted_count} file_in_collection links"
    )

    # 5. Find which candidate files still have links (shared with a public collection)
    if candidate_file_keys:
        still_linked: set[tuple[str, str]] = set()
        file_or_filter = [
            {"file_id_namespace": ns, "file_local_id": lid}
            for ns, lid in candidate_file_keys
        ]
        cursor = api.db.file_in_collection.find(
            {"$or": file_or_filter},
            {"file_id_namespace": 1, "file_local_id": 1},
        )
        async for doc in cursor:
            still_linked.add(
                (doc["file_id_namespace"], doc["file_local_id"])
            )

        orphaned = candidate_file_keys - still_linked

        # 6. Delete truly orphaned files
        if orphaned:
            orphan_filter = [
                {"id_namespace": ns, "local_id": lid}
                for ns, lid in orphaned
            ]
            file_result = await api.db.file.delete_many({"$or": orphan_filter})
            logger.info(
                f"HuBMAP pruning: deleted {file_result.deleted_count} orphaned file records"
            )
        else:
            logger.info("HuBMAP pruning: no orphaned file records to delete")
    else:
        logger.info("HuBMAP pruning: no file_in_collection links found for non-public collections")


async def _enrich_hubmap_files(dataset_metadata: dict[str, dict]) -> None:
    """Enrich materialized HuBMAP files with metadata from the Search API.

    Stores ``genome_assembly``, ``is_data_product``, and ``rel_path`` on
    ``extra``, then stamps all HuBMAP files as ``data_access_level = "public"``
    (non-public records were already pruned before materialization).
    """
    from pymongo import UpdateOne

    if api.db is None:
        raise RuntimeError("Database not initialized")

    # Build DOI -> file-level lookup
    doi_file_info: dict[str, dict] = {}
    for doi_url, meta in dataset_metadata.items():
        info: dict = {}

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
        # Still stamp all HuBMAP files as public
        await api.db.files.update_many(
            {"submission": "hubmap"},
            {"$set": {"data_access_level": "public"}},
        )
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

        # Promoted field
        genome_assembly = matched_info.get("genome_assembly")
        if genome_assembly:
            update["genome_assembly"] = genome_assembly

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
        # Still stamp all HuBMAP files as public
        await api.db.files.update_many(
            {"submission": "hubmap"},
            {"$set": {"data_access_level": "public"}},
        )
        return

    total_modified = 0
    for i in range(0, len(operations), BATCH_SIZE):
        batch = operations[i : i + BATCH_SIZE]
        result = await api.db.files.bulk_write(batch, ordered=False)
        total_modified += result.modified_count

    logger.info(f"HuBMAP file enrichment: updated {total_modified} files")

    # All surviving HuBMAP files are public (non-public pruned before materialization)
    await api.db.files.update_many(
        {"submission": "hubmap"},
        {"$set": {"data_access_level": "public"}},
    )


async def _ingest_encode_rows(
    task: SyncTask,
    label: str,
    rows,
    transform,
    compression_counts: Counter[str | None],
    annotation_type_counts: Counter[str],
    counts_by_phase: dict[str, int],
    stale_filter: dict,
) -> None:
    """Transform and batch-insert one ENCODE metadata stream.

    Shared by the experiment and annotation phases so both use the same
    batching, the same skip-on-None handling, and contribute to the same
    corpus-wide tallies.

    The row count is reported by mutating ``counts_by_phase`` rather than by
    returning, and a partial batch left buffered by a dead stream is flushed
    in a ``finally``. Both exist for the same reason: a stream can die
    *mid-flight* -- an ``asyncio.TimeoutError`` against the metadata budget
    is the known ENCODE failure mode -- and a return value is lost when it
    does. Reporting by return meant a failed phase contributed nothing to
    the caller's total even though its completed batches were already
    committed, so the sync under-reported the corpus by up to everything
    that phase had loaded, while the tallies below (mutated per row, not per
    batch) over-reported it by the discarded partial batch. Three numbers in
    one log block that disagreed with each other and with the database.

    The *clean-path* flush is deliberately not in that ``finally``. A
    ``finally`` runs whether or not an exception is in flight, so suppressing
    the flush's failure there suppressed it on the success path too: a stream
    that drained cleanly and then failed to commit its trailing rows returned
    normally and the sync reported a clean load over a corpus short by up to
    ``BATCH_SIZE - 1`` rows, against a DCC cleared before the load.

    Args:
        task: Sync task whose ``progress`` is updated per batch.
        label: Names this stream in progress and log lines.
        rows: Async iterable of TSV row dicts.
        transform: Row -> document callable; None means skip the row.
        compression_counts: Mutated with each document's compression term.
        annotation_type_counts: Mutated with each document's annotation type.
        counts_by_phase: Mutated with this phase's committed row count, on
            the failure path as well as the success path.
        stale_filter: Selects the slice of ``files`` this phase owns, deleted
            once replacement rows are in hand. See ``clear_stale_once``.
    """
    batch: list[dict] = []
    count = 0
    stale_cleared = False

    async def clear_stale_once() -> None:
        """Delete the slice this phase owns, once, before its first write.

        Deferred to the first batch rather than run before the stream opens.
        The known ENCODE failures -- a 504 while the portal assembles the
        response, an exhausted download budget -- strike before any row
        arrives, and clearing up front would delete the slice and then have
        nothing to put back, serving an empty one until the next successful
        sync. Deferring means the previous rows keep being served instead.
        A stream that dies *mid*-load still leaves a partial slice; only a
        shadow-and-swap would fix that.
        """
        nonlocal stale_cleared
        if stale_cleared:
            return
        stale_cleared = True
        deleted = await api.db.files.delete_many(
            {"submission": "encode", **stale_filter}
        )
        logger.info(
            "ENCODE %s: cleared %d stale files", label, deleted.deleted_count
        )

    async def commit(pending: list[dict]) -> None:
        """Send one batch, discounting it from the tally if it does not land."""
        nonlocal count
        await clear_stale_once()
        try:
            await api.db.files.insert_many(pending)
        except Exception:
            count -= len(pending)
            raise

    try:
        # ``aclosing`` because the caller isolates phase failures with a
        # broad ``except``, which abandons this generator mid-iteration.
        # The stream owns an aiohttp session inside its own ``async with``,
        # released only when the generator is finalized -- left to the
        # garbage collector that is neither prompt nor guaranteed in a
        # long-lived API process, and the annotation fan-out turned one
        # stream per sync into N+1.
        async with aclosing(rows):
            async for row in rows:
                doc = transform(row)
                if doc is None:
                    continue

                compression_counts[doc.get("compression_format")] += 1
                annotation_type = (
                    doc.get("extra", {}).get("encode", {}).get("annotation_type")
                )
                if annotation_type:
                    annotation_type_counts[annotation_type] += 1

                batch.append(doc)
                count += 1

                # Insert in batches. The buffer is detached *before* the
                # await rather than cleared after it, so an insert that
                # raises leaves nothing behind for the flush below to submit
                # a second time. Re-submitting is not harmless: the driver
                # stamps ``_id`` in place on these dicts before sending, so
                # a retry collides on the committed prefix and silently
                # drops the batch's uncommitted suffix.
                if len(batch) >= BATCH_SIZE:
                    pending, batch = batch, []
                    await commit(pending)
                    task.progress = f"Inserted {count} ENCODE {label} files..."
                    logger.info(task.progress)

        # The stream drained without error, so its result is authoritative
        # even when it is empty: clear here too, or a type ENCODE stopped
        # publishing would keep serving last sync's rows indefinitely.
        await clear_stale_once()

        # The clean-path flush: inside the ``try``, so a sink failure with no
        # exception in flight fails the phase rather than being swallowed.
        if batch:
            pending, batch = batch, []
            await commit(pending)
    except asyncio.CancelledError:
        # Never write on the way out of a cancellation. The decision to stop
        # has already been made, and the flush below could not protect the
        # unwind anyway: its ``except Exception`` does not catch a
        # re-delivered CancelledError, which would replace the original and
        # skip the ``counts_by_phase`` record.
        count -= len(batch)
        batch.clear()
        raise
    finally:
        # Only reachable with rows still buffered when the stream itself died
        # mid-flight -- the detach above guarantees they were never
        # submitted. They are already counted in the tallies, and the DCC was
        # cleared before the load, so discarding them would lose data for no
        # gain.
        if batch:
            try:
                await commit(batch)
            except Exception:
                # Never let the flush replace the exception that caused it.
                logger.exception(
                    "ENCODE %s: failed to commit the final %d-row batch",
                    label,
                    len(batch),
                )
        counts_by_phase[label] = count
        logger.info(f"ENCODE {label} ingest complete: {count} files")


async def _sync_encode(task: SyncTask) -> None:
    """
    Sync ENCODE data from REST API.

    Unlike C2M2 ZIP sources, ENCODE data is fetched from the REST API
    and pre-materialized directly into the files collection.

    Runs one phase for released experiments plus one per configured
    ``annotation_type``. Each phase is isolated: a phase that raises is
    logged and recorded, and the remaining phases still run, so a transient
    failure on one stream does not cost the corpus the others. The task is
    still failed at the end if any phase failed -- the alternative would
    report a clean sync over a partially loaded collection.

    Isolation covers the destructive half too. Each phase owns a slice of
    ``files`` and deletes only that slice, and only once it has rows to put
    back, so a phase that fails before delivering any -- the shape a portal
    504 or an exhausted download budget takes -- leaves its previous rows
    being served rather than emptying them. A phase that dies partway
    through still leaves its own slice partially loaded; nothing short of
    loading into a shadow collection and swapping would change that.
    """
    from cfdb.services.encode import (
        annotation_types_from_env,
        build_encode_dcc_record,
        fetch_encode_annotation_metadata,
        fetch_encode_metadata,
        metadata_budget_seconds,
        transform_annotation_to_c2m2,
        transform_to_c2m2,
    )

    if api.db is None:
        raise RuntimeError("Database not initialized")

    # Step 1: Clear existing ENCODE data
    task.current_step = "clearing"
    task.progress = "Clearing existing ENCODE data..."
    logger.info(task.progress)

    count = 0
    counts_by_phase: dict[str, int] = {}
    failures: list[tuple[str, Exception]] = []
    # Tally the derived compression terms. The derivation depends on the
    # shape of ENCODE's download URLs, which nobody here controls, so a
    # corpus-wide flip (an upstream column rename, a redirect stub) would
    # otherwise be invisible behind an unchanged row count.
    compression_counts: Counter[str | None] = Counter()
    # Same reasoning for annotation_type, and seeded from the configured
    # allowlist below so a type that returns nothing is reported as zero
    # rather than simply missing from the log.
    annotation_type_counts: Counter[str] = Counter()

    async with locks.CutoverLock("encode"):
        # ``files`` is deliberately not cleared here. One corpus-wide delete
        # before a fan-out designed to survive a phase failure means a failed
        # phase serves an absent slice rather than a stale one -- the
        # experiment phase is the ~295k-file bulk and the likeliest to time
        # out, and losing it leaves the API serving the ~29k annotation
        # documents as the whole corpus. Each phase clears only what it is
        # about to reload, immediately before reloading it.
        await _clear_dcc_data_async("encode", skip_collections={"files"})

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

        # Step 3: Fetch and transform files from ENCODE API. The experiment
        # stream first, then one per configured annotation type.
        task.current_step = "fetching"
        task.progress = "Fetching files from ENCODE API..."
        logger.info(task.progress)

        # One deadline for the whole fan-out, resolved here rather than per
        # stream. Every phase runs inside the cutover lock held above, which
        # gates the read surface, so a per-stream budget would multiply the
        # outage by the number of phases and carry the worst case past the
        # sync lock's one-hour stale threshold -- admitting a second sync
        # that clears the corpus while this one is still writing to it. A
        # phase that spends what is left fails as an ordinary TimeoutError,
        # so the isolation below still applies and its rows are still
        # committed; the phases after it fail immediately rather than
        # opening a request they have no time to finish.
        deadline = asyncio.get_running_loop().time() + metadata_budget_seconds()

        # Streams are built by a factory rather than constructed up front, so
        # a phase that is never reached never opens a request. Each phase
        # carries the filter selecting the slice of ``files`` it owns, so it
        # replaces exactly what it reloads. The experiment slice is defined
        # by absence: an annotation row published with a blank ``Annotation
        # type`` produces no such key, so the experiment phase sweeps it and
        # its own phase reinserts it -- it is lost only if that phase also
        # fails.
        phases = [
            (
                "experiment",
                partial(fetch_encode_metadata, deadline=deadline),
                transform_to_c2m2,
                {"extra.encode.annotation_type": {"$exists": False}},
            )
        ]
        for annotation_type in annotation_types_from_env():
            # Seeded at zero so a configured type that returns nothing logs
            # ": 0" rather than vanishing from the distribution. An absent
            # key is what nobody notices; a zero is what everybody does.
            annotation_type_counts[annotation_type] = 0
            phases.append(
                (
                    f"annotation[{annotation_type}]",
                    partial(
                        fetch_encode_annotation_metadata,
                        annotation_type,
                        deadline=deadline,
                    ),
                    transform_annotation_to_c2m2,
                    {"extra.encode.annotation_type": annotation_type},
                )
            )

        for label, open_stream, transform, stale_filter in phases:
            try:
                await _ingest_encode_rows(
                    task,
                    label,
                    open_stream(),
                    transform,
                    compression_counts,
                    annotation_type_counts,
                    counts_by_phase,
                    stale_filter,
                )
            except Exception as exc:
                # Deliberately broad: the point is that no failure mode of
                # one stream -- timeout, HTTP error, malformed TSV -- may
                # stop the others from being attempted. Narrower than
                # BaseException on purpose, so a CancelledError still
                # cancels the sync rather than being logged as a phase
                # failure and followed by N more phases.
                logger.exception(
                    "ENCODE %s ingest failed; continuing with the remaining "
                    "phases",
                    label,
                )
                failures.append((label, exc))
                continue

        # Summed from the accumulator rather than tracked alongside it, so
        # a phase that failed after committing rows still contributes what
        # it actually loaded.
        count = sum(counts_by_phase.values())

    # ENCODE writes straight into the materialized collection and never runs
    # the materializer, which is the only other creator of files indexes. On
    # a database where ENCODE is the only DCC synced, that leaves files with
    # no indexes at all and makes every accession lookup a full scan. Run
    # unconditionally: whatever a partially failed sync did load still has to
    # be servable without a full collection scan.
    await ensure_indexes(api.db, materialized_files_index_specs())

    if failures:
        # counts_by_phase carries an entry for every phase that started,
        # failed ones included -- they may have committed rows before
        # dying -- so the succeeded tally comes from the phase list, not
        # from the accumulator's length.
        task.progress = (
            f"ENCODE sync incomplete: {count} files from "
            f"{len(phases) - len(failures)} of {len(phases)} phases"
        )
    else:
        task.progress = f"ENCODE sync complete: {count} files"
    logger.info(task.progress)
    logger.info("ENCODE files inserted per phase: %s", counts_by_phase)
    logger.info(
        "ENCODE compression_format distribution: %s",
        {
            ("undetermined" if term is None else term or "uncompressed"): tally
            for term, tally in compression_counts.most_common()
        },
    )
    logger.info(
        "ENCODE annotation_type distribution: %s", dict(annotation_type_counts)
    )

    if failures:
        failed_labels = ", ".join(label for label, _ in failures)
        raise RuntimeError(
            f"ENCODE sync completed with {len(failures)} failed phase(s) "
            f"({failed_labels}); {count} files were loaded from the phases "
            "that succeeded"
        ) from failures[0][1]


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


async def _clear_dcc_data_async(
    submission: str, skip_collections: set[str] | None = None
) -> None:
    """Clear DCC data using async Motor client.

    Args:
        submission: DCC whose documents are removed.
        skip_collections: Collections left untouched. The ENCODE sync passes
            ``files`` here because it clears that collection one phase-slice
            at a time instead; see :func:`_sync_encode`.
    """
    if api.db is None:
        raise RuntimeError("Database not initialized")

    collection_names = await api.db.list_collection_names()
    skip = skip_collections or set()

    for collection_name in collection_names:
        if collection_name in skip:
            continue
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
