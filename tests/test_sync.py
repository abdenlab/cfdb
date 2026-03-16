"""Tests for sync service parallelization, shared memory, and enrichment."""

from __future__ import annotations

import asyncio
from multiprocessing.shared_memory import SharedMemory
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cfdb.services.sync import (
    _clear_dcc_data_async,
    _enrich_4dn_collections_batch,
    _enrich_4dn_files_batch,
    _enrich_hubmap_collections_and_subjects,
    _enrich_hubmap_collections_batch,
    _enrich_hubmap_files,
    _enrich_hubmap_subjects_batch,
    _load_dataset_async,
    _load_file,
    _prune_non_public_hubmap_raw_records,
    _transform_encode_batch,
    cleanup_shared,
    read_shared,
    write_shared,
)


# ---------------------------------------------------------------------------
# Shared memory helpers
# ---------------------------------------------------------------------------


class TestSharedMemoryExchange:
    def test_write_shared_with_dict(self):
        """
        GIVEN a small dictionary
        WHEN write_shared + read_shared round-trip
        THEN the original data is recovered
        """
        data = {"key": "value", "num": 42}
        name, size = write_shared(data)
        try:
            result = read_shared(name, size)
            assert result == data
        finally:
            cleanup_shared(name)

    def test_read_shared_with_large_payload(self):
        """
        GIVEN a 10k-entry dictionary
        WHEN write_shared + read_shared round-trip
        THEN the data is recovered intact
        """
        data = {f"k{i}": f"v{i}" for i in range(10_000)}
        name, size = write_shared(data)
        try:
            result = read_shared(name, size)
            assert result == data
        finally:
            cleanup_shared(name)

    def test_cleanup_shared_unlinks_block(self):
        """
        GIVEN a shared memory block
        WHEN cleanup_shared is called
        THEN the block is no longer accessible
        """
        name, _ = write_shared({"x": 1})
        cleanup_shared(name)
        with pytest.raises(FileNotFoundError):
            SharedMemory(name=name, create=False)

    def test_concurrent_reads_from_same_block(self):
        """
        GIVEN a shared memory block
        WHEN read_shared is called twice
        THEN both calls return the correct data
        """
        data = {"a": 1, "b": 2}
        name, size = write_shared(data)
        try:
            r1 = read_shared(name, size)
            r2 = read_shared(name, size)
            assert r1 == data
            assert r2 == data
        finally:
            cleanup_shared(name)


# ---------------------------------------------------------------------------
# _clear_dcc_data_async (concurrent collection clearing)
# ---------------------------------------------------------------------------


class TestClearDccDataAsync:
    @pytest.mark.asyncio
    async def test_clear_dcc_data_async_with_multiple_collections(self, mock_db):
        """
        GIVEN multiple collections with records matching the submission
        WHEN _clear_dcc_data_async is called
        THEN all matching records across all collections are deleted
        """
        mock_db.file.docs = [
            {"submission": "4dn", "name": "a"},
            {"submission": "hubmap", "name": "b"},
        ]
        mock_db.collection.docs = [
            {"submission": "4dn", "name": "c"},
        ]

        await _clear_dcc_data_async("4dn")

        assert len(mock_db.file.docs) == 1
        assert mock_db.file.docs[0]["submission"] == "hubmap"
        assert len(mock_db.collection.docs) == 0

    @pytest.mark.asyncio
    async def test_clear_dcc_data_async_preserves_other_dcc_data(self, mock_db):
        """
        GIVEN collections with records from multiple DCCs
        WHEN _clear_dcc_data_async is called for one DCC
        THEN only that DCC's records are removed
        """
        mock_db.file.docs = [
            {"submission": "4dn", "name": "a"},
            {"submission": "hubmap", "name": "b"},
            {"submission": "encode", "name": "c"},
        ]

        await _clear_dcc_data_async("4dn")

        remaining = {d["submission"] for d in mock_db.file.docs}
        assert remaining == {"hubmap", "encode"}


# ---------------------------------------------------------------------------
# _load_file
# ---------------------------------------------------------------------------


class TestLoadFile:
    @pytest.mark.asyncio
    async def test_load_file_with_csv(self, tmp_path, no_dispatch, worker_db):
        """
        GIVEN a 3-row CSV file
        WHEN _load_file is called
        THEN records are inserted with submission and table fields
        """
        csv_file = tmp_path / "sample.csv"
        csv_file.write_text("col_a,col_b\n1,2\n3,4\n5,6\n")

        table, count = await _load_file(str(csv_file), "test_dcc")

        assert table == "sample"
        assert count == 3
        assert len(worker_db.sample.docs) == 3
        assert worker_db.sample.docs[0]["submission"] == "test_dcc"
        assert worker_db.sample.docs[0]["table"] == "sample"

    @pytest.mark.asyncio
    async def test_load_file_with_tsv(self, tmp_path, no_dispatch, worker_db):
        """
        GIVEN a TSV file
        WHEN _load_file is called
        THEN the tab delimiter is used correctly
        """
        tsv_file = tmp_path / "data.tsv"
        tsv_file.write_text("x\ty\na\tb\n")

        table, count = await _load_file(str(tsv_file), "dcc")

        assert table == "data"
        assert count == 1
        assert worker_db.data.docs[0]["x"] == "a"
        assert worker_db.data.docs[0]["y"] == "b"

    @pytest.mark.asyncio
    async def test_load_file_marks_4dn_files_as_public(self, tmp_path, no_dispatch, worker_db):
        """
        GIVEN a CSV named "file.csv" with submission "4dn"
        WHEN _load_file is called
        THEN records get data_access_level set to "public"
        """
        csv_file = tmp_path / "file.csv"
        csv_file.write_text("id\n1\n")

        await _load_file(str(csv_file), "4dn")

        assert worker_db.file.docs[0]["data_access_level"] == "public"

    @pytest.mark.asyncio
    async def test_load_file_returns_table_and_count(self, tmp_path, no_dispatch, worker_db):
        """
        GIVEN a CSV file with 2 rows
        WHEN _load_file is called
        THEN it returns (table_name, row_count)
        """
        csv_file = tmp_path / "items.csv"
        csv_file.write_text("name\nalpha\nbeta\n")

        result = await _load_file(str(csv_file), "dcc")

        assert result == ("items", 2)


# ---------------------------------------------------------------------------
# _load_dataset_async
# ---------------------------------------------------------------------------


class TestLoadDatasetAsync:
    @pytest.mark.asyncio
    async def test_load_dataset_async_with_multiple_files(self, tmp_path, no_dispatch, worker_db):
        """
        GIVEN a directory with multiple CSV files
        WHEN _load_dataset_async is called
        THEN all files are loaded
        """
        (tmp_path / "a.csv").write_text("x\n1\n")
        (tmp_path / "b.csv").write_text("y\n2\n")

        await _load_dataset_async(tmp_path, "dcc")

        assert len(worker_db.a.docs) == 1
        assert len(worker_db.b.docs) == 1

    @pytest.mark.asyncio
    async def test_load_dataset_async_with_nested_directory(self, tmp_path, no_dispatch, worker_db):
        """
        GIVEN a directory with CSV files in a subdirectory
        WHEN _load_dataset_async is called
        THEN files from the subdirectory are found and loaded
        """
        subdir = tmp_path / "nested"
        subdir.mkdir()
        (subdir / "c.csv").write_text("z\n3\n")

        await _load_dataset_async(tmp_path, "dcc")

        assert len(worker_db.c.docs) == 1


# ---------------------------------------------------------------------------
# _enrich_4dn_files_batch
# ---------------------------------------------------------------------------


class TestEnrich4dnFilesBatch:
    @pytest.mark.asyncio
    async def test_matching_accessions(self, no_dispatch, worker_db):
        """
        GIVEN a batch with a doc_id/accession pair matching file_metadata
        WHEN _enrich_4dn_files_batch is called
        THEN the matching fields are written to the worker DB
        """
        worker_db.files.docs = [
            {"_id": "id1", "persistent_id": "4DNFI000001"}
        ]
        shared_data = {
            "file_metadata": {
                "4DNFI000001": {
                    "genome_assembly": "GRCh38",
                    "file_type": "bam",
                }
            },
            "biosource_tiers": {},
        }
        shm_name, shm_size = write_shared(shared_data)
        try:
            result = await _enrich_4dn_files_batch(
                [("id1", "4DNFI000001")], shm_name, shm_size
            )
            assert result == 1
            assert worker_db.files.docs[0]["genome_assembly"] == "GRCh38"
            assert worker_db.files.docs[0]["output_type"] == "bam"
        finally:
            cleanup_shared(shm_name)

    @pytest.mark.asyncio
    async def test_biosource_tier_derivation(self, no_dispatch, worker_db):
        """
        GIVEN file_metadata with a biosource_name that exists in biosource_tiers
        WHEN _enrich_4dn_files_batch is called
        THEN cell_line_tier is set under extra.fourdn
        """
        worker_db.files.docs = [{"_id": "id1"}]
        shared_data = {
            "file_metadata": {
                "ACC1": {"biosource_name": "H1-hESC"}
            },
            "biosource_tiers": {"H1-hESC": "Tier 1"},
        }
        shm_name, shm_size = write_shared(shared_data)
        try:
            result = await _enrich_4dn_files_batch(
                [("id1", "ACC1")], shm_name, shm_size
            )
            assert result == 1
            assert worker_db.files.docs[0]["extra.fourdn.cell_line_tier"] == "Tier 1"
            assert worker_db.files.docs[0]["extra.fourdn.biosource_name"] == "H1-hESC"
        finally:
            cleanup_shared(shm_name)

    @pytest.mark.asyncio
    async def test_no_match_returns_zero(self, no_dispatch, worker_db):
        """
        GIVEN a batch with no matching accessions in file_metadata
        WHEN _enrich_4dn_files_batch is called
        THEN it returns 0
        """
        shared_data = {
            "file_metadata": {},
            "biosource_tiers": {},
        }
        shm_name, shm_size = write_shared(shared_data)
        try:
            result = await _enrich_4dn_files_batch(
                [("id1", "UNKNOWN")], shm_name, shm_size
            )
            assert result == 0
        finally:
            cleanup_shared(shm_name)


# ---------------------------------------------------------------------------
# _enrich_4dn_collections_batch
# ---------------------------------------------------------------------------


class TestEnrich4dnCollectionsBatch:
    @pytest.mark.asyncio
    async def test_lab_and_experiment_type_promotion(self, no_dispatch, worker_db):
        """
        GIVEN experiment metadata with lab and experiment_type
        WHEN _enrich_4dn_collections_batch is called
        THEN those fields are promoted to collection top-level
        """
        worker_db.collection.docs = [{"_id": "c1"}]
        experiment_metadata = {
            "4DNES000001": {
                "lab": "Some Lab",
                "experiment_type": "Hi-C",
                "status": "released",
            }
        }
        shm_name, shm_size = write_shared(experiment_metadata)
        try:
            result = await _enrich_4dn_collections_batch(
                [("c1", "4DNES000001")], shm_name, shm_size
            )
            assert result == 1
            doc = worker_db.collection.docs[0]
            assert doc["lab"] == "Some Lab"
            assert doc["experiment_type"] == "Hi-C"
            assert doc["extra.fourdn"] == {"status": "released"}
        finally:
            cleanup_shared(shm_name)


# ---------------------------------------------------------------------------
# _enrich_hubmap_collections_batch
# ---------------------------------------------------------------------------


class TestEnrichHubmapCollectionsBatch:
    @pytest.mark.asyncio
    async def test_experiment_type_and_analyte_class(self, no_dispatch, worker_db):
        """
        GIVEN dataset metadata with dataset_type and analyte_class
        WHEN _enrich_hubmap_collections_batch is called
        THEN experiment_type and analyte_class are set, extra.hubmap holds rest
        """
        worker_db.collection.docs = [{"_id": "c1"}]
        dataset_metadata = {
            "doi:123": {
                "dataset_type": "RNAseq",
                "analyte_class": "RNA",
                "pipeline": "salmon",
            }
        }
        shm_name, shm_size = write_shared(dataset_metadata)
        try:
            result = await _enrich_hubmap_collections_batch(
                [("c1", "doi:123")], shm_name, shm_size
            )
            assert result == 1
            doc = worker_db.collection.docs[0]
            assert doc["experiment_type"] == "RNAseq"
            assert doc["analyte_class"] == "RNA"
            assert doc["extra.hubmap"] == {"pipeline": "salmon"}
        finally:
            cleanup_shared(shm_name)


# ---------------------------------------------------------------------------
# _enrich_hubmap_subjects_batch
# ---------------------------------------------------------------------------


class TestEnrichHubmapSubjectsBatch:
    @pytest.mark.asyncio
    async def test_donor_matching_and_list_unwrapping(self, no_dispatch, worker_db):
        """
        GIVEN a subject whose local_id contains a donor UUID,
              and donor metadata with single-element list fields
        WHEN _enrich_hubmap_subjects_batch is called
        THEN demographics are set with lists unwrapped
        """
        worker_db.subject.docs = [{"_id": "s1"}]
        donor_lookup = {
            "uuid-abc": {
                "age_value": [55],
                "age_unit": ["years"],
                "sex": ["Male"],
                "race": ["White"],
            }
        }
        shm_name, shm_size = write_shared(donor_lookup)
        try:
            result = await _enrich_hubmap_subjects_batch(
                [("s1", "prefix-uuid-abc-suffix")], shm_name, shm_size
            )
            assert result == 1
            doc = worker_db.subject.docs[0]
            assert doc["extra.hubmap"]["age_value"] == 55
            assert doc["extra.hubmap"]["sex"] == "Male"
        finally:
            cleanup_shared(shm_name)


# ---------------------------------------------------------------------------
# _transform_encode_batch
# ---------------------------------------------------------------------------


class TestTransformEncodeBatch:
    @pytest.mark.asyncio
    async def test_transform_and_insert(self, no_dispatch, worker_db, mocker):
        """
        GIVEN a list of ENCODE rows
        WHEN _transform_encode_batch is called
        THEN transformed docs are inserted, None rows are skipped
        """
        mocker.patch(
            "cfdb.services.encode.transform_to_c2m2",
            side_effect=lambda r: {"file": r["accession"]} if r.get("accession") else None,
        )
        rows = [
            {"accession": "ENCFF001"},
            {"accession": "ENCFF002"},
            {},  # will return None
        ]
        result = await _transform_encode_batch(rows)

        assert result == 2
        assert len(worker_db.files.docs) == 2


# ---------------------------------------------------------------------------
# _prune_non_public_hubmap_raw_records
# ---------------------------------------------------------------------------


class TestPruneNonPublicHubmapRawRecords:
    @pytest.mark.asyncio
    async def test_mixed_public_and_non_public(self, mock_db):
        """
        GIVEN a dataset_metadata dict with one public and one non-public dataset,
              and raw collection/file_in_collection/file records for both
        WHEN _prune_non_public_hubmap_raw_records is called
        THEN only the non-public records are deleted
        """
        dataset_metadata = {
            "https://doi.org/public": {"data_access_level": "public"},
            "https://doi.org/consortium": {"data_access_level": "consortium"},
        }

        mock_db.collection.docs = [
            {"submission": "hubmap", "persistent_id": "https://doi.org/public", "id_namespace": "ns", "local_id": "pub-coll"},
            {"submission": "hubmap", "persistent_id": "https://doi.org/consortium", "id_namespace": "ns", "local_id": "con-coll"},
        ]
        mock_db.file_in_collection.docs = [
            {"collection_id_namespace": "ns", "collection_local_id": "pub-coll", "file_id_namespace": "ns", "file_local_id": "file-pub"},
            {"collection_id_namespace": "ns", "collection_local_id": "con-coll", "file_id_namespace": "ns", "file_local_id": "file-con"},
        ]
        mock_db.file.docs = [
            {"id_namespace": "ns", "local_id": "file-pub"},
            {"id_namespace": "ns", "local_id": "file-con"},
        ]

        await _prune_non_public_hubmap_raw_records(dataset_metadata)

        assert len(mock_db.file_in_collection.docs) == 1
        assert mock_db.file_in_collection.docs[0]["collection_local_id"] == "pub-coll"
        assert len(mock_db.file.docs) == 1
        assert mock_db.file.docs[0]["local_id"] == "file-pub"

    @pytest.mark.asyncio
    async def test_all_public(self, mock_db):
        """
        GIVEN all datasets are public
        WHEN _prune_non_public_hubmap_raw_records is called
        THEN no records are deleted
        """
        dataset_metadata = {
            "https://doi.org/a": {"data_access_level": "public"},
            "https://doi.org/b": {"data_access_level": "public"},
        }

        mock_db.file_in_collection.docs = [
            {"collection_id_namespace": "ns", "collection_local_id": "c1", "file_id_namespace": "ns", "file_local_id": "f1"},
        ]
        mock_db.file.docs = [
            {"id_namespace": "ns", "local_id": "f1"},
        ]

        await _prune_non_public_hubmap_raw_records(dataset_metadata)

        assert len(mock_db.file_in_collection.docs) == 1
        assert len(mock_db.file.docs) == 1

    @pytest.mark.asyncio
    async def test_all_non_public(self, mock_db):
        """
        GIVEN all datasets are non-public
        WHEN _prune_non_public_hubmap_raw_records is called
        THEN all matching records are deleted
        """
        dataset_metadata = {
            "https://doi.org/a": {"data_access_level": "consortium"},
            "https://doi.org/b": {"data_access_level": "protected"},
        }

        mock_db.collection.docs = [
            {"submission": "hubmap", "persistent_id": "https://doi.org/a", "id_namespace": "ns", "local_id": "c1"},
            {"submission": "hubmap", "persistent_id": "https://doi.org/b", "id_namespace": "ns", "local_id": "c2"},
        ]
        mock_db.file_in_collection.docs = [
            {"collection_id_namespace": "ns", "collection_local_id": "c1", "file_id_namespace": "ns", "file_local_id": "f1"},
            {"collection_id_namespace": "ns", "collection_local_id": "c2", "file_id_namespace": "ns", "file_local_id": "f2"},
        ]
        mock_db.file.docs = [
            {"id_namespace": "ns", "local_id": "f1"},
            {"id_namespace": "ns", "local_id": "f2"},
        ]

        await _prune_non_public_hubmap_raw_records(dataset_metadata)

        assert len(mock_db.file_in_collection.docs) == 0
        assert len(mock_db.file.docs) == 0

    @pytest.mark.asyncio
    async def test_file_linked_to_both_public_and_non_public(self, mock_db):
        """
        GIVEN a file is linked to both a public and a non-public collection
        WHEN _prune_non_public_hubmap_raw_records is called
        THEN the non-public link is removed but the file is preserved
        """
        dataset_metadata = {
            "https://doi.org/public": {"data_access_level": "public"},
            "https://doi.org/consortium": {"data_access_level": "consortium"},
        }

        mock_db.collection.docs = [
            {"submission": "hubmap", "persistent_id": "https://doi.org/public", "id_namespace": "ns", "local_id": "pub-coll"},
            {"submission": "hubmap", "persistent_id": "https://doi.org/consortium", "id_namespace": "ns", "local_id": "con-coll"},
        ]
        mock_db.file_in_collection.docs = [
            {"collection_id_namespace": "ns", "collection_local_id": "pub-coll", "file_id_namespace": "ns", "file_local_id": "shared-file"},
            {"collection_id_namespace": "ns", "collection_local_id": "con-coll", "file_id_namespace": "ns", "file_local_id": "shared-file"},
        ]
        mock_db.file.docs = [
            {"id_namespace": "ns", "local_id": "shared-file"},
        ]

        await _prune_non_public_hubmap_raw_records(dataset_metadata)

        assert len(mock_db.file_in_collection.docs) == 1
        assert mock_db.file_in_collection.docs[0]["collection_local_id"] == "pub-coll"
        assert len(mock_db.file.docs) == 1

    @pytest.mark.asyncio
    async def test_missing_access_level_treated_as_non_public(self, mock_db):
        """
        GIVEN a dataset with no data_access_level key
        WHEN _prune_non_public_hubmap_raw_records is called
        THEN it is treated as non-public and its records are pruned
        """
        dataset_metadata = {
            "https://doi.org/unknown": {"dataset_type": "RNAseq"},
        }

        mock_db.collection.docs = [
            {"submission": "hubmap", "persistent_id": "https://doi.org/unknown", "id_namespace": "ns", "local_id": "c1"},
        ]
        mock_db.file_in_collection.docs = [
            {"collection_id_namespace": "ns", "collection_local_id": "c1", "file_id_namespace": "ns", "file_local_id": "f1"},
        ]
        mock_db.file.docs = [
            {"id_namespace": "ns", "local_id": "f1"},
        ]

        await _prune_non_public_hubmap_raw_records(dataset_metadata)

        assert len(mock_db.file_in_collection.docs) == 0
        assert len(mock_db.file.docs) == 0


# ---------------------------------------------------------------------------
# _enrich_hubmap_collections_and_subjects (integration)
# ---------------------------------------------------------------------------


class TestEnrichHubmapCollectionsAndSubjects:
    @pytest.mark.asyncio
    async def test_returns_dataset_metadata(self, mock_db, mocker, no_dispatch, worker_db):
        """
        GIVEN a mocked fetch_dataset_metadata_bulk returning sample data
        WHEN _enrich_hubmap_collections_and_subjects is called
        THEN it returns the dataset_metadata dict
        """
        sample = {
            "https://doi.org/10.35079/HBM123": {
                "data_access_level": "public",
                "dataset_type": "RNAseq",
            }
        }
        mocker.patch(
            "cfdb.services.hubmap.fetch_dataset_metadata_bulk",
            return_value=sample,
        )

        mock_db.collection.docs = []
        mock_db.subject.docs = []

        result = await _enrich_hubmap_collections_and_subjects()

        assert result is sample


# ---------------------------------------------------------------------------
# _enrich_hubmap_files
# ---------------------------------------------------------------------------


class TestEnrichHubmapFiles:
    @pytest.mark.asyncio
    async def test_sets_public_access_level_on_all_hubmap_files(self, mock_db):
        """
        GIVEN materialized HuBMAP files in the files collection
        WHEN _enrich_hubmap_files is called
        THEN all HuBMAP files get data_access_level set to "public"
        """
        mock_db.files.docs = [
            {"_id": "1", "submission": "hubmap", "filename": "a.bam", "collections": []},
            {"_id": "2", "submission": "hubmap", "filename": "b.bam", "collections": []},
            {"_id": "3", "submission": "4dn", "filename": "c.bam", "collections": []},
        ]
        mock_db.collection.docs = []

        await _enrich_hubmap_files({})

        for doc in mock_db.files.docs:
            if doc["submission"] == "hubmap":
                assert doc["data_access_level"] == "public"

        non_hubmap = [d for d in mock_db.files.docs if d["submission"] == "4dn"]
        assert "data_access_level" not in non_hubmap[0]


# ---------------------------------------------------------------------------
# _sync_dccs (DCC-level concurrency)
# ---------------------------------------------------------------------------


class TestSyncDccs:
    @pytest.mark.asyncio
    async def test_sync_dccs_with_multiple_dccs(self, mock_db, mocker):
        """
        GIVEN a task with multiple DCCs
        WHEN _sync_dccs is called
        THEN all DCCs are synced concurrently
        """
        from cfdb.services.sync import SyncTask, _sync_dccs

        task = SyncTask(id="t1", dcc_names=["4dn", "hubmap"])

        c2m2_calls = []

        async def fake_c2m2(task, data_path, downloads_path, dcc):
            c2m2_calls.append(dcc)

        mocker.patch("cfdb.services.sync._sync_c2m2_zip", side_effect=fake_c2m2)
        mocker.patch("cfdb.services.sync.get_dcc_type", return_value="c2m2")

        await _sync_dccs(task)

        assert set(c2m2_calls) == {"4dn", "hubmap"}
        assert task.progress == "All DCCs synced successfully"

    @pytest.mark.asyncio
    async def test_sync_dccs_with_failing_dcc(self, mock_db, mocker):
        """
        GIVEN a task where one DCC fails
        WHEN _sync_dccs is called
        THEN an ExceptionGroup is raised
        """
        from cfdb.services.sync import SyncTask, _sync_dccs

        task = SyncTask(id="t1", dcc_names=["4dn", "hubmap"])

        async def fail_c2m2(task, data_path, downloads_path, dcc):
            if dcc == "hubmap":
                raise RuntimeError("hubmap failed")

        mocker.patch("cfdb.services.sync._sync_c2m2_zip", side_effect=fail_c2m2)
        mocker.patch("cfdb.services.sync.get_dcc_type", return_value="c2m2")

        with pytest.raises(ExceptionGroup):
            await _sync_dccs(task)


# ---------------------------------------------------------------------------
# WorkerPool lifecycle
# ---------------------------------------------------------------------------


class TestWorkerPoolLifecycle:
    @pytest.mark.asyncio
    async def test_run_sync_enters_worker_pool(self, mocker):
        """
        GIVEN a SyncTask
        WHEN _run_sync is called
        THEN wool.WorkerPool is used as an async context manager
        """
        from cfdb.services.sync import SyncTask, _run_sync

        mock_pool = MagicMock()
        mock_pool.__aenter__ = AsyncMock(return_value=mock_pool)
        mock_pool.__aexit__ = AsyncMock(return_value=False)
        mocker.patch("cfdb.services.sync.wool.WorkerPool", return_value=mock_pool)
        mocker.patch("cfdb.services.sync._sync_dccs", new_callable=AsyncMock)
        mocker.patch("cfdb.services.sync.locks.release_sync_lock", new_callable=AsyncMock)

        task = SyncTask(id="t1", dcc_names=[])
        await _run_sync(task)

        mock_pool.__aenter__.assert_awaited_once()
        mock_pool.__aexit__.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_run_sync_cleans_up_on_failure(self, mocker):
        """
        GIVEN _sync_dccs raises an exception
        WHEN _run_sync is called
        THEN the WorkerPool exits cleanly and the lock is released
        """
        from cfdb.services.sync import SyncTask, TaskStatus, _run_sync

        mock_pool = MagicMock()
        mock_pool.__aenter__ = AsyncMock(return_value=mock_pool)
        mock_pool.__aexit__ = AsyncMock(return_value=False)
        mocker.patch("cfdb.services.sync.wool.WorkerPool", return_value=mock_pool)
        mocker.patch(
            "cfdb.services.sync._sync_dccs",
            new_callable=AsyncMock,
            side_effect=RuntimeError("boom"),
        )
        release_mock = mocker.patch(
            "cfdb.services.sync.locks.release_sync_lock", new_callable=AsyncMock
        )

        task = SyncTask(id="t1", dcc_names=[])
        await _run_sync(task)

        assert task.status == TaskStatus.FAILED
        mock_pool.__aexit__.assert_awaited_once()
        release_mock.assert_awaited_once_with("t1")
