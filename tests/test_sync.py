"""Tests for sync service pruning and enrichment changes."""

from __future__ import annotations

import pytest

from cfdb.services import sync as sync_module
from cfdb.services.sync import (
    SyncTask,
    _enrich_hubmap_collections_and_subjects,
    _enrich_hubmap_files,
    _prune_non_public_hubmap_raw_records,
    _sync_dccs,
)
from cfdb.indexes import data_index_specs


class TestSyncDccsDataIndexing:
    @pytest.mark.asyncio
    async def test__sync_dccs_should_ensure_data_indexes_after_load(
        self, mocker, mock_db, tmp_path
    ):
        """Test that a sync ensures the data indexes after loading.

        Given:
            A sync of one DCC with the per-DCC load and the applier mocked.
        When:
            ``_sync_dccs`` runs.
        Then:
            It should ensure ``data_index_specs()`` once, after the load
            loop.
        """
        # Arrange
        mocker.patch.object(sync_module, "DATA_DIR", str(tmp_path))
        mocker.patch.object(sync_module, "get_dcc_type", return_value="rest_api")
        mocker.patch.object(sync_module, "_sync_encode", mocker.AsyncMock())
        ensure_mock = mocker.patch.object(
            sync_module, "ensure_indexes", mocker.AsyncMock()
        )
        task = SyncTask(id="t1", dcc_names=["encode"])

        # Act
        await _sync_dccs(task)

        # Assert
        ensure_mock.assert_awaited_once()
        specs = ensure_mock.await_args.args[1]
        assert [s.name for s in specs] == [s.name for s in data_index_specs()]

    @pytest.mark.asyncio
    async def test__sync_dccs_should_skip_indexing_when_no_dccs(
        self, mocker, mock_db, tmp_path
    ):
        """Test that an empty sync does not build data indexes.

        Given:
            A sync task with no DCC names.
        When:
            ``_sync_dccs`` runs.
        Then:
            It should not call the index applier (nothing was loaded).
        """
        # Arrange
        mocker.patch.object(sync_module, "DATA_DIR", str(tmp_path))
        ensure_mock = mocker.patch.object(
            sync_module, "ensure_indexes", mocker.AsyncMock()
        )
        task = SyncTask(id="t2", dcc_names=[])

        # Act
        await _sync_dccs(task)

        # Assert
        ensure_mock.assert_not_awaited()


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

        # Public file_in_collection link survives
        assert len(mock_db.file_in_collection.docs) == 1
        assert mock_db.file_in_collection.docs[0]["collection_local_id"] == "pub-coll"

        # Orphaned non-public file is deleted; public file survives
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
        # shared-file belongs to both collections
        mock_db.file_in_collection.docs = [
            {"collection_id_namespace": "ns", "collection_local_id": "pub-coll", "file_id_namespace": "ns", "file_local_id": "shared-file"},
            {"collection_id_namespace": "ns", "collection_local_id": "con-coll", "file_id_namespace": "ns", "file_local_id": "shared-file"},
        ]
        mock_db.file.docs = [
            {"id_namespace": "ns", "local_id": "shared-file"},
        ]

        await _prune_non_public_hubmap_raw_records(dataset_metadata)

        # Non-public link deleted, public link survives
        assert len(mock_db.file_in_collection.docs) == 1
        assert mock_db.file_in_collection.docs[0]["collection_local_id"] == "pub-coll"

        # File is preserved because it still has a public link
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
# _enrich_hubmap_collections_and_subjects
# ---------------------------------------------------------------------------


class TestEnrichHubmapCollectionsAndSubjects:
    @pytest.mark.asyncio
    async def test_returns_dataset_metadata(self, mock_db, mocker):
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

        # Need at least an empty collection cursor to iterate
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

        # HuBMAP files stamped as public
        for doc in mock_db.files.docs:
            if doc["submission"] == "hubmap":
                assert doc["data_access_level"] == "public"

        # Non-HuBMAP files untouched
        non_hubmap = [d for d in mock_db.files.docs if d["submission"] == "4dn"]
        assert "data_access_level" not in non_hubmap[0]
