"""Tests for sync service pruning and enrichment changes."""

from __future__ import annotations

import logging

import pytest

from cfdb.services import encode as encode_module
from cfdb.services import sync as sync_module
from cfdb.services.sync import (
    SyncTask,
    _enrich_hubmap_collections_and_subjects,
    _enrich_hubmap_files,
    _load_dataset_async,
    _prune_non_public_hubmap_raw_records,
    _sync_dccs,
    _sync_encode,
)
from cfdb.indexes import data_index_specs


def _encode_metadata_row(accession: str, filename: str) -> dict:
    """Return a minimal ENCODE metadata TSV row for the given download name."""
    return {
        "File accession": accession,
        "File format": "bed",
        "File download URL": (
            f"https://www.encodeproject.org/files/{accession}/@@download/{filename}"
        ),
    }


async def _async_iter(rows):
    """Yield the given rows, standing in for the streaming metadata fetch."""
    for row in rows:
        yield row


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


# ---------------------------------------------------------------------------
# _load_dataset_async
# ---------------------------------------------------------------------------


class TestLoadDatasetAsync:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("submission", ["4dn", "hubmap"])
    async def test__load_dataset_async_should_store_compression_format_verbatim(
        self, mock_db, tmp_path, submission
    ):
        """Test that the upstream compression_format column is copied unchanged.

        Given:
            A C2M2 file table whose compression_format values deliberately
            contradict what the filenames imply — an uncompressed value on a
            gzipped name, and a gzip term on a plain name.
        When:
            _load_dataset_async loads it for a C2M2-sourced DCC.
        Then:
            It should store the column's values, not the filename-implied
            ones.
        """
        # Arrange
        table = tmp_path / "file.tsv"
        table.write_text(
            "local_id\tfilename\tcompression_format\n"
            "a\tx.bed.gz\t\n"
            "b\ty.bed\tformat:3989\n"
            "c\tz.bed.gz\tformat:3989\n"
        )

        # Act
        await _load_dataset_async(tmp_path, submission)

        # Assert
        loaded = {doc["local_id"]: doc["compression_format"] for doc in mock_db.file.docs}
        assert loaded == {"a": "", "b": "format:3989", "c": "format:3989"}

    @pytest.mark.asyncio
    async def test__load_dataset_async_should_not_add_compression_format_when_absent(
        self, mock_db, tmp_path
    ):
        """Test that the loader never synthesizes the column.

        Given:
            A C2M2 file table with no compression_format column at all.
        When:
            _load_dataset_async loads it for a C2M2-sourced DCC.
        Then:
            The stored records should carry no compression_format key.
        """
        # Arrange
        table = tmp_path / "file.tsv"
        table.write_text("local_id\tfilename\na\tx.bed.gz\n")

        # Act
        await _load_dataset_async(tmp_path, "4dn")

        # Assert
        assert all("compression_format" not in doc for doc in mock_db.file.docs)


# ---------------------------------------------------------------------------
# _sync_encode
# ---------------------------------------------------------------------------


class TestSyncEncode:
    @pytest.mark.asyncio
    async def test__sync_encode_should_populate_compression_format_on_inserted_docs(
        self, mock_db, mocker
    ):
        """Test that the derived value reaches the documents the sync inserts.

        Given:
            Three ENCODE metadata rows published as gzip, bigWig and starch,
            the three outcomes the released corpus actually produces.
        When:
            _sync_encode runs the fetch-transform-insert pipeline.
        Then:
            Each inserted document should carry the compression term its
            filename implies, and the starch document should carry no
            compression_format key at all.
        """
        # Arrange
        rows = [
            _encode_metadata_row("ENCFF001AAA", "ENCFF001AAA.bed.gz"),
            _encode_metadata_row("ENCFF002BBB", "ENCFF002BBB.bigWig"),
            _encode_metadata_row("ENCFF003CCC", "ENCFF003CCC.bed.starch"),
        ]
        mocker.patch.object(
            encode_module, "fetch_encode_metadata", lambda: _async_iter(rows)
        )

        # Act
        await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        derived = {
            doc["local_id"]: doc.get("compression_format", "<absent>")
            for doc in mock_db.files.docs
        }
        assert derived == {
            "ENCFF001AAA": "format:3989",
            "ENCFF002BBB": "",
            "ENCFF003CCC": "<absent>",
        }

    @pytest.mark.asyncio
    async def test__sync_encode_should_log_the_compression_format_distribution(
        self, mock_db, mocker, caplog
    ):
        """Test that the sync reports how the derived values came out.

        Given:
            ENCODE metadata rows covering gzip, no compression and an
            undetermined compression.
        When:
            _sync_encode runs.
        Then:
            It should log a tally of the derived terms, so a corpus-wide flip
            in ENCODE's URL shape is visible rather than hidden behind an
            unchanged row count.
        """
        # Arrange
        rows = [
            _encode_metadata_row("ENCFF001AAA", "ENCFF001AAA.bed.gz"),
            _encode_metadata_row("ENCFF002BBB", "ENCFF002BBB.bigWig"),
            _encode_metadata_row("ENCFF003CCC", "ENCFF003CCC.bed.starch"),
        ]
        mocker.patch.object(
            encode_module, "fetch_encode_metadata", lambda: _async_iter(rows)
        )

        # Act
        with caplog.at_level(logging.INFO, logger="cfdb.services.sync"):
            await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        distribution = next(
            record.getMessage()
            for record in caplog.records
            if "compression_format distribution" in record.getMessage()
        )
        assert "'format:3989': 1" in distribution
        assert "'uncompressed': 1" in distribution
        assert "'undetermined': 1" in distribution

    @pytest.mark.asyncio
    async def test__sync_encode_should_leave_other_dcc_documents_unchanged(
        self, mock_db, mocker
    ):
        """Test that the ENCODE derivation does not touch the other DCCs.

        Given:
            Materialized 4DN and HuBMAP file documents whose
            compression_format values contradict their filenames, alongside
            one ENCODE row to sync.
        When:
            _sync_encode runs.
        Then:
            The 4DN and HuBMAP documents should be byte-identical afterwards.
        """
        # Arrange
        others = [
            {"submission": "4dn", "filename": "a.pairs.gz", "compression_format": ""},
            {
                "submission": "hubmap",
                "filename": "b.bed",
                "compression_format": "format:3989",
            },
        ]
        mock_db.files.docs = [dict(doc) for doc in others]
        mocker.patch.object(
            encode_module,
            "fetch_encode_metadata",
            lambda: _async_iter([_encode_metadata_row("ENCFF001AAA", "x.bed.gz")]),
        )

        # Act
        await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        survivors = [d for d in mock_db.files.docs if d["submission"] != "encode"]
        assert survivors == others
