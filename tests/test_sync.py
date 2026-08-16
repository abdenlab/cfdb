"""Tests for sync service pruning and enrichment changes."""

from __future__ import annotations

import asyncio
import logging

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from cfdb import api
from cfdb.services import encode as encode_module
from cfdb.services import fourdn as fourdn_module
from cfdb.services import sync as sync_module
from cfdb.services.sync import (
    SyncTask,
    _enrich_4dn_api_metadata,
    _enrich_4dn_collections,
    _enrich_hubmap_collections_and_subjects,
    _enrich_hubmap_files,
    _load_dataset_async,
    _prune_non_public_hubmap_raw_records,
    _set_accession_ids,
    _stamp_4dn_file_accessions,
    _sync_dccs,
    _sync_encode,
)
from cfdb.indexes import data_index_specs
from tests.conftest import FakeDB


class _EncodeSyncTestBase:
    """Base for the classes that drive ``_sync_encode``.

    ``_sync_encode`` fans out over ``annotation_types_from_env()``, whose
    default allowlist is non-empty, so a test that mocks only the experiment
    fetch would reach the real ENCODE portal once per annotation type and
    stream tens of thousands of live rows into its assertions. The autouse
    fixture below turns the annotation phases off by default.

    Scoped to these classes rather than the module: nothing in the 4DN or
    HuBMAP tests reads the variable, and a module-wide autouse fixture that
    silently governs a network boundary is worth keeping close to the tests
    that depend on it.

    To opt back in, either set ``ENCODE_ANNOTATION_TYPES`` to the types
    under test or ``monkeypatch.delenv`` it to exercise the default
    allowlist -- the fixture is function-scoped, so a test's own
    monkeypatching wins.
    """

    @pytest.fixture(autouse=True)
    def no_annotation_phases(self, monkeypatch):
        """Turn the annotation phases off unless a test asks for them."""
        monkeypatch.setenv("ENCODE_ANNOTATION_TYPES", "")


def _encode_metadata_row(accession: str, filename: str) -> dict:
    """Return a minimal ENCODE metadata TSV row for the given download name."""
    return {
        "File accession": accession,
        "File format": "bed",
        "File download URL": (
            f"https://www.encodeproject.org/files/{accession}/@@download/{filename}"
        ),
    }


def _encode_annotation_row(accession: str, dataset: str, filename: str) -> dict:
    """Return a minimal ENCODE annotation TSV row, using annotation column names."""
    return {
        "File accession": accession,
        "File format": "bed",
        "Dataset accession": dataset,
        "Annotation type": "candidate Cis-Regulatory Elements",
        "Assembly": "GRCh38",
        "Organism": "Homo sapiens",
        "File download URL": (
            f"https://www.encodeproject.org/files/{accession}/@@download/{filename}"
        ),
    }


async def _async_iter(rows):
    """Yield the given rows, standing in for the streaming metadata fetch."""
    for row in rows:
        yield row


async def _async_raise(exc):
    """Stand in for a metadata stream that fails before yielding anything."""
    raise exc
    yield  # pragma: no cover - unreachable, marks this an async generator


async def _async_iter_then_raise(rows, exc):
    """Stand in for a stream that dies partway through.

    The production failure shape: the metadata fetch's timeout bounds the
    whole streamed body, so it fires with rows already delivered and
    inserted, not before the first one.
    """
    for row in rows:
        yield row
    raise exc


def _fail_insert_on_call(collection, failing_call: int, exc: Exception) -> None:
    """Make one of a fake collection's insert_many calls raise.

    Fails the *sink* rather than the stream, which is the half of the ingest
    the stream helpers above cannot reach. Keyed by call ordinal so a test can
    choose a batch boundary without inspecting the rows.
    """
    real = collection.insert_many
    calls = 0

    async def insert_many(docs):
        nonlocal calls
        calls += 1
        if calls == failing_call:
            raise exc
        return await real(docs)

    collection.insert_many = insert_many


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


class TestSyncDccsFailureIsolation:
    @pytest.mark.asyncio
    async def test__sync_dccs_should_sync_the_remaining_dccs_when_one_fails(
        self, mocker, mock_db, tmp_path
    ):
        """Test one DCC's failure does not cost the others their sync.

        get_all_dcc_names sorts, so ENCODE is attempted before HuBMAP on a
        whole-corpus sync. Letting the failure escape the loop meant one
        failed ENCODE phase -- three network streams by default, against a
        portal that 504s on slow requests -- skipped HuBMAP entirely.

        Given:
            A three-DCC sync whose first DCC raises.
        When:
            _sync_dccs runs.
        Then:
            It should still attempt both remaining DCCs before failing.
        """
        # Arrange
        mocker.patch.object(sync_module, "DATA_DIR", str(tmp_path))
        mocker.patch.object(sync_module, "get_dcc_type", return_value="rest_api")
        mocker.patch.object(
            sync_module,
            "_sync_encode",
            mocker.AsyncMock(side_effect=RuntimeError("encode died")),
        )
        zip_sync = mocker.patch.object(
            sync_module, "_sync_c2m2_zip", mocker.AsyncMock()
        )
        mocker.patch.object(sync_module, "ensure_indexes", mocker.AsyncMock())
        mocker.patch.object(
            sync_module, "_log_accession_coverage", mocker.AsyncMock()
        )
        task = SyncTask(id="t1", dcc_names=["encode", "4dn", "hubmap"])

        # Act & assert
        with pytest.raises(RuntimeError, match="encode"):
            await _sync_dccs(task)

        assert zip_sync.await_count == 2

    @pytest.mark.asyncio
    async def test__sync_dccs_should_ensure_data_indexes_when_a_dcc_failed(
        self, mocker, mock_db, tmp_path
    ):
        """Test a partial run still leaves what loaded queryable.

        The index build sat after the loop, so the first failure skipped it
        and left the DCCs that did load answering every query by collection
        scan.

        Given:
            A two-DCC sync whose first DCC raises.
        When:
            _sync_dccs runs.
        Then:
            It should still ensure the data indexes once.
        """
        # Arrange
        mocker.patch.object(sync_module, "DATA_DIR", str(tmp_path))
        mocker.patch.object(sync_module, "get_dcc_type", return_value="rest_api")
        mocker.patch.object(
            sync_module,
            "_sync_encode",
            mocker.AsyncMock(side_effect=RuntimeError("encode died")),
        )
        mocker.patch.object(sync_module, "_sync_c2m2_zip", mocker.AsyncMock())
        ensure_mock = mocker.patch.object(
            sync_module, "ensure_indexes", mocker.AsyncMock()
        )
        mocker.patch.object(
            sync_module, "_log_accession_coverage", mocker.AsyncMock()
        )
        task = SyncTask(id="t1", dcc_names=["encode", "hubmap"])

        # Act & assert
        with pytest.raises(RuntimeError):
            await _sync_dccs(task)

        ensure_mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test__sync_dccs_should_not_report_a_failed_dcc_as_synced(
        self, mocker, mock_db, tmp_path
    ):
        """Test the per-DCC success reporting skips the DCC that failed.

        Given:
            A two-DCC sync whose first DCC raises.
        When:
            _sync_dccs runs.
        Then:
            Only the DCC that succeeded should be logged as synced and have
            its accession coverage reported.
        """
        # Arrange
        mocker.patch.object(sync_module, "DATA_DIR", str(tmp_path))
        mocker.patch.object(sync_module, "get_dcc_type", return_value="rest_api")
        mocker.patch.object(
            sync_module,
            "_sync_encode",
            mocker.AsyncMock(side_effect=RuntimeError("encode died")),
        )
        mocker.patch.object(sync_module, "_sync_c2m2_zip", mocker.AsyncMock())
        mocker.patch.object(sync_module, "ensure_indexes", mocker.AsyncMock())
        coverage = mocker.patch.object(
            sync_module, "_log_accession_coverage", mocker.AsyncMock()
        )
        task = SyncTask(id="t1", dcc_names=["encode", "hubmap"])

        # Act & assert
        with pytest.raises(RuntimeError):
            await _sync_dccs(task)

        assert [call.args[0] for call in coverage.await_args_list] == ["hubmap"]
        assert task.progress == "Sync incomplete: 1 of 2 DCCs synced"

    @pytest.mark.asyncio
    async def test__sync_dccs_should_chain_the_first_dcc_failure_as_the_cause(
        self, mocker, mock_db, tmp_path
    ):
        """Test the raised error keeps the first failure's traceback.

        Given:
            A two-DCC sync where both DCCs raise distinguishable errors.
        When:
            _sync_dccs runs.
        Then:
            The error's cause should be the first failure's exception.
        """
        # Arrange
        first = RuntimeError("encode died")
        second = RuntimeError("hubmap died")
        mocker.patch.object(sync_module, "DATA_DIR", str(tmp_path))
        mocker.patch.object(sync_module, "get_dcc_type", return_value="rest_api")
        mocker.patch.object(
            sync_module, "_sync_encode", mocker.AsyncMock(side_effect=first)
        )
        mocker.patch.object(
            sync_module, "_sync_c2m2_zip", mocker.AsyncMock(side_effect=second)
        )
        mocker.patch.object(sync_module, "ensure_indexes", mocker.AsyncMock())
        mocker.patch.object(
            sync_module, "_log_accession_coverage", mocker.AsyncMock()
        )
        task = SyncTask(id="t1", dcc_names=["encode", "hubmap"])

        # Act & assert
        with pytest.raises(RuntimeError) as excinfo:
            await _sync_dccs(task)

        assert excinfo.value.__cause__ is first

    @pytest.mark.asyncio
    async def test__sync_dccs_should_propagate_a_cancellation(
        self, mocker, mock_db, tmp_path
    ):
        """Test cancellation cancels the run rather than failing one DCC.

        The per-DCC handler is deliberately broad, and a CancelledError
        caught by it would be recorded as a DCC failure and followed by
        every remaining DCC -- the opposite of cancelling.

        Given:
            A two-DCC sync whose first DCC raises CancelledError.
        When:
            _sync_dccs runs.
        Then:
            The CancelledError should propagate and the second DCC should
            never run.
        """
        # Arrange
        mocker.patch.object(sync_module, "DATA_DIR", str(tmp_path))
        mocker.patch.object(sync_module, "get_dcc_type", return_value="rest_api")
        mocker.patch.object(
            sync_module,
            "_sync_encode",
            mocker.AsyncMock(side_effect=asyncio.CancelledError()),
        )
        zip_sync = mocker.patch.object(
            sync_module, "_sync_c2m2_zip", mocker.AsyncMock()
        )
        mocker.patch.object(sync_module, "ensure_indexes", mocker.AsyncMock())
        task = SyncTask(id="t1", dcc_names=["encode", "hubmap"])

        # Act & assert
        with pytest.raises(asyncio.CancelledError):
            await _sync_dccs(task)

        zip_sync.assert_not_awaited()


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


class TestSyncEncode(_EncodeSyncTestBase):
    @pytest.mark.asyncio
    async def test__sync_encode_should_populate_accession_id_on_inserted_docs(
        self, mock_db, mocker
    ):
        """Test that the accession survives the path that reaches the database.

        ENCODE writes the files collection directly rather than through the
        materializer, so this pipeline is the only writer of both accession
        levels for that DCC.

        Given:
            An ENCODE row carrying a file accession, an experiment accession
            and a biosample term.
        When:
            _sync_encode runs the fetch-transform-insert pipeline.
        Then:
            The inserted document should carry the folded accession at both
            the file and the collection level.
        """
        # Arrange
        row = _encode_metadata_row("encff001aaa", "encff001aaa.bed.gz")
        row["Experiment accession"] = "encsr918zsj"
        row["Biosample term name"] = "K562"
        mocker.patch.object(
            encode_module, "fetch_encode_metadata", lambda deadline=None: _async_iter([row])
        )

        # Act
        await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        doc = next(d for d in mock_db.files.docs if d["submission"] == "encode")
        assert doc["accession_id"] == "ENCFF001AAA"
        assert doc["collections"][0]["accession_id"] == "ENCSR918ZSJ"

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
            encode_module, "fetch_encode_metadata", lambda deadline=None: _async_iter(rows)
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
            encode_module, "fetch_encode_metadata", lambda deadline=None: _async_iter(rows)
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
            lambda deadline=None: _async_iter([_encode_metadata_row("ENCFF001AAA", "x.bed.gz")]),
        )

        # Act
        await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        survivors = [d for d in mock_db.files.docs if d["submission"] != "encode"]
        assert survivors == others


class TestSyncEncodeAnnotationPhases(_EncodeSyncTestBase):
    @pytest.mark.asyncio
    async def test__sync_encode_should_ingest_annotations_alongside_experiments(
        self, mock_db, mocker, monkeypatch
    ):
        """Test that both metadata streams reach the files collection.

        Given:
            One configured annotation type, plus an experiment stream and an
            annotation stream each yielding one row.
        When:
            _sync_encode runs.
        Then:
            Both documents should be inserted, and the annotation one should
            carry the annotation_type that makes it findable.
        """
        # Arrange
        monkeypatch.setenv(
            "ENCODE_ANNOTATION_TYPES", "candidate Cis-Regulatory Elements"
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_metadata",
            lambda deadline=None: _async_iter([_encode_metadata_row("ENCFF001AAA", "x.bed.gz")]),
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            lambda annotation_type, deadline=None: _async_iter(
                [_encode_annotation_row("ENCFF002BBB", "ENCSR001AAA", "y.bed.gz")]
            ),
        )

        # Act
        await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        by_id = {d["local_id"]: d for d in mock_db.files.docs}
        assert set(by_id) == {"ENCFF001AAA", "ENCFF002BBB"}
        annotation = by_id["ENCFF002BBB"]
        assert (
            annotation["extra"]["encode"]["annotation_type"]
            == "candidate Cis-Regulatory Elements"
        )
        assert annotation["collections"][0]["accession_id"] == "ENCSR001AAA"

    @pytest.mark.asyncio
    async def test__sync_encode_should_request_only_the_configured_types(
        self, mock_db, mocker, monkeypatch
    ):
        """Test that the allowlist bounds which annotation types are fetched.

        Given:
            Two annotation types configured out of the far larger space
            ENCODE publishes.
        When:
            _sync_encode runs.
        Then:
            The annotation fetch should be called once per configured type
            and with no other type.
        """
        # Arrange
        monkeypatch.setenv("ENCODE_ANNOTATION_TYPES", "chromatin state, footprints")
        mocker.patch.object(
            encode_module, "fetch_encode_metadata", lambda deadline=None: _async_iter([])
        )
        fetch_annotations = mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            side_effect=lambda annotation_type, deadline=None: _async_iter([]),
        )

        # Act
        await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        requested = [call.args[0] for call in fetch_annotations.call_args_list]
        assert requested == ["chromatin state", "footprints"]

    @pytest.mark.asyncio
    async def test__sync_encode_should_ingest_annotations_when_experiments_fail(
        self, mock_db, mocker, monkeypatch
    ):
        """Test that a failed experiment stream does not cost the annotations.

        Given:
            An experiment stream that raises, and a healthy annotation
            stream.
        When:
            _sync_encode runs.
        Then:
            The annotation document should still be inserted, and the sync
            should fail afterwards naming the phase that broke.
        """
        # Arrange
        monkeypatch.setenv(
            "ENCODE_ANNOTATION_TYPES", "candidate Cis-Regulatory Elements"
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_metadata",
            lambda deadline=None: _async_raise(RuntimeError("experiment stream died")),
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            lambda annotation_type, deadline=None: _async_iter(
                [_encode_annotation_row("ENCFF002BBB", "ENCSR001AAA", "y.bed.gz")]
            ),
        )

        # Act & assert
        with pytest.raises(RuntimeError, match="experiment"):
            await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        assert [d["local_id"] for d in mock_db.files.docs] == ["ENCFF002BBB"]

    @pytest.mark.asyncio
    async def test__sync_encode_should_ingest_experiments_when_an_annotation_fails(
        self, mock_db, mocker, monkeypatch
    ):
        """Test that one failed annotation type does not cost the others.

        Given:
            Two configured annotation types where the first stream raises,
            alongside a healthy experiment stream.
        When:
            _sync_encode runs.
        Then:
            The experiment document and the surviving annotation type's
            document should both be inserted.
        """
        # Arrange
        monkeypatch.setenv("ENCODE_ANNOTATION_TYPES", "broken, healthy")
        mocker.patch.object(
            encode_module,
            "fetch_encode_metadata",
            lambda deadline=None: _async_iter([_encode_metadata_row("ENCFF001AAA", "x.bed.gz")]),
        )

        def _annotation_stream(annotation_type, deadline=None):
            if annotation_type == "broken":
                return _async_raise(RuntimeError("annotation stream died"))
            return _async_iter(
                [_encode_annotation_row("ENCFF002BBB", "ENCSR001AAA", "y.bed.gz")]
            )

        mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            side_effect=_annotation_stream,
        )

        # Act & assert
        with pytest.raises(RuntimeError, match="annotation\\[broken\\]"):
            await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        assert {d["local_id"] for d in mock_db.files.docs} == {
            "ENCFF001AAA",
            "ENCFF002BBB",
        }

    @pytest.mark.asyncio
    async def test__sync_encode_should_ensure_the_indexes_when_a_phase_failed(
        self, mock_db, mocker, monkeypatch
    ):
        """Test that a partial load is still left queryable.

        Given:
            An experiment stream that raises and a healthy annotation
            stream, so the sync ends in failure with rows loaded.
        When:
            _sync_encode runs.
        Then:
            The accession indexes should still be ensured, so what did load
            is not left behind a full collection scan.
        """
        # Arrange
        monkeypatch.setenv(
            "ENCODE_ANNOTATION_TYPES", "candidate Cis-Regulatory Elements"
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_metadata",
            lambda deadline=None: _async_raise(RuntimeError("experiment stream died")),
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            lambda annotation_type, deadline=None: _async_iter(
                [_encode_annotation_row("ENCFF002BBB", "ENCSR001AAA", "y.bed.gz")]
            ),
        )
        ensure = mocker.patch.object(
            sync_module, "ensure_indexes", mocker.AsyncMock(return_value=0)
        )

        # Act & assert
        with pytest.raises(RuntimeError):
            await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        assert ensure.await_count == 1

    @pytest.mark.asyncio
    async def test__sync_encode_should_report_the_rows_a_failed_phase_committed(
        self, mock_db, mocker, monkeypatch
    ):
        """Test the reported total matches what is actually in the database.

        A stream bounded by a wall-clock budget fails mid-flight, with rows
        already committed. Reporting a phase's contribution only on its
        clean return dropped every one of those rows from the total while
        leaving them in the collection, so the sync under-reported the
        corpus it had just written.

        Given:
            A batch size of 2 and an experiment stream that yields 5 rows
            and then dies, alongside a healthy annotation phase.
        When:
            _sync_encode runs.
        Then:
            The count in the final progress should equal the ENCODE
            documents actually present.
        """
        # Arrange
        monkeypatch.setenv(
            "ENCODE_ANNOTATION_TYPES", "candidate Cis-Regulatory Elements"
        )
        monkeypatch.setattr(sync_module, "BATCH_SIZE", 2)
        rows = [
            _encode_metadata_row(f"ENCFF00{i}AAA", f"f{i}.bed.gz") for i in range(5)
        ]
        mocker.patch.object(
            encode_module,
            "fetch_encode_metadata",
            lambda deadline=None: _async_iter_then_raise(rows, TimeoutError("budget exhausted")),
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            lambda annotation_type, deadline=None: _async_iter(
                [_encode_annotation_row("ENCFF999ZZZ", "ENCSR001AAA", "y.bed.gz")]
            ),
        )
        task = SyncTask(id="t1", dcc_names=["encode"])

        # Act & assert
        with pytest.raises(RuntimeError):
            await _sync_encode(task)

        inserted = [d for d in mock_db.files.docs if d["submission"] == "encode"]
        # Anchored rather than a substring: "6 files" is contained in
        # "16 files", so containment passes on an order-of-magnitude error.
        assert task.progress.startswith(
            f"ENCODE sync incomplete: {len(inserted)} files"
        )

    @pytest.mark.asyncio
    async def test__sync_encode_should_commit_the_partial_batch_of_a_failed_phase(
        self, mock_db, mocker, monkeypatch
    ):
        """Test rows transformed before a stream died are not thrown away.

        The DCC is cleared before the load, so a row discarded here is a
        row the corpus loses outright until the next full re-sync.

        Given:
            A batch size of 2 and a stream that yields 5 rows and then
            dies, leaving one row in an uncommitted partial batch.
        When:
            _sync_encode runs.
        Then:
            All 5 rows should be committed, not just the two full batches.
        """
        # Arrange
        monkeypatch.setattr(sync_module, "BATCH_SIZE", 2)
        rows = [
            _encode_metadata_row(f"ENCFF00{i}AAA", f"f{i}.bed.gz") for i in range(5)
        ]
        mocker.patch.object(
            encode_module,
            "fetch_encode_metadata",
            lambda deadline=None: _async_iter_then_raise(rows, TimeoutError("budget exhausted")),
        )

        # Act & assert
        with pytest.raises(RuntimeError):
            await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        assert len(mock_db.files.docs) == 5

    @pytest.mark.asyncio
    async def test__sync_encode_should_fail_when_the_trailing_batch_fails_to_commit(
        self, mock_db, mocker, monkeypatch
    ):
        """Test a sink failure on a cleanly drained stream fails the sync.

        The flush of the trailing partial batch used to sit in a ``finally``
        that suppressed its own failure. A ``finally`` also runs when nothing
        is propagating, so this reported a clean sync over a corpus short by
        the trailing batch -- against a DCC cleared before the load.

        Given:
            A batch size of 2 and a stream of 3 rows that drains cleanly,
            whose trailing one-row batch fails to insert.
        When:
            _sync_encode runs.
        Then:
            It should raise and report only the two rows that committed.
        """
        # Arrange
        monkeypatch.setattr(sync_module, "BATCH_SIZE", 2)
        rows = [
            _encode_metadata_row(f"ENCFF00{i}AAA", f"f{i}.bed.gz") for i in range(3)
        ]
        mocker.patch.object(
            encode_module, "fetch_encode_metadata", lambda deadline=None: _async_iter(rows)
        )
        _fail_insert_on_call(mock_db.files, 2, RuntimeError("mongo failover"))
        task = SyncTask(id="t1", dcc_names=["encode"])

        # Act & assert
        with pytest.raises(RuntimeError):
            await _sync_encode(task)

        assert len(mock_db.files.docs) == 2
        assert task.progress.startswith("ENCODE sync incomplete: 2 files")

    @pytest.mark.asyncio
    async def test__sync_encode_should_not_resubmit_a_batch_the_sink_rejected(
        self, mock_db, mocker, monkeypatch
    ):
        """Test a batch that failed to insert is not sent a second time.

        The buffer used to be cleared only after a successful insert, so a
        failed one left the same list for the trailing flush to submit again.

        Given:
            A batch size of 2 and a stream of 4 rows whose first batch fails
            to insert.
        When:
            _sync_encode runs.
        Then:
            The rejected batch should never reach the collection, and the
            reported count should exclude it.
        """
        # Arrange
        monkeypatch.setattr(sync_module, "BATCH_SIZE", 2)
        rows = [
            _encode_metadata_row(f"ENCFF00{i}AAA", f"f{i}.bed.gz") for i in range(4)
        ]
        mocker.patch.object(
            encode_module, "fetch_encode_metadata", lambda deadline=None: _async_iter(rows)
        )
        _fail_insert_on_call(mock_db.files, 1, RuntimeError("mongo failover"))
        task = SyncTask(id="t1", dcc_names=["encode"])

        # Act & assert
        with pytest.raises(RuntimeError):
            await _sync_encode(task)

        assert mock_db.files.docs == []
        assert task.progress.startswith("ENCODE sync incomplete: 0 files")

    @pytest.mark.asyncio
    async def test__sync_encode_should_give_every_phase_the_same_deadline(
        self, mock_db, mocker, monkeypatch
    ):
        """Test the download budget bounds the sync rather than each stream.

        Every phase runs inside the one cutover lock that gates the read
        surface, so a per-stream budget would multiply the outage by the
        phase count and carry the worst case past the sync lock's one-hour
        stale threshold -- at which point a second sync is admitted and
        clears the corpus while this one is still writing to it.

        Given:
            Two configured annotation types, so three phases run.
        When:
            _sync_encode runs.
        Then:
            Every phase should be handed one and the same deadline.
        """
        # Arrange
        monkeypatch.setenv("ENCODE_ANNOTATION_TYPES", "alpha,beta")
        deadlines = []

        def _experiment_stream(deadline=None):
            deadlines.append(deadline)
            return _async_iter([])

        def _annotation_stream(annotation_type, deadline=None):
            deadlines.append(deadline)
            return _async_iter([])

        mocker.patch.object(
            encode_module, "fetch_encode_metadata", _experiment_stream
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            side_effect=_annotation_stream,
        )

        # Act
        await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        assert len(deadlines) == 3
        assert None not in deadlines
        assert len(set(deadlines)) == 1

    @pytest.mark.asyncio
    async def test__sync_encode_should_ingest_a_repeated_type_only_once(
        self, mock_db, mocker, monkeypatch
    ):
        """Test a duplicated allowlist entry does not double-load its type.

        ENCODE documents are written with insert_many into a collection
        carrying no unique key, so a repeated token would insert every file
        of that type twice with nothing to reject it.

        Given:
            ENCODE_ANNOTATION_TYPES naming the same type twice.
        When:
            _sync_encode runs.
        Then:
            The annotation fetch should run once and each document should
            appear once.
        """
        # Arrange
        monkeypatch.setenv("ENCODE_ANNOTATION_TYPES", "chromatin state,chromatin state")
        mocker.patch.object(
            encode_module, "fetch_encode_metadata", lambda deadline=None: _async_iter([])
        )
        fetch_annotations = mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            side_effect=lambda annotation_type, deadline=None: _async_iter(
                [_encode_annotation_row("ENCFF002BBB", "ENCSR001AAA", "y.bed.gz")]
            ),
        )

        # Act
        await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        assert fetch_annotations.call_count == 1
        assert [d["local_id"] for d in mock_db.files.docs] == ["ENCFF002BBB"]

    @pytest.mark.asyncio
    async def test__sync_encode_should_report_a_configured_type_that_returned_nothing(
        self, mock_db, mocker, monkeypatch, caplog
    ):
        """Test an empty annotation type is logged as zero, not omitted.

        A type ENCODE stops publishing is the failure this tally exists to
        surface. An absent key is what nobody notices when diffing logs; a
        zero is what everybody does.

        Given:
            Two configured types, one returning rows and one returning
            none.
        When:
            _sync_encode runs.
        Then:
            The annotation_type distribution should carry the empty type
            with a count of zero.
        """
        # Arrange
        monkeypatch.setenv("ENCODE_ANNOTATION_TYPES", "populated,vanished")
        mocker.patch.object(
            encode_module, "fetch_encode_metadata", lambda deadline=None: _async_iter([])
        )

        def _annotation_stream(annotation_type, deadline=None):
            if annotation_type == "vanished":
                return _async_iter([])
            return _async_iter(
                [_encode_annotation_row("ENCFF002BBB", "ENCSR001AAA", "y.bed.gz")]
            )

        mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            side_effect=_annotation_stream,
        )

        # Act
        with caplog.at_level(logging.INFO):
            await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        distribution = next(
            m for m in caplog.messages if "annotation_type distribution" in m
        )
        assert "'vanished': 0" in distribution

    @pytest.mark.asyncio
    async def test__sync_encode_should_propagate_a_cancellation(
        self, mock_db, mocker, monkeypatch
    ):
        """Test cancellation cancels the sync rather than failing one phase.

        The per-phase handler is deliberately broad, and a CancelledError
        caught by it would be logged as a phase failure and followed by
        every remaining phase -- the opposite of cancelling.

        Given:
            An experiment stream raising CancelledError and a configured
            annotation type.
        When:
            _sync_encode runs.
        Then:
            The CancelledError should propagate and the annotation phase
            should never run.
        """
        # Arrange
        monkeypatch.setenv(
            "ENCODE_ANNOTATION_TYPES", "candidate Cis-Regulatory Elements"
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_metadata",
            lambda deadline=None: _async_raise(asyncio.CancelledError()),
        )
        fetch_annotations = mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            side_effect=lambda annotation_type, deadline=None: _async_iter([]),
        )

        # Act & assert
        with pytest.raises(asyncio.CancelledError):
            await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        fetch_annotations.assert_not_called()

    @pytest.mark.asyncio
    async def test__sync_encode_should_not_commit_buffered_rows_when_cancelled(
        self, mock_db, mocker, monkeypatch
    ):
        """Test a cancelled sync stops writing rather than flushing on its way out.

        The trailing-batch flush runs from a ``finally``, which a cancellation
        unwinds through as readily as a failure. Writing there would commit
        rows after the decision to stop, and a cancellation re-delivered by
        that await would replace the original and skip the phase's tally.

        Given:
            A batch size large enough to leave every row buffered, and a
            stream that yields three rows and then raises CancelledError.
        When:
            _sync_encode runs.
        Then:
            The cancellation should propagate with the buffered rows never
            committed.
        """
        # Arrange
        monkeypatch.setattr(sync_module, "BATCH_SIZE", 100)
        rows = [
            _encode_metadata_row(f"ENCFF00{i}AAA", f"f{i}.bed.gz") for i in range(3)
        ]
        mocker.patch.object(
            encode_module,
            "fetch_encode_metadata",
            lambda deadline=None: _async_iter_then_raise(rows, asyncio.CancelledError()),
        )

        # Act & assert
        with pytest.raises(asyncio.CancelledError):
            await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        assert mock_db.files.docs == []

    @pytest.mark.asyncio
    async def test__sync_encode_should_still_index_when_every_phase_fails(
        self, mock_db, mocker, monkeypatch
    ):
        """Test the total-failure path leaves a coherent, queryable state.

        No phase delivered a row, so no phase replaced its slice: the corpus
        is last sync's, in full, rather than empty. Emptying it would have
        been the one outcome with nothing to recommend it -- the load failed,
        so there is nothing to put in its place.

        Given:
            An experiment stream and both configured annotation streams
            all raising, over a pre-seeded stale ENCODE document.
        When:
            _sync_encode runs.
        Then:
            The stale document should survive, the indexes should still be
            ensured, and the error should name all three phases.
        """
        # Arrange
        monkeypatch.setenv("ENCODE_ANNOTATION_TYPES", "alpha,beta")
        mock_db.files.docs = [{"submission": "encode", "local_id": "STALE"}]
        mocker.patch.object(
            encode_module,
            "fetch_encode_metadata",
            lambda deadline=None: _async_raise(RuntimeError("experiment died")),
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            side_effect=lambda annotation_type, deadline=None: _async_raise(
                RuntimeError(f"{annotation_type} died")
            ),
        )
        ensure = mocker.patch.object(
            sync_module, "ensure_indexes", mocker.AsyncMock(return_value=0)
        )
        task = SyncTask(id="t1", dcc_names=["encode"])

        # Act & assert
        with pytest.raises(RuntimeError) as excinfo:
            await _sync_encode(task)

        assert [d["local_id"] for d in mock_db.files.docs] == ["STALE"]
        assert ensure.await_count == 1
        for label in ("experiment", "annotation[alpha]", "annotation[beta]"):
            assert label in str(excinfo.value)
        assert "0 of 3 phases" in task.progress

    @pytest.mark.asyncio
    async def test__sync_encode_should_chain_the_first_failure_as_the_cause(
        self, mock_db, mocker, monkeypatch
    ):
        """Test the raised error keeps the first failure's traceback.

        Given:
            Two failing phases raising distinguishable exceptions.
        When:
            _sync_encode runs.
        Then:
            The RuntimeError's __cause__ should be the first one, so the
            traceback points at the failure that started the cascade.
        """
        # Arrange
        monkeypatch.setenv("ENCODE_ANNOTATION_TYPES", "alpha")
        first = RuntimeError("experiment died first")
        second = RuntimeError("annotation died second")
        mocker.patch.object(
            encode_module, "fetch_encode_metadata", lambda deadline=None: _async_raise(first)
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            side_effect=lambda annotation_type, deadline=None: _async_raise(second),
        )

        # Act & assert
        with pytest.raises(RuntimeError) as excinfo:
            await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        assert excinfo.value.__cause__ is first

    @pytest.mark.asyncio
    async def test__sync_encode_should_not_report_a_partial_load_as_complete(
        self, mock_db, mocker, monkeypatch
    ):
        """Test the task's own progress text admits a phase was lost.

        Given:
            An experiment stream that raises and a healthy annotation
            stream.
        When:
            _sync_encode runs.
        Then:
            The task's final progress should say the sync was incomplete,
            rather than reporting a clean sync over a partial corpus.
        """
        # Arrange
        monkeypatch.setenv(
            "ENCODE_ANNOTATION_TYPES", "candidate Cis-Regulatory Elements"
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_metadata",
            lambda deadline=None: _async_raise(RuntimeError("experiment stream died")),
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            lambda annotation_type, deadline=None: _async_iter(
                [_encode_annotation_row("ENCFF002BBB", "ENCSR001AAA", "y.bed.gz")]
            ),
        )
        task = SyncTask(id="t1", dcc_names=["encode"])

        # Act & assert
        with pytest.raises(RuntimeError):
            await _sync_encode(task)

        assert "incomplete" in task.progress

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "row_count, expected_inserts",
        [(3, 1), (4, 2), (0, 0)],
        ids=["exactly-one-batch", "one-batch-plus-remainder", "empty-stream"],
    )
    async def test__sync_encode_should_insert_one_batch_per_batch_size_rows(
        self, mock_db, mocker, monkeypatch, row_count, expected_inserts
    ):
        """Test the batching boundary commits every row exactly once.

        The three cases are the boundary itself, the remainder path, and
        the empty stream that must not issue a trailing empty insert.

        Given:
            A batch size of 3 and a stream of 3, 4 or 0 rows.
        When:
            _sync_encode runs.
        Then:
            insert_many should be called the expected number of times and
            every row should land exactly once.
        """
        # Arrange
        monkeypatch.setattr(sync_module, "BATCH_SIZE", 3)
        rows = [
            _encode_metadata_row(f"ENCFF00{i}AAA", f"f{i}.bed.gz")
            for i in range(row_count)
        ]
        mocker.patch.object(
            encode_module, "fetch_encode_metadata", lambda deadline=None: _async_iter(rows)
        )
        # Spied, not asserted on by argument: _ingest_encode_rows clears the
        # same list it hands to insert_many, so a recorded call reads back
        # empty.
        spy = mocker.spy(mock_db.files, "insert_many")

        # Act
        await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        assert spy.call_count == expected_inserts
        assert len(mock_db.files.docs) == row_count

    @pytest.mark.asyncio
    async def test__sync_encode_should_not_count_a_row_it_skipped(
        self, mock_db, mocker, caplog
    ):
        """Test an untransformable row is absent from the corpus and tallies.

        Given:
            Three experiment rows where the middle one has no File
            accession and so transforms to None.
        When:
            _sync_encode runs.
        Then:
            Two documents should be inserted and the compression
            distribution should account for two, not three.
        """
        # Arrange
        rows = [
            _encode_metadata_row("ENCFF001AAA", "a.bed.gz"),
            {"File accession": "   ", "File format": "bed"},
            _encode_metadata_row("ENCFF003CCC", "c.bed.gz"),
        ]
        mocker.patch.object(
            encode_module, "fetch_encode_metadata", lambda deadline=None: _async_iter(rows)
        )
        task = SyncTask(id="t1", dcc_names=["encode"])

        # Act
        with caplog.at_level(logging.INFO):
            await _sync_encode(task)

        # Assert
        assert len(mock_db.files.docs) == 2
        assert task.progress == "ENCODE sync complete: 2 files"
        distribution = next(
            m for m in caplog.messages if "compression_format distribution" in m
        )
        assert "'format:3989': 2" in distribution

    @pytest.mark.asyncio
    async def test__sync_encode_should_not_let_a_phase_clear_another_phases_rows(
        self, mock_db, mocker, monkeypatch
    ):
        """Test each phase's clear reaches only the slice that phase reloads.

        Every phase clears before loading, so a filter that selected more
        than its own slice would delete the preceding phases' documents as
        each new one started, leaving only the last phase's rows.

        Given:
            A stale ENCODE document and three healthy phases.
        When:
            _sync_encode runs.
        Then:
            The stale document should be gone and all three phases'
            documents should survive together.
        """
        # Arrange
        monkeypatch.setenv("ENCODE_ANNOTATION_TYPES", "alpha,beta")
        mock_db.files.docs = [{"submission": "encode", "local_id": "STALE"}]
        mocker.patch.object(
            encode_module,
            "fetch_encode_metadata",
            lambda deadline=None: _async_iter([_encode_metadata_row("ENCFF001AAA", "a.bed.gz")]),
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            side_effect=lambda annotation_type, deadline=None: _async_iter(
                [
                    _encode_annotation_row(
                        f"ENCFF_{annotation_type}", "ENCSR001AAA", "y.bed.gz"
                    )
                ]
            ),
        )

        # Act
        await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        assert {d["local_id"] for d in mock_db.files.docs} == {
            "ENCFF001AAA",
            "ENCFF_alpha",
            "ENCFF_beta",
        }

    @pytest.mark.asyncio
    async def test__sync_encode_should_keep_a_failed_phases_rows_until_it_reloads_them(
        self, mock_db, mocker, monkeypatch
    ):
        """Test a failed phase serves stale data rather than none.

        One corpus-wide clear before the fan-out destroyed every phase's data
        before any failure was knowable, so a failed experiment phase left
        the API serving the annotation documents as the whole corpus. The
        experiment phase is the largest and slowest stream, so it is the
        likeliest to be the one that fails.

        Given:
            A stale experiment document, an experiment phase that fails
            before yielding, and a healthy annotation phase.
        When:
            _sync_encode runs.
        Then:
            The stale experiment document should survive alongside the
            freshly loaded annotation document.
        """
        # Arrange
        monkeypatch.setenv("ENCODE_ANNOTATION_TYPES", "alpha")
        mock_db.files.docs = [{"submission": "encode", "local_id": "STALE_EXPERIMENT"}]
        mocker.patch.object(
            encode_module,
            "fetch_encode_metadata",
            lambda deadline=None: _async_raise(RuntimeError("experiment died")),
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            side_effect=lambda annotation_type, deadline=None: _async_iter(
                [_encode_annotation_row("ENCFF_alpha", "ENCSR001AAA", "y.bed.gz")]
            ),
        )

        # Act & assert
        with pytest.raises(RuntimeError):
            await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        assert {d["local_id"] for d in mock_db.files.docs} == {
            "STALE_EXPERIMENT",
            "ENCFF_alpha",
        }

    @pytest.mark.asyncio
    async def test__sync_encode_should_empty_a_slice_whose_stream_returned_nothing(
        self, mock_db, mocker, monkeypatch
    ):
        """Test a type that stopped being published stops being served.

        The clear is deferred until a phase has rows to put back, so that a
        phase failing before it delivers any leaves its previous rows alone.
        A stream that drains cleanly with nothing in it is the other case
        entirely: the empty result is the answer, and keeping last sync's
        rows would serve documents ENCODE no longer publishes.

        Given:
            A stale document for a configured type whose stream then yields
            no rows at all.
        When:
            _sync_encode runs.
        Then:
            The stale document should be gone.
        """
        # Arrange
        monkeypatch.setenv("ENCODE_ANNOTATION_TYPES", "alpha")
        mock_db.files.docs = [
            {
                "submission": "encode",
                "local_id": "STALE_ALPHA",
                "extra": {"encode": {"annotation_type": "alpha"}},
            }
        ]
        mocker.patch.object(
            encode_module, "fetch_encode_metadata", lambda deadline=None: _async_iter([])
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            side_effect=lambda annotation_type, deadline=None: _async_iter([]),
        )

        # Act
        await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        assert mock_db.files.docs == []

    @pytest.mark.asyncio
    async def test__sync_encode_should_record_a_failed_phase_as_zero_in_the_per_phase_log(
        self, mock_db, mocker, monkeypatch, caplog
    ):
        """Test the per-phase log distinguishes what loaded from what did not.

        Given:
            One healthy annotation phase and one that fails before
            yielding.
        When:
            _sync_encode runs.
        Then:
            The per-phase counts should name the healthy phase and record
            the failed one as having loaded nothing.
        """
        # Arrange
        monkeypatch.setenv("ENCODE_ANNOTATION_TYPES", "healthy,broken")
        mocker.patch.object(
            encode_module, "fetch_encode_metadata", lambda deadline=None: _async_iter([])
        )

        def _annotation_stream(annotation_type, deadline=None):
            if annotation_type == "broken":
                return _async_raise(RuntimeError("died"))
            return _async_iter(
                [_encode_annotation_row("ENCFF002BBB", "ENCSR001AAA", "y.bed.gz")]
            )

        mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            side_effect=_annotation_stream,
        )

        # Act
        with caplog.at_level(logging.INFO):
            with pytest.raises(RuntimeError):
                await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        per_phase = next(m for m in caplog.messages if "inserted per phase" in m)
        assert "'annotation[healthy]': 1" in per_phase
        assert "'annotation[broken]': 0" in per_phase

    @pytest.mark.asyncio
    @settings(
        max_examples=40,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        # One (row count, fails) pair per annotation phase, plus one for the
        # experiment phase, so the fan-out and the failure pattern both vary.
        phase_specs=st.lists(
            st.tuples(st.integers(min_value=0, max_value=6), st.booleans()),
            min_size=1,
            max_size=4,
        ),
        batch_size=st.integers(min_value=1, max_value=4),
    )
    async def test__sync_encode_should_report_exactly_what_it_loaded(
        self, mocker, monkeypatch, phase_specs, batch_size
    ):
        """Test the reported total always equals the documents committed.

        The invariant the mid-stream accounting defect broke, over
        arbitrary fan-out and failure patterns rather than the handful of
        shapes the example tests fix.

        Given:
            Any number of phases with any row counts, any subset of which
            die after yielding all their rows, at any batch size.
        When:
            _sync_encode runs.
        Then:
            The count in the final progress should equal the ENCODE
            documents in the collection.
        """
        # Arrange
        # A fresh database per generated example rather than the function
        # scoped ``mock_db`` fixture, which Hypothesis does not reset between
        # examples -- documents would accumulate across them.
        db = FakeDB()
        monkeypatch.setattr(api, "db", db)
        monkeypatch.setattr(sync_module, "BATCH_SIZE", batch_size)
        experiment_spec, *annotation_specs = phase_specs
        monkeypatch.setenv(
            "ENCODE_ANNOTATION_TYPES",
            ",".join(f"type{i}" for i in range(len(annotation_specs))),
        )

        def _stream(prefix, spec):
            count, fails = spec
            rows = [
                _encode_metadata_row(f"ENCFF{prefix}{i:03d}", f"f{i}.bed.gz")
                for i in range(count)
            ]
            if fails:
                return _async_iter_then_raise(rows, RuntimeError("died"))
            return _async_iter(rows)

        mocker.patch.object(
            encode_module,
            "fetch_encode_metadata",
            lambda deadline=None: _stream("E", experiment_spec),
        )
        mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            side_effect=lambda annotation_type, deadline=None: _stream(
                annotation_type[-1], annotation_specs[int(annotation_type[-1])]
            ),
        )
        task = SyncTask(id="t1", dcc_names=["encode"])
        any_failure = any(fails for _, fails in phase_specs)

        # Act
        if any_failure:
            with pytest.raises(RuntimeError):
                await _sync_encode(task)
        else:
            await _sync_encode(task)

        # Assert
        committed = len([d for d in db.files.docs if d["submission"] == "encode"])
        # Anchored rather than a substring. The generated counts reach into
        # the twenties, so "6 files" would be satisfied by a reported
        # "16 files" -- and this assertion is the one standing guard over
        # the reported half of the accounting invariant.
        state = "incomplete" if any_failure else "complete"
        assert task.progress.startswith(f"ENCODE sync {state}: {committed} files")
        assert committed == sum(count for count, _ in phase_specs)

    @pytest.mark.asyncio
    async def test__sync_encode_should_run_the_default_allowlist_when_unset(
        self, mock_db, mocker, monkeypatch
    ):
        """Test the default allowlist reaches the sync, not just the parser.

        Every other test in this class pins the variable, so without this
        one nothing joins ``annotation_types_from_env``'s default to the
        phases actually run.

        Given:
            No ENCODE_ANNOTATION_TYPES in the environment.
        When:
            _sync_encode runs.
        Then:
            It should fetch exactly the two default annotation types.
        """
        # Arrange
        monkeypatch.delenv("ENCODE_ANNOTATION_TYPES", raising=False)
        mocker.patch.object(
            encode_module, "fetch_encode_metadata", lambda deadline=None: _async_iter([])
        )
        fetch_annotations = mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            side_effect=lambda annotation_type, deadline=None: _async_iter([]),
        )

        # Act
        await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        assert [call.args[0] for call in fetch_annotations.call_args_list] == [
            "candidate Cis-Regulatory Elements",
            "element gene regulatory interaction predictions",
        ]

    @pytest.mark.asyncio
    async def test__sync_encode_should_skip_annotations_when_the_allowlist_is_empty(
        self, mock_db, mocker, monkeypatch
    ):
        """Test that an explicitly empty allowlist disables the fetch entirely.

        Given:
            ENCODE_ANNOTATION_TYPES set to an empty value.
        When:
            _sync_encode runs.
        Then:
            No annotation request should be made, and the experiment ingest
            should complete normally.
        """
        # Arrange
        monkeypatch.setenv("ENCODE_ANNOTATION_TYPES", "")
        mocker.patch.object(
            encode_module,
            "fetch_encode_metadata",
            lambda deadline=None: _async_iter([_encode_metadata_row("ENCFF001AAA", "x.bed.gz")]),
        )
        fetch_annotations = mocker.patch.object(
            encode_module,
            "fetch_encode_annotation_metadata",
            side_effect=lambda annotation_type, deadline=None: _async_iter([]),
        )

        # Act
        await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        fetch_annotations.assert_not_called()
        assert [d["local_id"] for d in mock_db.files.docs] == ["ENCFF001AAA"]


class TestStamp4dnFileAccessions:
    @pytest.mark.asyncio
    async def test__stamp_4dn_file_accessions_should_write_the_raw_file_collection(
        self, mock_db
    ):
        """Test that the accession survives a re-materialization.

        The materializer rebuilds ``files`` from ``file`` on every run, so a
        value written to ``files`` lasts only until the next
        ``make materialize-dcc``. Writing the raw document instead lets
        ``enrich_file``'s in-place mutation carry it forward, which is what
        already makes the collection accession durable.

        Given:
            A raw 4DN file document whose persistent_id carries an accession.
        When:
            _stamp_4dn_file_accessions runs.
        Then:
            It should stamp the raw file collection, leaving the derived
            files collection untouched.
        """
        # Arrange
        mock_db.file.docs = [
            {
                "_id": "f1",
                "submission": "4dn",
                "persistent_id": "https://data.4dnucleome.org/4DNFIMCJXZKH",
            }
        ]
        mock_db.files.docs = [{"_id": "f1", "submission": "4dn"}]

        # Act
        await _stamp_4dn_file_accessions()

        # Assert
        assert mock_db.file.docs[0]["accession_id"] == "4DNFIMCJXZKH"
        assert "accession_id" not in mock_db.files.docs[0]

    @pytest.mark.asyncio
    async def test__stamp_4dn_file_accessions_should_stamp_without_the_search_api(
        self, mock_db
    ):
        """Test that accession_id does not depend on the Search API at all.

        Given:
            Two raw 4DN files with parseable accessions and no Search API
            stub of any kind, since this pass predates the fetch.
        When:
            _stamp_4dn_file_accessions runs.
        Then:
            It should stamp both, so every 4DN file is queryable by accession
            rather than only the API-matched subset.
        """
        # Arrange
        mock_db.file.docs = [
            {
                "_id": "f1",
                "submission": "4dn",
                "persistent_id": "https://data.4dnucleome.org/4DNFIMCJXZKH",
            },
            {
                "_id": "f2",
                "submission": "4dn",
                "persistent_id": "https://data.4dnucleome.org/4DNFIMEMLGM5",
            },
        ]

        # Act
        await _stamp_4dn_file_accessions()

        # Assert
        assert [d["accession_id"] for d in mock_db.file.docs] == [
            "4DNFIMCJXZKH",
            "4DNFIMEMLGM5",
        ]

    @pytest.mark.asyncio
    async def test__stamp_4dn_file_accessions_should_warn_when_an_accession_is_unparseable(
        self, mock_db, caplog
    ):
        """Test the operator's only signal that the field is partial.

        Given:
            Two raw 4DN files, one of whose persistent_ids carries no
            accession.
        When:
            _stamp_4dn_file_accessions runs.
        Then:
            It should log a warning naming the unparseable count, since a
            null accession_id is otherwise indistinguishable from a DCC
            that issues no accession.
        """
        # Arrange
        mock_db.file.docs = [
            {
                "_id": "f1",
                "submission": "4dn",
                "persistent_id": "https://data.4dnucleome.org/4DNFIMCJXZKH",
            },
            {"_id": "f2", "submission": "4dn", "persistent_id": "https://x/nope"},
        ]

        # Act
        with caplog.at_level(logging.WARNING):
            await _stamp_4dn_file_accessions()

        # Assert
        assert "1 files have no parseable accession" in caplog.text

    @pytest.mark.asyncio
    async def test__stamp_4dn_file_accessions_should_leave_accession_id_unset_when_unparseable(
        self, mock_db
    ):
        """Test that a file with no parseable accession is skipped, not failed.

        Given:
            A raw 4DN file whose persistent_id carries no 4DNF accession.
        When:
            _stamp_4dn_file_accessions runs.
        Then:
            It should leave accession_id absent and complete without raising,
            so one malformed row cannot abort the sync.
        """
        # Arrange
        mock_db.file.docs = [
            {
                "_id": "f1",
                "submission": "4dn",
                "persistent_id": "https://data.4dnucleome.org/no-accession-here",
            }
        ]

        # Act
        await _stamp_4dn_file_accessions()

        # Assert
        assert "accession_id" not in mock_db.file.docs[0]


class TestEnrich4dnApiMetadata:
    @pytest.mark.asyncio
    async def test__enrich_4dn_api_metadata_should_enrich_both_files_sharing_an_accession(
        self, mocker, mock_db
    ):
        """Test that a duplicate accession does not cost a file its metadata.

        The lookup was an accession-keyed dict, which is last-write-wins:
        of two files resolving to one accession, whichever the cursor
        yielded second overwrote the first and only it was enriched. Which
        one lost depended on cursor order, and nothing reported it.

        Given:
            Two 4DN files whose persistent_ids carry the same accession,
            and a Search API returning metadata for it.
        When:
            _enrich_4dn_api_metadata runs.
        Then:
            It should enrich both.
        """
        # Arrange
        mock_db.files.docs = [
            {
                "_id": "f1",
                "submission": "4dn",
                "persistent_id": "https://data.4dnucleome.org/4DNFIMCJXZKH",
            },
            {
                "_id": "f2",
                "submission": "4dn",
                "persistent_id": "https://data.4dnucleome.org/4DNFIMCJXZKH/@@download",
            },
        ]
        mocker.patch.object(
            fourdn_module,
            "fetch_file_metadata_bulk",
            mocker.AsyncMock(
                return_value={"4DNFIMCJXZKH": {"genome_assembly": "GRCh38"}}
            ),
        )
        mocker.patch.object(
            fourdn_module, "fetch_biosource_tiers", mocker.AsyncMock(return_value={})
        )

        # Act
        await _enrich_4dn_api_metadata()

        # Assert
        assert [d.get("genome_assembly") for d in mock_db.files.docs] == [
            "GRCh38",
            "GRCh38",
        ]

    @pytest.mark.asyncio
    async def test__enrich_4dn_api_metadata_should_enrich_a_mixed_case_persistent_id(
        self, mocker, mock_db
    ):
        """Test that the extracted accession joins the Search API response.

        The extracted value is the key for the API round trip, and the
        portal answers with its own upper-case form. While the extractor
        returned the raw match, a mixed-case persistent_id produced a key
        that joined against nothing: the file kept a correct accession_id
        and silently lost every enriched field, without being counted in
        the unparseable warning that is the operator's only signal.

        Given:
            A 4DN file whose persistent_id carries a mixed-case accession,
            and a Search API keyed on the canonical upper-case form.
        When:
            _enrich_4dn_api_metadata runs.
        Then:
            It should apply the enrichment, not merely fail quietly.
        """
        # Arrange
        mock_db.files.docs = [
            {
                "_id": "f1",
                "submission": "4dn",
                "persistent_id": "https://data.4dnucleome.org/4DNFImcjxzkh",
            }
        ]
        mocker.patch.object(
            fourdn_module,
            "fetch_file_metadata_bulk",
            mocker.AsyncMock(
                return_value={"4DNFIMCJXZKH": {"genome_assembly": "GRCh38"}}
            ),
        )
        mocker.patch.object(
            fourdn_module, "fetch_biosource_tiers", mocker.AsyncMock(return_value={})
        )

        # Act
        await _enrich_4dn_api_metadata()

        # Assert
        assert mock_db.files.docs[0]["genome_assembly"] == "GRCh38"

    @pytest.mark.asyncio
    async def test__enrich_4dn_api_metadata_should_enrich_only_the_matched_file(
        self, mocker, mock_db
    ):
        """Test that enrichment applies to exactly the API-matched subset.

        Given:
            Two materialized 4DN files with parseable accessions, where the
            Search API returns metadata for only one of them.
        When:
            _enrich_4dn_api_metadata runs.
        Then:
            It should apply the enrichment fields to the matched file only,
            leaving the unmatched one untouched.
        """
        # Arrange
        mock_db.files.docs = [
            {
                "_id": "f1",
                "submission": "4dn",
                "persistent_id": "https://data.4dnucleome.org/4DNFIMCJXZKH",
            },
            {
                "_id": "f2",
                "submission": "4dn",
                "persistent_id": "https://data.4dnucleome.org/4DNFIMEMLGM5",
            },
        ]
        mocker.patch.object(
            fourdn_module,
            "fetch_file_metadata_bulk",
            mocker.AsyncMock(
                return_value={"4DNFIMCJXZKH": {"genome_assembly": "GRCh38"}}
            ),
        )
        mocker.patch.object(
            fourdn_module, "fetch_biosource_tiers", mocker.AsyncMock(return_value={})
        )

        # Act
        await _enrich_4dn_api_metadata()

        # Assert
        matched, unmatched = mock_db.files.docs
        assert matched["genome_assembly"] == "GRCh38"
        assert "genome_assembly" not in unmatched


class TestEnrich4dnCollections:
    @pytest.mark.asyncio
    async def test__enrich_4dn_collections_should_stamp_accession_id_when_api_returns_nothing(
        self, mocker, mock_db
    ):
        """Test that the collection accession does not depend on an API match.

        Given:
            A raw 4DN collection whose persistent_id carries an experiment
            accession, and a Search API that returns no experiments.
        When:
            _enrich_4dn_collections runs.
        Then:
            It should still set accession_id, so the value is present for the
            materializer to embed into files.collections[].
        """
        # Arrange
        mock_db.collection.docs = [
            {
                "_id": "c1",
                "submission": "4dn",
                "persistent_id": "https://data.4dnucleome.org/4DNEXNHE6X77",
            }
        ]
        mocker.patch.object(
            fourdn_module,
            "fetch_experiment_metadata_bulk",
            mocker.AsyncMock(return_value={}),
        )

        # Act
        await _enrich_4dn_collections()

        # Assert
        assert mock_db.collection.docs[0]["accession_id"] == "4DNEXNHE6X77"

    @pytest.mark.asyncio
    async def test__enrich_4dn_collections_should_stamp_accession_id_when_the_api_raises(
        self, mocker, mock_db
    ):
        """Test that the stamp survives an API failure, not just an empty result.

        fetch_experiment_metadata_bulk catches only aiohttp.ClientError, so
        a TimeoutError from its 60-second budget propagates. While the
        fetch ran before the scan, that aborted the pass with nothing
        stamped -- the same guarantee the file pass makes, but only against
        the gentler failure.

        Given:
            A raw 4DN collection with a parseable accession, and a Search
            API that raises rather than returning an empty result.
        When:
            _enrich_4dn_collections runs.
        Then:
            It should have stamped accession_id before the failure
            propagates.
        """
        # Arrange
        mock_db.collection.docs = [
            {
                "_id": "c1",
                "submission": "4dn",
                "persistent_id": "https://data.4dnucleome.org/4DNEXNHE6X77",
            }
        ]
        mocker.patch.object(
            fourdn_module,
            "fetch_experiment_metadata_bulk",
            mocker.AsyncMock(side_effect=asyncio.TimeoutError()),
        )

        # Act
        with pytest.raises(asyncio.TimeoutError):
            await _enrich_4dn_collections()

        # Assert
        assert mock_db.collection.docs[0]["accession_id"] == "4DNEXNHE6X77"

    @pytest.mark.asyncio
    async def test__enrich_4dn_collections_should_leave_accession_id_unset_when_unparseable(
        self, mocker, mock_db
    ):
        """Test that a collection with no parseable accession is skipped.

        Given:
            A 4DN collection whose persistent_id carries no 4DNE accession.
        When:
            _enrich_4dn_collections runs.
        Then:
            It should leave accession_id absent and complete without raising.
        """
        # Arrange
        mock_db.collection.docs = [
            {
                "_id": "c1",
                "submission": "4dn",
                "persistent_id": "https://data.4dnucleome.org/nothing-here",
            }
        ]
        mocker.patch.object(
            fourdn_module,
            "fetch_experiment_metadata_bulk",
            mocker.AsyncMock(return_value={}),
        )

        # Act
        await _enrich_4dn_collections()

        # Assert
        assert "accession_id" not in mock_db.collection.docs[0]

    @pytest.mark.asyncio
    async def test__enrich_4dn_collections_should_stamp_accession_id_alongside_api_metadata(
        self, mocker, mock_db
    ):
        """Test that stamping does not displace the existing enrichment.

        Given:
            A 4DN collection the Search API does return experiment metadata for.
        When:
            _enrich_4dn_collections runs.
        Then:
            It should set accession_id and the promoted experiment fields
            together, so the new write does not regress the existing pass.
        """
        # Arrange
        mock_db.collection.docs = [
            {
                "_id": "c1",
                "submission": "4dn",
                "persistent_id": "https://data.4dnucleome.org/4DNEXNHE6X77",
            }
        ]
        mocker.patch.object(
            fourdn_module,
            "fetch_experiment_metadata_bulk",
            mocker.AsyncMock(
                return_value={
                    "4DNEXNHE6X77": {
                        "lab": "Some Lab",
                        "experiment_type": "in situ Hi-C",
                        # Lands under extra.fourdn via a dotted $set, so this
                        # also pins that the nested write still nests.
                        "status": "released",
                    }
                }
            ),
        )

        # Act
        await _enrich_4dn_collections()

        # Assert
        doc = mock_db.collection.docs[0]
        assert doc["accession_id"] == "4DNEXNHE6X77"
        assert doc["lab"] == "Some Lab"
        assert doc["experiment_type"] == "in situ Hi-C"
        assert doc["extra"]["fourdn"] == {"status": "released"}


class TestLogAccessionCoverage:
    @pytest.mark.asyncio
    async def test__log_accession_coverage_should_report_partial_coverage(
        self, mock_db, caplog
    ):
        """Test that the operator can see how much of a DCC is queryable.

        Given:
            Three 4DN files of which two carry an accession.
        When:
            _log_accession_coverage runs for that DCC.
        Then:
            It should log the covered and total counts.
        """
        # Arrange
        mock_db.files.docs = [
            {"_id": "f1", "submission": "4dn", "accession_id": "4DNFAAA"},
            {"_id": "f2", "submission": "4dn", "accession_id": "4DNFBBB"},
            {"_id": "f3", "submission": "4dn"},
        ]

        # Act
        with caplog.at_level(logging.INFO):
            await sync_module._log_accession_coverage("4dn")

        # Assert
        assert "2/3" in caplog.text

    @pytest.mark.asyncio
    async def test__log_accession_coverage_should_warn_when_nothing_is_covered(
        self, mock_db, caplog
    ):
        """Test the signal that distinguishes empty from unpopulated.

        A filter against an unstamped corpus returns totalCount 0 with no
        error, which reads exactly like "no such accession". Zero coverage
        is the one case an operator has to be told about -- it is also what
        a standalone re-materialization leaves behind.

        Given:
            A DCC whose files carry no accession at all.
        When:
            _log_accession_coverage runs.
        Then:
            It should warn that accession filters will not match.
        """
        # Arrange
        mock_db.files.docs = [{"_id": "f1", "submission": "hubmap"}]

        # Act
        with caplog.at_level(logging.WARNING):
            await sync_module._log_accession_coverage("hubmap")

        # Assert
        assert "will return no matches" in caplog.text

class TestSyncEncodeIndexes(_EncodeSyncTestBase):
    @pytest.mark.asyncio
    async def test__sync_encode_should_ensure_the_accession_indexes(
        self, mocker, mock_db
    ):
        """Test that an ENCODE-only database is not left unindexed.

        The materializer creates the files indexes at the end of its run,
        and _sync_encode never invokes it -- it writes documents straight
        into files. On a database where ENCODE is the only DCC synced, that
        left files with no index at all, so every accession lookup scanned
        the whole collection on a public endpoint.

        Given:
            An ENCODE sync over a single row.
        When:
            _sync_encode completes.
        Then:
            It should have ensured both accession indexes on files.
        """
        # Arrange
        row = _encode_metadata_row("encff001aaa", "encff001aaa.bed.gz")
        mocker.patch.object(
            encode_module, "fetch_encode_metadata", lambda deadline=None: _async_iter([row])
        )

        # Act
        await _sync_encode(SyncTask(id="t1", dcc_names=["encode"]))

        # Assert
        indexed = {tuple(keys.items()) for keys, _ in mock_db.files._indexes}
        assert (("accession_id", 1),) in indexed
        assert (("collections.accession_id", 1),) in indexed

class TestSetAccessionIds:
    @pytest.mark.asyncio
    async def test__set_accession_ids_should_continue_when_a_batch_fails(
        self, mocker, mock_db, caplog
    ):
        """Test that a failed stamp degrades the field, not the whole sync.

        Stamping is the first write of each enrichment pass, so an escaping
        BulkWriteError would abort the pass before any Search API
        enrichment ran and fail the sync -- costing more than the
        accessions it failed to write. The two are independent by design.

        Given:
            A bulk_write that raises BulkWriteError.
        When:
            _set_accession_ids runs.
        Then:
            It should report zero modifications, log the shortfall at
            ERROR, and return rather than propagate, so the caller's
            enrichment pass still runs.
        """
        # Arrange
        from pymongo.errors import BulkWriteError

        mocker.patch.object(
            mock_db.files,
            "bulk_write",
            mocker.AsyncMock(side_effect=BulkWriteError({"writeErrors": []})),
        )

        # Act
        with caplog.at_level(logging.ERROR):
            modified = await _set_accession_ids(
                mock_db.files, [("f0", "4DNFAAA")], "test"
            )

        # Assert
        assert modified == 0
        assert "not stamped" in caplog.text

    @pytest.mark.asyncio
    async def test__set_accession_ids_should_stamp_every_document(self, mock_db):
        """Test that each pair produces its own stamp.

        Given:
            Three documents, each with its own accession.
        When:
            _set_accession_ids runs.
        Then:
            It should stamp all three and report three modifications.
        """
        # Arrange
        mock_db.files.docs = [{"_id": f"f{i}"} for i in range(3)]
        stamps = [("f0", "4DNFAAA"), ("f1", "4DNFBBB"), ("f2", "4DNFCCC")]

        # Act
        modified = await _set_accession_ids(mock_db.files, stamps, "test")

        # Assert
        assert modified == 3
        assert [d["accession_id"] for d in mock_db.files.docs] == [
            "4DNFAAA",
            "4DNFBBB",
            "4DNFCCC",
        ]

    @pytest.mark.asyncio
    async def test__set_accession_ids_should_stamp_both_documents_sharing_an_accession(
        self, mock_db
    ):
        """Test that a duplicate accession does not cost a document its stamp.

        This is why the helper takes a list of pairs rather than the
        accession-keyed dict the callers also build: that dict is
        last-write-wins, so one of these two documents would be dropped
        before any update was issued, left with a null accession despite
        having parsed cleanly, and which one lost would depend on cursor
        order.

        Given:
            Two distinct documents whose persistent_ids resolve to the same
            accession.
        When:
            _set_accession_ids runs.
        Then:
            It should stamp both.
        """
        # Arrange
        mock_db.files.docs = [{"_id": "f1"}, {"_id": "f2"}]
        stamps = [("f1", "4DNFIMCJXZKH"), ("f2", "4DNFIMCJXZKH")]

        # Act
        modified = await _set_accession_ids(mock_db.files, stamps, "test")

        # Assert
        assert modified == 2
        assert all(d["accession_id"] == "4DNFIMCJXZKH" for d in mock_db.files.docs)

    @pytest.mark.asyncio
    async def test__set_accession_ids_should_fold_the_accession(self, mock_db):
        """Test that the stored form matches what a filter folds to.

        Given:
            A pair whose accession is lower-cased and padded.
        When:
            _set_accession_ids runs.
        Then:
            It should store the stripped, upper-cased form.
        """
        # Arrange
        mock_db.files.docs = [{"_id": "f1"}]

        # Act
        await _set_accession_ids(mock_db.files, [("f1", "  4dnfimcjxzkh ")], "test")

        # Assert
        assert mock_db.files.docs[0]["accession_id"] == "4DNFIMCJXZKH"

    @pytest.mark.asyncio
    async def test__set_accession_ids_should_stamp_across_batch_boundaries(
        self, mocker, mock_db
    ):
        """Test that batching does not drop documents at the seams.

        Given:
            Five documents and a batch size of two, so the final batch is
            partial.
        When:
            _set_accession_ids runs.
        Then:
            It should issue three unordered bulk writes and stamp all five,
            so no document is lost to a boundary.
        """
        # Arrange
        mocker.patch.object(sync_module, "BATCH_SIZE", 2)
        mock_db.files.docs = [{"_id": f"f{i}"} for i in range(5)]
        stamps = [(f"f{i}", f"4DNF{i}") for i in range(5)]
        spy = mocker.spy(mock_db.files, "bulk_write")

        # Act
        modified = await _set_accession_ids(mock_db.files, stamps, "test")

        # Assert
        assert modified == 5
        assert spy.call_count == 3
        assert all(call.kwargs["ordered"] is False for call in spy.call_args_list)

    @pytest.mark.asyncio
    async def test__set_accession_ids_should_do_nothing_when_given_no_pairs(
        self, mock_db, caplog
    ):
        """Test the empty guard.

        Given:
            No pairs at all, as happens when a DCC yields no parseable
            accession.
        When:
            _set_accession_ids runs.
        Then:
            It should report zero, issue no write, and warn under a label
            naming which pass was empty.
        """
        # Arrange
        mock_db.files.docs = [{"_id": "f1"}]

        # Act
        with caplog.at_level(logging.WARNING):
            modified = await _set_accession_ids(mock_db.files, [], "4DN file")

        # Assert
        assert modified == 0
        assert "accession_id" not in mock_db.files.docs[0]
        assert "4DN file" in caplog.text
