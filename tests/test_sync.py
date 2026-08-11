"""Tests for sync service pruning and enrichment changes."""

from __future__ import annotations

import asyncio
import logging

import pytest

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
            encode_module, "fetch_encode_metadata", lambda: _async_iter([row])
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
