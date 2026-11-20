"""Tests for the HuBMAP files search API fetches and transform."""

from __future__ import annotations

import aiohttp
import pytest
from hypothesis import given
from hypothesis import strategies as st

from cfdb.models import FileMetadataModel
from cfdb.services import hubmap as hubmap_module
from cfdb.services.hubmap import (
    build_hubmap_dcc_record,
    derive_file_format,
    extract_genome_assembly,
    fetch_dataset_metadata_bulk,
    fetch_file_metadata,
    fetch_organ_map,
    file_extensions_from_env,
    transform_file_to_cfdb,
)

#: The variable bounding which file extensions are ingested. Named here
#: rather than imported so the tests pin the operator-facing contract:
#: renaming it in the module should fail these tests, not silently follow
#: along.
EXTENSIONS_ENV = "HUBMAP_FILE_EXTENSIONS"


def _file_doc(**overrides) -> dict:
    """Return a minimal but realistic files-index ``_source`` document."""
    doc = {
        "file_uuid": "abc123",
        "rel_path": "processed/secondary_analysis.h5ad",
        "file_extension": ".h5ad",
        "size": 5529765,
        "md5_checksum": "0b05f823fd4b3cc86204366c6f71eae9",
        "sha256_checksum": (
            "f51518c924f3bab02847b19594afced3bc908c9d8be7f58fe36941a5c966ce72"
        ),
        "data_access_level": "public",
        "dataset_uuid": "ds-uuid-1",
        "dataset_hubmap_id": "HBM339.XXWC.842",
        "dataset_type": "RNAseq",
        "dataset_status": "Published",
        "analyte_class": "RNA",
        "donors": [{"entity_type": "Donor", "uuid": "donor-1"}],
        "organs": [{"code": "TH", "label": "Thymus", "uuid": "org-1"}],
    }
    doc.update(overrides)
    return doc


def _transform(src: dict, **overrides) -> dict | None:
    """Run the transform with empty join inputs unless a test supplies them."""
    kwargs = {
        "dataset_metadata": {},
        "donor_lookup": {},
        "organ_map": {},
    }
    kwargs.update(overrides)
    return transform_file_to_cfdb(src, **kwargs)


class _FakeJsonResponse:
    """Async-context-manager stand-in for a JSON aiohttp response."""

    def __init__(self, status: int, payload):
        self.status = status
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._payload


class _SearchSession:
    """Fake aiohttp session serving one Elasticsearch page per ``post``.

    Records every request body so pagination (``search_after``) and the
    server-side extension filter are both observable. Once the configured
    pages run out, an empty page is served, which is how the real index
    ends a ``search_after`` walk.
    """

    def __init__(self, pages=None, status=200, error=None):
        self._pages = list(pages or [])
        self._status = status
        self._error = error
        self.post_bodies: list[dict] = []
        self.post_urls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def post(self, url, json=None, **kwargs):
        self.post_urls.append(url)
        self.post_bodies.append(json)
        if self._error is not None:
            raise self._error
        if self._pages:
            payload = self._pages.pop(0)
        else:
            payload = {"hits": {"hits": []}}
        return _FakeJsonResponse(self._status, payload)


class _OrganSession:
    """Fake aiohttp session for the single UBKG organs ``get``."""

    def __init__(self, payload=None, status=200, error=None):
        self._payload = payload or []
        self._status = status
        self._error = error

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, **kwargs):
        if self._error is not None:
            raise self._error
        return _FakeJsonResponse(self._status, self._payload)


def _page(*sources: dict, sort_key: str = "file_uuid") -> dict:
    """Wrap ``_source`` dicts in the Elasticsearch page envelope."""
    return {
        "hits": {
            "hits": [
                {"_source": src, "sort": [src.get(sort_key, "x")]}
                for src in sources
            ]
        }
    }


class TestFileExtensionsFromEnv:
    def test_file_extensions_from_env_should_return_default_when_unset(
        self, monkeypatch
    ):
        """Test the default allowlist applies when the variable is unset.

        Given:
            No HUBMAP_FILE_EXTENSIONS in the environment.
        When:
            file_extensions_from_env resolves the allowlist.
        Then:
            It should return the non-empty default, dot-prefixed throughout.
        """
        # Arrange
        monkeypatch.delenv(EXTENSIONS_ENV, raising=False)

        # Act
        allowlist = file_extensions_from_env()

        # Assert
        assert allowlist
        assert ".h5ad" in allowlist
        assert all(ext.startswith(".") for ext in allowlist)

    def test_file_extensions_from_env_should_normalize_case_and_missing_dots(
        self, monkeypatch
    ):
        """Test entries normalize to what the index's field values look like.

        The server-side terms filter matches ``file_extension`` exactly, so
        an entry that keeps its case or lacks its dot silently matches
        nothing.

        Given:
            A mixed-case, mixed-dot, whitespace-padded override.
        When:
            file_extensions_from_env resolves the allowlist.
        Then:
            Every entry should come out lower-cased with a leading dot, in
            the configured order.
        """
        # Arrange
        monkeypatch.setenv(EXTENSIONS_ENV, " h5ad, .BCF ,Gz")

        # Act
        allowlist = file_extensions_from_env()

        # Assert
        assert allowlist == (".h5ad", ".bcf", ".gz")

    def test_file_extensions_from_env_should_return_empty_and_warn_when_set_empty(
        self, monkeypatch, caplog
    ):
        """Test an explicitly empty value disables ingest loudly.

        Given:
            HUBMAP_FILE_EXTENSIONS set to a blank string.
        When:
            file_extensions_from_env resolves the allowlist.
        Then:
            It should return an empty tuple and log a warning naming the
            variable.
        """
        # Arrange
        monkeypatch.setenv(EXTENSIONS_ENV, "  ")

        # Act
        with caplog.at_level("WARNING"):
            allowlist = file_extensions_from_env()

        # Assert
        assert allowlist == ()
        assert any(EXTENSIONS_ENV in record.message for record in caplog.records)

    def test_file_extensions_from_env_should_collapse_repeats_keeping_order(
        self, monkeypatch
    ):
        """Test a duplicated token cannot widen or reorder the filter.

        Given:
            An override repeating one extension in two spellings.
        When:
            file_extensions_from_env resolves the allowlist.
        Then:
            The first occurrence should win and order should be preserved.
        """
        # Arrange
        monkeypatch.setenv(EXTENSIONS_ENV, ".h5ad,bcf,.H5AD,.bcf")

        # Act
        allowlist = file_extensions_from_env()

        # Assert
        assert allowlist == (".h5ad", ".bcf")

    @given(
        entries=st.lists(
            st.text(
                alphabet=st.characters(
                    whitelist_categories=("Ll", "Lu", "Nd")
                ),
                min_size=1,
                max_size=8,
            ),
            min_size=1,
            max_size=8,
        )
    )
    def test_file_extensions_from_env_should_emit_index_shaped_entries(
        self, entries
    ):
        """Test every resolved entry matches the index's value shape.

        Given:
            Any comma-joined list of alphanumeric tokens.
        When:
            file_extensions_from_env resolves the allowlist.
        Then:
            Every entry should be lower-cased, dot-prefixed, and unique.
        """
        import os

        # Arrange
        os.environ[EXTENSIONS_ENV] = ",".join(entries)
        try:
            # Act
            allowlist = file_extensions_from_env()
        finally:
            del os.environ[EXTENSIONS_ENV]

        # Assert
        assert all(ext.startswith(".") and ext == ext.lower() for ext in allowlist)
        assert len(set(allowlist)) == len(allowlist)


class TestExtractGenomeAssembly:
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("aligned against hg38 reference", "GRCh38"),
            ("aligned against GRCh38", "GRCh38"),
            ("built on hg19", "GRCh37"),
            ("mouse mm10 pipeline", "GRCm38"),
            ("GRCm39 assembly", "GRCm39"),
            ("no assembly named here", None),
        ],
    )
    def test_extract_genome_assembly_should_normalize_to_canonical_names(
        self, text, expected
    ):
        """Test assembly mentions resolve to their canonical identifiers.

        Given:
            A workflow description naming an assembly in one of the
            recognized spellings, or none at all.
        When:
            extract_genome_assembly scans it.
        Then:
            It should return the canonical assembly name, or None when no
            pattern matches.
        """
        # Act & assert
        assert extract_genome_assembly(text) == expected


class TestDeriveFileFormat:
    @pytest.mark.parametrize(
        ("filename", "expected_name"),
        [
            ("secondary_analysis.h5ad", "HDF5"),
            ("reads.fastq.gz", "FASTQ"),
            ("variants.bcf", "BCF"),
            ("matrix.mtx", "MTX"),
            ("expr.csv", "CSV"),
        ],
    )
    def test_derive_file_format_should_map_the_inner_extension(
        self, filename, expected_name
    ):
        """Test the format comes from the extension under any compression.

        The index's file_extension names the outermost suffix, which for a
        .fastq.gz is the wrapper rather than the format.

        Given:
            A filename, compressed or not.
        When:
            derive_file_format maps it.
        Then:
            It should name the format of the bytes under the compression.
        """
        # Act
        result = derive_file_format(filename)

        # Assert
        assert result is not None
        assert result["name"] == expected_name

    def test_derive_file_format_should_return_none_when_nothing_maps(self):
        """Test an unknown or absent extension maps to no format.

        Given:
            A filename with an unmapped extension and one with none.
        When:
            derive_file_format maps each.
        Then:
            It should return None for both rather than guessing.
        """
        # Act & assert
        assert derive_file_format("notes.unknownext") is None
        assert derive_file_format("README") is None


class TestTransformFileToCfdb:
    def test_transform_file_to_cfdb_should_map_the_file_level_fields(self):
        """Test the per-file surface of the index lands on the document.

        Given:
            A complete files-index document with no dataset join inputs.
        When:
            transform_file_to_cfdb runs.
        Then:
            It should carry the identity, checksum, size, status, format,
            and assets access_url fields the index provides.
        """
        # Act
        doc = _transform(_file_doc())

        # Assert
        assert doc["submission"] == "hubmap"
        assert doc["local_id"] == "abc123"
        assert doc["accession_id"] is None
        assert doc["filename"] == "secondary_analysis.h5ad"
        assert doc["extra"]["hubmap"]["rel_path"] == (
            "processed/secondary_analysis.h5ad"
        )
        assert doc["size_in_bytes"] == 5529765
        assert isinstance(doc["size_in_bytes"], int)
        assert doc["md5"] == "0b05f823fd4b3cc86204366c6f71eae9"
        assert doc["sha256"].startswith("f51518c924f3")
        assert doc["access_url"] == (
            "https://assets.hubmapconsortium.org/ds-uuid-1/"
            "processed/secondary_analysis.h5ad"
        )
        assert doc["status"] == "Published"
        assert doc["data_access_level"] == "public"
        assert doc["file_format"]["name"] == "HDF5"

    def test_transform_file_to_cfdb_should_produce_a_valid_model(self):
        """Test the emitted document validates against the file model.

        Given:
            A complete files-index document with full join inputs.
        When:
            transform_file_to_cfdb runs and the result is validated.
        Then:
            FileMetadataModel should accept it without error.
        """
        # Arrange
        dataset_metadata = {
            "ds-uuid-1": {
                "pipeline": "Salmon",
                "genome_assembly": "GRCh38",
                "donor_uuid": "donor-1",
                "donor_metadata": {"sex": ["Female"]},
            }
        }

        # Act
        doc = _transform(
            _file_doc(),
            dataset_metadata=dataset_metadata,
            donor_lookup={"donor-1": {"sex": ["Female"], "age_value": [67.0]}},
            organ_map={"TH": {"id": "UBERON:0002370", "name": "Thymus"}},
        )

        # Assert
        model = FileMetadataModel(**doc)
        assert model.local_id == "abc123"
        assert model.collections[0].extra.hubmap.dataset_status == "Published"

    def test_transform_file_to_cfdb_should_percent_encode_the_access_url(self):
        """Test a rel_path with URL-hostile characters still forms a URL.

        Given:
            A rel_path containing spaces.
        When:
            transform_file_to_cfdb builds the assets URL.
        Then:
            Path segments should be percent-encoded with the separators
            kept.
        """
        # Act
        doc = _transform(_file_doc(rel_path="a dir/file name.h5ad"))

        # Assert
        assert doc["access_url"] == (
            "https://assets.hubmapconsortium.org/ds-uuid-1/"
            "a%20dir/file%20name.h5ad"
        )

    def test_transform_file_to_cfdb_should_join_dataset_and_donor_metadata(self):
        """Test the in-memory join lands demographics and dataset context.

        Given:
            Dataset metadata and donor demographics keyed for the file's
            dataset and donor.
        When:
            transform_file_to_cfdb runs.
        Then:
            The collection should carry the dataset context and the subject
            the unwrapped demographics.
        """
        # Arrange
        dataset_metadata = {
            "ds-uuid-1": {
                "pipeline": "Salmon",
                "processing": "processed",
                "group_name": "Stanford TMC",
                "visualization": True,
                "vitessce_hints": ["rna"],
                "metadata": {"library_layout": "paired-end"},
                "genome_assembly": "GRCh38",
            }
        }
        donor_lookup = {
            "donor-1": {
                "sex": ["Female"],
                "age_value": [67.0],
                "medical_history": ["Diabetes", "Hypertension"],
            }
        }

        # Act
        doc = _transform(
            _file_doc(),
            dataset_metadata=dataset_metadata,
            donor_lookup=donor_lookup,
        )

        # Assert
        assert doc["genome_assembly"] == "GRCh38"
        assert doc["extra"]["hubmap"]["genome_assembly"] == "GRCh38"
        collection = doc["collections"][0]
        assert collection["local_id"] == "ds-uuid-1"
        assert collection["name"] == "HBM339.XXWC.842"
        assert collection["persistent_id"] == (
            "https://portal.hubmapconsortium.org/browse/dataset/ds-uuid-1"
        )
        assert collection["experiment_type"] == "RNAseq"
        assert collection["analyte_class"] == "RNA"
        assert collection["extra"]["hubmap"] == {
            "pipeline": "Salmon",
            "processing": "processed",
            "group_name": "Stanford TMC",
            "visualization": True,
            "vitessce_hints": ["rna"],
            "metadata": {"library_layout": "paired-end"},
            "dataset_status": "Published",
        }
        subject = collection["subjects"][0]
        assert subject["local_id"] == "donor-1"
        assert subject["taxonomy"]["id"] == "NCBI:txid9606"
        assert subject["extra"]["hubmap"]["sex"] == "Female"
        assert subject["extra"]["hubmap"]["age_value"] == 67.0
        assert subject["extra"]["hubmap"]["medical_history"] == [
            "Diabetes",
            "Hypertension",
        ]

    def test_transform_file_to_cfdb_should_degrade_when_dataset_unknown(self):
        """Test a file whose dataset is missing from the map still loads.

        The dataset map is prefetched from a different index, so a dataset
        the files index knows and the dataset index does not must degrade
        the document rather than drop it.

        Given:
            Empty join inputs.
        When:
            transform_file_to_cfdb runs.
        Then:
            The document should carry the file fields, no genome_assembly,
            a subject without demographics, and a collection without the
            dataset-index extras.
        """
        # Act
        doc = _transform(_file_doc())

        # Assert
        assert "genome_assembly" not in doc
        collection = doc["collections"][0]
        assert collection["extra"]["hubmap"] == {"dataset_status": "Published"}
        assert "extra" not in collection["subjects"][0]

    def test_transform_file_to_cfdb_should_map_organs_through_the_organ_map(
        self,
    ):
        """Test anatomy terms come from the UBKG mapping, never fabricated.

        Given:
            One mappable organ code, one unmapped, and one duplicated, with
            a stats counter attached.
        When:
            transform_file_to_cfdb runs.
        Then:
            The collection should carry exactly one UBERON term named by
            the document's label, and the unmapped code should be counted.
        """
        from collections import Counter

        # Arrange
        organs = [
            {"code": "TH", "label": "Thymus"},
            {"code": "TH", "label": "Thymus"},
            {"code": "ZZ", "label": "Mystery"},
        ]
        stats: Counter[str] = Counter()

        # Act
        doc = transform_file_to_cfdb(
            _file_doc(organs=organs),
            dataset_metadata={},
            donor_lookup={},
            organ_map={"TH": {"id": "UBERON:0002370", "name": "Thymus"}},
            stats=stats,
        )

        # Assert
        assert doc["collections"][0]["anatomy"] == [
            {"id": "UBERON:0002370", "name": "Thymus"}
        ]
        assert stats["unmapped_organ:ZZ"] == 1

    @pytest.mark.parametrize(
        "missing", ["file_uuid", "rel_path", "dataset_uuid"]
    )
    def test_transform_file_to_cfdb_should_return_none_when_unaddressable(
        self, missing
    ):
        """Test a document missing an identity field is skipped.

        Given:
            A files-index document missing one of the fields that make it
            addressable.
        When:
            transform_file_to_cfdb runs with a stats counter.
        Then:
            It should return None and count the skip.
        """
        from collections import Counter

        # Arrange
        stats: Counter[str] = Counter()

        # Act
        doc = transform_file_to_cfdb(
            _file_doc(**{missing: None}),
            dataset_metadata={},
            donor_lookup={},
            organ_map={},
            stats=stats,
        )

        # Assert
        assert doc is None
        assert stats["skipped:incomplete"] == 1

    def test_transform_file_to_cfdb_should_null_an_unparseable_size(self):
        """Test a size the index mistypes degrades to null, not a string.

        Given:
            A document whose size is not a number.
        When:
            transform_file_to_cfdb runs.
        Then:
            size_in_bytes should be None.
        """
        # Act
        doc = _transform(_file_doc(size="not-a-number"))

        # Assert
        assert doc["size_in_bytes"] is None

    def test_transform_file_to_cfdb_should_count_extensions_into_stats(self):
        """Test the transform reports what it emitted for the sync's log.

        Given:
            A document and a stats counter.
        When:
            transform_file_to_cfdb runs.
        Then:
            The counter should tally the document under its extension.
        """
        from collections import Counter

        # Arrange
        stats: Counter[str] = Counter()

        # Act
        transform_file_to_cfdb(
            _file_doc(),
            dataset_metadata={},
            donor_lookup={},
            organ_map={},
            stats=stats,
        )

        # Assert
        assert stats["extension:.h5ad"] == 1


class TestFetchFileMetadata:
    @pytest.mark.asyncio
    async def test_fetch_file_metadata_should_paginate_with_search_after(
        self, mocker
    ):
        """Test the walk carries each page's sort key into the next request.

        Given:
            Two pages of file documents.
        When:
            fetch_file_metadata drains the stream.
        Then:
            It should yield every document and send the second request with
            the first page's last sort value as search_after.
        """
        # Arrange
        session = _SearchSession(
            pages=[
                _page(_file_doc(file_uuid="aaa"), _file_doc(file_uuid="bbb")),
                _page(_file_doc(file_uuid="ccc")),
            ]
        )
        mocker.patch.object(
            hubmap_module.aiohttp, "ClientSession", return_value=session
        )

        # Act
        docs = [doc async for doc in fetch_file_metadata((".h5ad",))]

        # Assert
        assert [doc["file_uuid"] for doc in docs] == ["aaa", "bbb", "ccc"]
        assert "search_after" not in session.post_bodies[0]
        assert session.post_bodies[1]["search_after"] == ["bbb"]

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_should_filter_extensions_server_side(
        self, mocker
    ):
        """Test the allowlist becomes a terms filter in the request body.

        Excluding 9.7M microscopy tiles client-side would stream them all;
        the filter has to ride the query.

        Given:
            A two-extension allowlist.
        When:
            fetch_file_metadata issues its first request.
        Then:
            The body should carry a terms filter on file_extension.keyword
            with exactly those values.
        """
        # Arrange
        session = _SearchSession()
        mocker.patch.object(
            hubmap_module.aiohttp, "ClientSession", return_value=session
        )

        # Act
        [doc async for doc in fetch_file_metadata((".h5ad", ".bcf"))]

        # Assert
        assert session.post_bodies[0]["query"] == {
            "terms": {"file_extension.keyword": [".h5ad", ".bcf"]}
        }

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_should_yield_nothing_when_allowlist_empty(
        self, mocker
    ):
        """Test the explicit-empty configuration never opens a request.

        Given:
            An empty allowlist.
        When:
            fetch_file_metadata is drained.
        Then:
            It should yield nothing and make no network request.
        """
        # Arrange
        session_factory = mocker.patch.object(
            hubmap_module.aiohttp, "ClientSession"
        )

        # Act
        docs = [doc async for doc in fetch_file_metadata(())]

        # Assert
        assert docs == []
        session_factory.assert_not_called()

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_should_raise_on_http_error(self, mocker):
        """Test an upstream error fails the stream instead of truncating it.

        The loader clears before reloading, so a stream that quietly ended
        early would silently shrink the corpus.

        Given:
            A files search endpoint answering HTTP 503.
        When:
            fetch_file_metadata is drained.
        Then:
            It should raise rather than end the stream.
        """
        # Arrange
        session = _SearchSession(status=503)
        mocker.patch.object(
            hubmap_module.aiohttp, "ClientSession", return_value=session
        )

        # Act & assert
        with pytest.raises(RuntimeError, match="HTTP 503"):
            [doc async for doc in fetch_file_metadata((".h5ad",))]


class TestFetchDatasetMetadataBulk:
    @pytest.mark.asyncio
    async def test_fetch_dataset_metadata_bulk_should_key_results_by_uuid(
        self, mocker
    ):
        """Test the map joins against what the files index actually carries.

        The files index names a parent dataset only by dataset_uuid, so a
        map keyed any other way (the retired doi_url keying) cannot be
        joined against it.

        Given:
            One dataset document with donor metadata and a workflow
            description naming hg38.
        When:
            fetch_dataset_metadata_bulk drains the index.
        Then:
            The entry should sit under the dataset uuid with the donor
            fields and the canonicalized assembly.
        """
        # Arrange
        src = {
            "uuid": "ds-uuid-1",
            "pipeline": "Salmon",
            "donor": {"uuid": "donor-1", "mapped_metadata": {"sex": ["F"]}},
            "ingest_metadata": {"workflow_description": "aligned to hg38"},
        }
        session = _SearchSession(pages=[_page(src, sort_key="uuid")])
        mocker.patch.object(
            hubmap_module.aiohttp, "ClientSession", return_value=session
        )

        # Act
        results = await fetch_dataset_metadata_bulk()

        # Assert
        assert set(results) == {"ds-uuid-1"}
        entry = results["ds-uuid-1"]
        assert entry["pipeline"] == "Salmon"
        assert entry["donor_uuid"] == "donor-1"
        assert entry["donor_metadata"] == {"sex": ["F"]}
        assert entry["genome_assembly"] == "GRCh38"

    @pytest.mark.asyncio
    async def test_fetch_dataset_metadata_bulk_should_raise_on_http_error(
        self, mocker
    ):
        """Test a partial dataset map is refused rather than returned.

        A silently partial map would mean silently under-enriched
        documents behind a clear-before-reload.

        Given:
            A dataset search endpoint answering HTTP 502.
        When:
            fetch_dataset_metadata_bulk runs.
        Then:
            It should raise rather than return what it fetched so far.
        """
        # Arrange
        session = _SearchSession(status=502)
        mocker.patch.object(
            hubmap_module.aiohttp, "ClientSession", return_value=session
        )

        # Act & assert
        with pytest.raises(RuntimeError, match="HTTP 502"):
            await fetch_dataset_metadata_bulk()


class TestFetchOrganMap:
    @pytest.mark.asyncio
    async def test_fetch_organ_map_should_key_uberon_terms_by_rui_code(
        self, mocker
    ):
        """Test the UBKG payload becomes a code-keyed UBERON lookup.

        Given:
            A UBKG organs payload with one complete entry and one missing
            its UBERON id.
        When:
            fetch_organ_map runs.
        Then:
            Only the complete entry should be mapped.
        """
        # Arrange
        payload = [
            {"rui_code": "TH", "term": "Thymus", "organ_uberon": "UBERON:0002370"},
            {"rui_code": "XX", "term": "No Uberon", "organ_uberon": None},
        ]
        session = _OrganSession(payload=payload)
        mocker.patch.object(
            hubmap_module.aiohttp, "ClientSession", return_value=session
        )

        # Act
        organ_map = await fetch_organ_map()

        # Assert
        assert organ_map == {"TH": {"id": "UBERON:0002370", "name": "Thymus"}}

    @pytest.mark.asyncio
    async def test_fetch_organ_map_should_return_empty_and_warn_on_failure(
        self, mocker, caplog
    ):
        """Test a UBKG outage degrades anatomy instead of failing the sync.

        Given:
            A UBKG endpoint that raises a client error.
        When:
            fetch_organ_map runs.
        Then:
            It should return an empty map and log a warning.
        """
        # Arrange
        session = _OrganSession(error=aiohttp.ClientError("down"))
        mocker.patch.object(
            hubmap_module.aiohttp, "ClientSession", return_value=session
        )

        # Act
        with caplog.at_level("WARNING"):
            organ_map = await fetch_organ_map()

        # Assert
        assert organ_map == {}
        assert any("UBKG" in record.message for record in caplog.records)


class TestBuildHubmapDccRecord:
    def test_build_hubmap_dcc_record_should_carry_the_static_dcc_fields(self):
        """Test the DCC record names HuBMAP under its registry id.

        Given:
            The static HuBMAP configuration.
        When:
            build_hubmap_dcc_record runs.
        Then:
            The record should carry the submission, registry id, labels,
            and project namespace fields the dcc collection expects.
        """
        # Act
        record = build_hubmap_dcc_record()

        # Assert
        assert record["submission"] == "hubmap"
        assert record["id"] == "cfde_registry_dcc:hubmap"
        assert record["dcc_abbreviation"] == "HuBMAP"
        assert record["dcc_url"] == "https://hubmapconsortium.org"
        assert record["project_id_namespace"] == "https://hubmapconsortium.org"
        assert record["project_local_id"] == "HuBMAP"
