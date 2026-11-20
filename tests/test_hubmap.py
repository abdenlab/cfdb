"""Tests for the HuBMAP files search API fetches and transform."""

from __future__ import annotations

import asyncio
import os
from urllib.parse import unquote, urlparse

import aiohttp
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from cfdb.models import FileMetadataModel
from cfdb.services import hubmap as hubmap_module
from cfdb.services.hubmap import (
    build_donor_lookup,
    build_hubmap_dcc_record,
    derive_file_format,
    extract_genome_assembly,
    fetch_dataset_metadata_bulk,
    fetch_file_metadata,
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
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _SearchSession:
    """Fake aiohttp session serving one Elasticsearch page per ``post``.

    Records every request body so pagination (``search_after``) and the
    server-side extension filter are both observable. Once the configured
    pages run out, an empty page is served, which is how the real index
    ends a ``search_after`` walk.

    ``failures`` are served first, one per request, without consuming a
    page: an int is answered as that HTTP status, an exception is raised,
    and a dict is answered as a 200 carrying that body. That is what lets
    a test fail one attempt at a page and then serve the same page.
    """

    def __init__(self, pages=None, status=200, error=None, failures=None):
        self._pages = list(pages or [])
        self._status = status
        self._error = error
        self._failures = list(failures or [])
        self.post_bodies: list[dict] = []
        self.post_urls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def post(self, url, json=None, **kwargs):
        self.post_urls.append(url)
        self.post_bodies.append(json)
        if self._failures:
            failure = self._failures.pop(0)
            if isinstance(failure, Exception):
                raise failure
            if isinstance(failure, int):
                return _FakeJsonResponse(failure, {})
            return _FakeJsonResponse(200, failure)
        if self._error is not None:
            raise self._error
        if self._pages:
            payload = self._pages.pop(0)
        else:
            payload = {"hits": {"hits": []}}
        return _FakeJsonResponse(self._status, payload)


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


@pytest.fixture
def no_backoff(mocker):
    """Make the fetchers' retry backoff and rate-limit pauses instant.

    Zeroes the loader's own pacing constants rather than stubbing
    ``asyncio.sleep``, which is the shared module attribute every coroutine
    running during the test would see.
    """
    mocker.patch.object(hubmap_module, "RETRY_BACKOFF_SECONDS", 0)
    mocker.patch.object(hubmap_module, "REQUEST_INTERVAL_SECONDS", 0)


def test_file_extensions_from_env_should_return_default_when_unset(
    monkeypatch
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


def test_file_extensions_from_env_should_exclude_bcf_by_default(monkeypatch):
    """Test the default allowlist leaves out HuBMAP's .bcf files.

    Every .bcf in the index is a CODEX raw-acquisition sidecar, not binary
    VCF, so ingesting them would label microscope files as variant calls.

    Given:
        No HUBMAP_FILE_EXTENSIONS in the environment.
    When:
        file_extensions_from_env resolves the allowlist.
    Then:
        It should not include .bcf.
    """
    # Arrange
    monkeypatch.delenv(EXTENSIONS_ENV, raising=False)

    # Act
    allowlist = file_extensions_from_env()

    # Assert
    assert ".bcf" not in allowlist


def test_file_extensions_from_env_should_normalize_case_and_missing_dots(
    monkeypatch
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
    monkeypatch.setenv(EXTENSIONS_ENV, " h5ad, .MTX ,Gz")

    # Act
    allowlist = file_extensions_from_env()

    # Assert
    assert allowlist == (".h5ad", ".mtx", ".gz")


def test_file_extensions_from_env_should_return_empty_and_warn_when_set_empty(
    monkeypatch, caplog
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
    monkeypatch
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
    monkeypatch.setenv(EXTENSIONS_ENV, ".h5ad,mtx,.H5AD,.mtx")

    # Act
    allowlist = file_extensions_from_env()

    # Assert
    assert allowlist == (".h5ad", ".mtx")


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
@settings(
    max_examples=50,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_file_extensions_from_env_should_emit_index_shaped_entries(
    entries, mocker
):
    """Test every resolved entry matches the index's value shape.

    Given:
        Any comma-joined list of alphanumeric tokens.
    When:
        file_extensions_from_env resolves the allowlist.
    Then:
        Every entry should be lower-cased, dot-prefixed, and unique.
    """
    # Arrange
    mocker.patch.dict(os.environ, {EXTENSIONS_ENV: ",".join(entries)})

    # Act
    allowlist = file_extensions_from_env()

    # Assert
    assert all(ext.startswith(".") and ext == ext.lower() for ext in allowlist)
    assert len(set(allowlist)) == len(allowlist)


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
    text, expected
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


@pytest.mark.parametrize(
    ("filename", "expected_name"),
    [
        ("secondary_analysis.h5ad", "HDF5"),
        ("reads.fastq.gz", "FASTQ"),
        ("matrix.mtx", "MTX"),
        ("expr.csv", "CSV"),
    ],
)
def test_derive_file_format_should_map_the_inner_extension(
    filename, expected_name
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


def test_derive_file_format_should_return_none_when_nothing_maps():
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


@given(
    base=st.text(
        alphabet="abcdefghijklmnopqrstuvwxyz0123456789_-", min_size=1, max_size=12
    ),
    extension=st.sampled_from(
        ["h5ad", "h5mu", "hdf5", "csv", "tsv", "mtx", "bed", "bam", "fastq"]
    ),
    compression=st.sampled_from([".gz", ".bz2", ".xz"]),
)
@settings(max_examples=100)
def test_derive_file_format_should_ignore_a_compression_wrapper(
    base, extension, compression
):
    """Test a compression suffix never changes the derived format.

    Given:
        Any plain base name, any mapped extension, and any compression
        suffix the derivation strips.
    When:
        derive_file_format runs on the name with and without the suffix.
    Then:
        Both should map to the same format term.
    """
    # Arrange
    name = f"{base}.{extension}"

    # Act
    plain = derive_file_format(name)
    wrapped = derive_file_format(name + compression)

    # Assert
    assert plain is not None
    assert wrapped == plain


def test_transform_file_to_cfdb_should_map_the_file_level_fields():
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


def test_transform_file_to_cfdb_should_produce_a_valid_model():
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


def test_transform_file_to_cfdb_should_percent_encode_the_access_url():
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


@given(
    dataset_uuid=st.text(alphabet="0123456789abcdef", min_size=8, max_size=32),
    rel_path=st.text(
        alphabet=st.characters(blacklist_categories=("Cs",)),
        min_size=1,
        max_size=40,
    ),
)
@settings(max_examples=100)
def test_transform_file_to_cfdb_should_build_an_access_url_that_round_trips(
    dataset_uuid, rel_path
):
    """Test the assets URL always decodes back to the dataset and path.

    Given:
        Any dataset uuid and any rel_path, including spaces, reserved
        URL characters, and non-ASCII text.
    When:
        transform_file_to_cfdb builds the access_url.
    Then:
        It should point at the assets host, and its unquoted path should
        be exactly the dataset uuid followed by the rel_path.
    """
    # Act
    doc = _transform(_file_doc(dataset_uuid=dataset_uuid, rel_path=rel_path))

    # Assert
    parsed = urlparse(doc["access_url"])
    assert f"{parsed.scheme}://{parsed.netloc}" == hubmap_module.ASSETS_BASE_URL
    assert unquote(parsed.path) == f"/{dataset_uuid}/{rel_path}"
    assert not parsed.query and not parsed.fragment


def test_transform_file_to_cfdb_should_join_dataset_and_donor_metadata():
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


def test_transform_file_to_cfdb_should_degrade_when_dataset_unknown():
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
    ):
    """Test anatomy terms come from the organ map, never fabricated.

    Given:
        One mappable organ code, one unmapped, and one duplicated, with
        a stats counter attached.
    When:
        transform_file_to_cfdb runs.
    Then:
        The collection should carry exactly one UBERON term, named by
        the organ map's term rather than the document's drifting label, and
        the unmapped code should be counted.
    """
    from collections import Counter

    # Arrange
    organs = [
        {"code": "TH", "label": "Thymus (Lobe)"},
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
    missing
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


def test_transform_file_to_cfdb_should_null_an_unparseable_size():
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


def test_transform_file_to_cfdb_should_count_extensions_into_stats():
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


def test_transform_file_to_cfdb_should_name_anatomy_from_the_uberon_table():
    """Test the default organ table supplies UBERON ids and UBERON labels.

    UBKG's display terms ("Kidney (Left)") are not UBERON labels ("left
    kidney"), so naming anatomy by them gave one id different names in
    HuBMAP and in the other DCCs.

    Given:
        A document naming a plain organ code and a lateral one, with no
        organ map supplied.
    When:
        transform_file_to_cfdb runs.
    Then:
        The collection should carry each code's UBERON term from the
        checked-in table, the lateral knee on its side-neutral parent.
    """
    # Arrange
    organs = [
        {"code": "TH", "label": "Thymus"},
        {"code": "RN", "label": "Knee (Right)"},
    ]

    # Act
    doc = transform_file_to_cfdb(
        _file_doc(organs=organs), dataset_metadata={}, donor_lookup={}
    )

    # Assert
    assert doc["collections"][0]["anatomy"] == [
        {"id": "UBERON:0002370", "name": "thymus"},
        {"id": "UBERON:0001465", "name": "knee"},
    ]


def test_transform_file_to_cfdb_should_count_join_misses():
    """Test a dataset or donor absent from the join maps is counted.

    A wholesale join failure would otherwise load every file without its
    dataset context or demographics and look like success.

    Given:
        A document whose dataset is absent from the dataset map and whose
        donor is absent from the donor lookup, with a stats counter.
    When:
        transform_file_to_cfdb runs.
    Then:
        It should still return the document and count one unjoined
        dataset and one unjoined donor.
    """
    from collections import Counter

    # Arrange
    stats: Counter[str] = Counter()

    # Act
    doc = transform_file_to_cfdb(
        _file_doc(), dataset_metadata={}, donor_lookup={}, stats=stats
    )

    # Assert
    assert doc is not None
    assert stats["unjoined_dataset"] == 1
    assert stats["unjoined_donor"] == 1


def test_transform_file_to_cfdb_should_dedupe_repeated_donors():
    """Test a donor the index repeats becomes one subject.

    The live index lists a donor once per contributing tissue block --
    one file names the same uuid 26 times -- so emitting a subject per
    entry would stack identical subjects on one collection.

    Given:
        A document whose donors list repeats one uuid around a second.
    When:
        transform_file_to_cfdb runs.
    Then:
        The collection should carry one subject per distinct donor, in
        first-occurrence order.
    """
    # Arrange
    donors = [
        {"entity_type": "Donor", "uuid": "donor-1"},
        {"entity_type": "Donor", "uuid": "donor-1"},
        {"entity_type": "Donor", "uuid": "donor-2"},
        {"entity_type": "Donor", "uuid": "donor-1"},
    ]

    # Act
    doc = _transform(_file_doc(donors=donors))

    # Assert
    subjects = doc["collections"][0]["subjects"]
    assert [s["local_id"] for s in subjects] == ["donor-1", "donor-2"]


@given(
    uuids=st.lists(
        st.one_of(st.sampled_from(["d1", "d2", "d3"]), st.none(), st.just("")),
        max_size=12,
    )
)
@settings(max_examples=50)
def test_transform_file_to_cfdb_should_emit_each_donor_once(uuids):
    """Test subjects are the distinct donors, whatever the repetition.

    Given:
        Any donors list drawn from a small uuid pool, with repeats and
        blank or missing uuids mixed in.
    When:
        transform_file_to_cfdb runs.
    Then:
        Subject local_ids should equal the distinct non-blank uuids in
        first-occurrence order.
    """
    # Arrange
    donors = [{"entity_type": "Donor", "uuid": uuid} for uuid in uuids]
    expected = list(dict.fromkeys(uuid for uuid in uuids if uuid))

    # Act
    doc = _transform(_file_doc(donors=donors))

    # Assert
    subjects = doc["collections"][0]["subjects"]
    assert [s["local_id"] for s in subjects] == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("True", True),
        ("False", False),
        (" false ", False),
        (True, True),
        (False, False),
    ],
)
def test_transform_file_to_cfdb_should_parse_is_data_product_flags(
    raw, expected
):
    """Test the index's stringly-typed flag lands as a real bool.

    The index serializes is_data_product as "True"/"False", so a
    truthiness test would read "False" as true.

    Given:
        A document whose is_data_product is one of the recognized
        spellings.
    When:
        transform_file_to_cfdb runs.
    Then:
        extra.hubmap.is_data_product should be the matching bool.
    """
    # Act
    doc = _transform(_file_doc(is_data_product=raw))

    # Assert
    assert doc["extra"]["hubmap"]["is_data_product"] is expected


@pytest.mark.parametrize("raw", [None, "yes", "", 1])
def test_transform_file_to_cfdb_should_omit_is_data_product_when_unrecognized(
    raw
):
    """Test an unrecognized flag reads as unknown, not as false.

    Given:
        A document whose is_data_product is missing or neither
        recognized spelling.
    When:
        transform_file_to_cfdb runs.
    Then:
        extra.hubmap should carry no is_data_product key at all.
    """
    # Act
    doc = _transform(_file_doc(is_data_product=raw))

    # Assert
    assert "is_data_product" not in doc["extra"]["hubmap"]


def test_transform_file_to_cfdb_should_omit_is_data_product_when_absent():
    """Test a document without the flag stays unknown.

    A third of the live allowlisted files carry no is_data_product, and
    defaulting them to false would fabricate a fact.

    Given:
        A document with no is_data_product key.
    When:
        transform_file_to_cfdb runs.
    Then:
        extra.hubmap should carry no is_data_product key.
    """
    # Arrange
    src = _file_doc()
    src.pop("is_data_product", None)

    # Act
    doc = _transform(src)

    # Assert
    assert "is_data_product" not in doc["extra"]["hubmap"]


@pytest.mark.asyncio
async def test_fetch_file_metadata_should_paginate_with_search_after(
    mocker
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
    mocker
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
    [doc async for doc in fetch_file_metadata((".h5ad", ".mtx"))]

    # Assert
    filters = session.post_bodies[0]["query"]["bool"]["filter"]
    assert {"terms": {"file_extension.keyword": [".h5ad", ".mtx"]}} in filters


@pytest.mark.asyncio
async def test_fetch_file_metadata_should_yield_nothing_when_allowlist_empty(
    mocker
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
async def test_fetch_file_metadata_should_raise_on_http_error(mocker, no_backoff):
    """Test an upstream error fails the stream instead of truncating it.

    The loader clears before reloading, so a stream that quietly ended
    early would silently shrink the corpus.

    Given:
        A files search endpoint answering HTTP 503.
    When:
        fetch_file_metadata is drained.
    Then:
        It should retry the page, then raise rather than end the
        stream.
    """
    # Arrange
    session = _SearchSession(status=503)
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    with pytest.raises(RuntimeError, match="HTTP 503"):
        [doc async for doc in fetch_file_metadata((".h5ad",))]

    # Assert
    assert len(session.post_bodies) == 3


@pytest.mark.asyncio
async def test_fetch_file_metadata_should_request_is_data_product(
    mocker
):
    """Test the requested source fields include the data-product flag.

    A field missing from _source never crosses the wire, so the
    transform would see every file as flagless.

    Given:
        An allowlist.
    When:
        fetch_file_metadata issues its first request.
    Then:
        The body's _source should name is_data_product.
    """
    # Arrange
    session = _SearchSession()
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    [doc async for doc in fetch_file_metadata((".h5ad",))]

    # Assert
    assert "is_data_product" in session.post_bodies[0]["_source"]


@pytest.mark.asyncio
async def test_fetch_file_metadata_should_raise_on_network_error(mocker, no_backoff):
    """Test a dropped connection fails the stream instead of ending it.

    Given:
        A files search endpoint whose request raises a client error.
    When:
        fetch_file_metadata is drained.
    Then:
        The client error should propagate once the page's retries
        are spent.
    """
    # Arrange
    session = _SearchSession(error=aiohttp.ClientError("reset"))
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    with pytest.raises(aiohttp.ClientError, match="reset"):
        [doc async for doc in fetch_file_metadata((".h5ad",))]

    # Assert
    assert len(session.post_bodies) == 3


@pytest.mark.asyncio
async def test_fetch_file_metadata_should_stop_when_a_page_has_no_sort(
    mocker
):
    """Test a page without a sort cursor ends the walk.

    Without a cursor there is nothing to resume from; re-sending the
    query would re-serve the same page forever.

    Given:
        A page whose last hit carries no sort value.
    When:
        fetch_file_metadata is drained.
    Then:
        It should yield that page and issue no second request.
    """
    # Arrange
    session = _SearchSession(
        pages=[{"hits": {"hits": [{"_source": _file_doc()}]}}]
    )
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    docs = [doc async for doc in fetch_file_metadata((".h5ad",))]

    # Assert
    assert len(docs) == 1
    assert len(session.post_bodies) == 1


@pytest.mark.asyncio
async def test_fetch_file_metadata_should_filter_to_public_files(mocker):
    """Test the query admits only public files.

    The retired pruning pass relied on the index holding only public
    files; the filter makes that an invariant rather than an assumption.

    Given:
        An allowlist.
    When:
        fetch_file_metadata issues its first request.
    Then:
        The body should filter on data_access_level public.
    """
    # Arrange
    session = _SearchSession()
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    [doc async for doc in fetch_file_metadata((".h5ad",))]

    # Assert
    filters = session.post_bodies[0]["query"]["bool"]["filter"]
    assert {"term": {"data_access_level.keyword": "public"}} in filters


@pytest.mark.asyncio
async def test_fetch_file_metadata_should_retry_a_page_that_failed_transiently(
    mocker, no_backoff
):
    """Test one transient failure costs a retry, not the sync.

    Given:
        A files endpoint that answers HTTP 503 once and then serves a page
        of two documents.
    When:
        fetch_file_metadata is drained.
    Then:
        It should yield both documents, having re-sent the identical
        first-page request.
    """
    # Arrange
    session = _SearchSession(
        pages=[_page(_file_doc(file_uuid="aaa"), _file_doc(file_uuid="bbb"))],
        failures=[503],
    )
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    docs = [doc async for doc in fetch_file_metadata((".h5ad",))]

    # Assert
    assert [doc["file_uuid"] for doc in docs] == ["aaa", "bbb"]
    assert session.post_bodies[0] == session.post_bodies[1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "message"),
    [
        (
            {"_shards": {"failed": 1}, "hits": {"hits": []}},
            "failed shard",
        ),
        ({"timed_out": True, "hits": {"hits": []}}, "timed out upstream"),
        ({}, "no hits envelope"),
    ],
)
async def test_fetch_file_metadata_should_raise_when_every_attempt_is_partial(
    mocker, no_backoff, failure, message
):
    """Test a partial or malformed 200 is refused rather than walked past.

    Elasticsearch answers 200 with whatever healthy shards returned, and a
    body with no hits envelope would read as end-of-stream -- on the first
    page, a cleared corpus with nothing loaded.

    Given:
        A files endpoint whose every answer is a failed-shard, timed-out,
        or envelope-less 200.
    When:
        fetch_file_metadata is drained.
    Then:
        It should raise naming the defect, after retrying the page.
    """
    # Arrange
    session = _SearchSession(failures=[failure] * 3)
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    with pytest.raises(RuntimeError, match=message):
        [doc async for doc in fetch_file_metadata((".h5ad",))]

    # Assert
    assert len(session.post_bodies) == 3


@pytest.mark.asyncio
async def test_fetch_file_metadata_should_raise_at_once_on_a_client_error_status(
    mocker, no_backoff
):
    """Test a request the server refuses outright is not retried.

    Given:
        A files endpoint answering HTTP 400.
    When:
        fetch_file_metadata is drained.
    Then:
        It should raise after a single request.
    """
    # Arrange
    session = _SearchSession(failures=[400])
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    with pytest.raises(RuntimeError, match="HTTP 400"):
        [doc async for doc in fetch_file_metadata((".h5ad",))]

    # Assert
    assert len(session.post_bodies) == 1


@pytest.mark.asyncio
async def test_fetch_file_metadata_should_raise_when_the_walk_ends_short(mocker):
    """Test a walk that delivers less than the index reported is refused.

    Given:
        A first page reporting an exact total of 3 but carrying 2 hits,
        after which the index serves nothing more.
    When:
        fetch_file_metadata is drained.
    Then:
        It should yield the 2 documents, then raise naming the shortfall,
        having asked for an exact total on the first page.
    """
    # Arrange
    first = {
        "hits": {
            "total": {"value": 3, "relation": "eq"},
            "hits": [
                {"_source": _file_doc(file_uuid="aaa"), "sort": ["aaa"]},
                {"_source": _file_doc(file_uuid="bbb"), "sort": ["bbb"]},
            ],
        }
    }
    session = _SearchSession(pages=[first])
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )
    docs = []

    # Act
    with pytest.raises(RuntimeError, match="2 of the 3"):
        async for doc in fetch_file_metadata((".h5ad",)):
            docs.append(doc)

    # Assert
    assert [doc["file_uuid"] for doc in docs] == ["aaa", "bbb"]
    assert session.post_bodies[0]["track_total_hits"] is True


@pytest.mark.asyncio
async def test_fetch_file_metadata_should_raise_when_the_deadline_has_passed(
    mocker,
):
    """Test a spent budget is refused rather than turned into no timeout.

    aiohttp reads a non-positive total as "no timeout", so passing the
    exhausted remainder through would make the request unbounded.

    Given:
        A deadline already behind the event-loop clock.
    When:
        fetch_file_metadata is drained.
    Then:
        It should raise a timeout without issuing a request.
    """
    # Arrange
    session = _SearchSession(pages=[_page(_file_doc())])
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )
    deadline = asyncio.get_running_loop().time() - 1

    # Act
    with pytest.raises(asyncio.TimeoutError):
        [
            doc
            async for doc in fetch_file_metadata((".h5ad",), deadline=deadline)
        ]

    # Assert
    assert session.post_bodies == []


@pytest.mark.asyncio
async def test_fetch_dataset_metadata_bulk_should_key_results_by_uuid(
    mocker
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
async def test_fetch_dataset_metadata_bulk_should_raise_on_http_error(mocker, no_backoff):
    """Test a partial dataset map is refused rather than returned.

    A silently partial map would mean silently under-enriched
    documents behind a clear-before-reload.

    Given:
        A dataset search endpoint answering HTTP 502.
    When:
        fetch_dataset_metadata_bulk runs.
    Then:
        It should retry the page, then raise rather than return what
        it fetched so far.
    """
    # Arrange
    session = _SearchSession(status=502)
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    with pytest.raises(RuntimeError, match="HTTP 502"):
        await fetch_dataset_metadata_bulk()

    # Assert
    assert len(session.post_bodies) == 3


@pytest.mark.asyncio
async def test_fetch_dataset_metadata_bulk_should_skip_hits_without_uuid(
    mocker
):
    """Test unkeyable datasets are dropped and the rest keep their context.

    Given:
        One dataset hit with no uuid and one carrying vitessce-hints
        and assay metadata.
    When:
        fetch_dataset_metadata_bulk drains the index.
    Then:
        Only the keyed dataset should be returned, with vitessce-hints
        under vitessce_hints and the metadata dict intact.
    """
    # Arrange
    keyed = {
        "uuid": "ds-uuid-1",
        "vitessce-hints": ["is_sc", "rna"],
        "metadata": {"assay_category": "sequence"},
    }
    session = _SearchSession(
        pages=[_page({"pipeline": "Salmon"}, keyed, sort_key="uuid")]
    )
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    results = await fetch_dataset_metadata_bulk()

    # Assert
    assert set(results) == {"ds-uuid-1"}
    assert results["ds-uuid-1"]["vitessce_hints"] == ["is_sc", "rna"]
    assert results["ds-uuid-1"]["metadata"] == {"assay_category": "sequence"}


@pytest.mark.asyncio
async def test_fetch_dataset_metadata_bulk_should_raise_on_network_error(mocker, no_backoff):
    """Test a dropped connection refuses the partial map.

    Given:
        A dataset search endpoint whose request raises a client error.
    When:
        fetch_dataset_metadata_bulk runs.
    Then:
        The client error should propagate once the page's retries
        are spent.
    """
    # Arrange
    session = _SearchSession(error=aiohttp.ClientError("reset"))
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    with pytest.raises(aiohttp.ClientError, match="reset"):
        await fetch_dataset_metadata_bulk()

    # Assert
    assert len(session.post_bodies) == 3


@pytest.mark.asyncio
async def test_fetch_dataset_metadata_bulk_should_stop_when_a_page_has_no_sort(
    mocker
):
    """Test a page without a sort cursor ends the walk.

    Given:
        A page whose last hit carries no sort value.
    When:
        fetch_dataset_metadata_bulk runs.
    Then:
        It should return that page's dataset after a single request.
    """
    # Arrange
    session = _SearchSession(
        pages=[{"hits": {"hits": [{"_source": {"uuid": "ds-uuid-1"}}]}}]
    )
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    results = await fetch_dataset_metadata_bulk()

    # Assert
    assert set(results) == {"ds-uuid-1"}
    assert len(session.post_bodies) == 1


@pytest.mark.asyncio
async def test_fetch_dataset_metadata_bulk_should_collect_every_listed_donor(
    mocker,
):
    """Test a pooled dataset's donors are all carried, not just the first.

    Given:
        A dataset whose singular donor is one of the three it lists under
        donors, each with demographics.
    When:
        fetch_dataset_metadata_bulk drains the index.
    Then:
        The entry's donors map should hold all three donors' demographics.
    """
    # Arrange
    src = {
        "uuid": "ds-uuid-1",
        "donor": {"uuid": "d1", "mapped_metadata": {"sex": ["F"]}},
        "donors": [
            {"uuid": "d1", "mapped_metadata": {"sex": ["F"]}},
            {"uuid": "d2", "mapped_metadata": {"sex": ["M"]}},
            {"uuid": "d3", "mapped_metadata": {"sex": ["F"]}},
        ],
    }
    session = _SearchSession(pages=[_page(src, sort_key="uuid")])
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    results = await fetch_dataset_metadata_bulk()

    # Assert
    assert results["ds-uuid-1"]["donors"] == {
        "d1": {"sex": ["F"]},
        "d2": {"sex": ["M"]},
        "d3": {"sex": ["F"]},
    }


@pytest.mark.asyncio
async def test_fetch_dataset_metadata_bulk_should_raise_when_the_deadline_has_passed(
    mocker,
):
    """Test the dataset prefetch honors the sync's shared deadline.

    Given:
        A deadline already behind the event-loop clock.
    When:
        fetch_dataset_metadata_bulk runs.
    Then:
        It should raise a timeout without issuing a request.
    """
    # Arrange
    session = _SearchSession(pages=[_page({"uuid": "ds-uuid-1"}, sort_key="uuid")])
    mocker.patch.object(
        hubmap_module.aiohttp, "ClientSession", return_value=session
    )
    deadline = asyncio.get_running_loop().time() - 1

    # Act
    with pytest.raises(asyncio.TimeoutError):
        await fetch_dataset_metadata_bulk(deadline=deadline)

    # Assert
    assert session.post_bodies == []


def test_build_donor_lookup_should_merge_singular_and_plural_donors():
    """Test every donor any dataset names joins, the first naming winning.

    Given:
        One dataset naming a donor only under donors, another naming a
        donor only singly, and a third repeating the first donor with
        different demographics.
    When:
        build_donor_lookup runs.
    Then:
        All three donors should be present, the repeated one keeping the
        demographics of the first dataset that named it.
    """
    # Arrange
    dataset_metadata = {
        "ds1": {"donors": {"d1": {"sex": ["F"]}}},
        "ds2": {"donor_uuid": "d2", "donor_metadata": {"sex": ["M"]}},
        "ds3": {"donors": {"d1": {"sex": ["M"]}}},
    }

    # Act
    lookup = build_donor_lookup(dataset_metadata)

    # Assert
    assert lookup == {"d1": {"sex": ["F"]}, "d2": {"sex": ["M"]}}


def test_build_hubmap_dcc_record_should_carry_the_static_dcc_fields():
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
