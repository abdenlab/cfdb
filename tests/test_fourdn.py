"""Tests for 4DN enrichment helpers."""

from __future__ import annotations

import asyncio
import math
import re

import aiohttp
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from cfdb.models import NUMERIC_PROTOCOL_FIELDS, EnrichedFourdnCollection
from cfdb.services import fourdn
from cfdb.services.fourdn import parse_experiment_metadata, parse_extra_files


def test_parse_extra_files_should_store_token_when_file_format_is_cv_object():
    """Test that a CV-object file_format is stored as its token.

    Given:
        A raw 4DN extra_files entry whose file_format is an embedded CV
        object carrying the token under display_title.
    When:
        parse_extra_files processes it.
    Then:
        The stored file_format should be the display_title string.
    """
    # Arrange
    raw = [
        {
            "href": "/files/x.pairs_px2",
            "file_format": {
                "principals_allowed": {"view": ["system.Everyone"]},
                "status": "released",
                "display_title": "pairs_px2",
            },
        }
    ]

    # Act
    result = parse_extra_files(raw)

    # Assert
    assert result == [{"href": "/files/x.pairs_px2", "file_format": "pairs_px2"}]


def test_parse_extra_files_should_preserve_string_file_format_and_other_fields():
    """Test that a string file_format and the other fields carry through.

    Given:
        A raw entry with a bare-string file_format plus href, md5sum, and
        file_size.
    When:
        parse_extra_files processes it.
    Then:
        It should preserve every field unchanged.
    """
    # Arrange
    raw = [
        {
            "href": "/files/x.bai",
            "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
            "file_size": 1024,
            "file_format": "bai",
        }
    ]

    # Act
    result = parse_extra_files(raw)

    # Assert
    assert result == [
        {
            "href": "/files/x.bai",
            "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
            "file_size": 1024,
            "file_format": "bai",
        }
    ]


def test_parse_extra_files_should_return_empty_list_when_input_empty():
    """Test that an empty extra_files list yields an empty list.

    Given:
        An empty list of raw extra_files entries.
    When:
        parse_extra_files is called.
    Then:
        It should return an empty list.
    """
    # Act
    result = parse_extra_files([])

    # Assert
    assert result == []


def test_parse_extra_files_should_drop_entry_when_it_yields_no_fields():
    """Test that an entry with no usable fields is dropped.

    Given:
        A raw extra_files entry whose only key is a dict file_format with
        no display_title (so it normalizes away) alongside an entry with
        usable fields.
    When:
        parse_extra_files processes the list.
    Then:
        It should drop the empty entry and keep the usable one.
    """
    # Arrange
    raw = [
        {"file_format": {"status": "released"}},
        {"href": "/files/x.bai", "file_format": "bai"},
    ]

    # Act
    result = parse_extra_files(raw)

    # Assert
    assert result == [{"href": "/files/x.bai", "file_format": "bai"}]


def test_parse_experiment_metadata_should_stringify_numeric_protocol_fields():
    """Test that numeric protocol fields are stored as strings.

    Given:
        A raw 4DN experiment item whose protocol fields are JSON numbers
        (the shape that crashed the files query).
    When:
        parse_experiment_metadata processes it.
    Then:
        Each numeric protocol field should be stored as its string form so
        persisted documents match the EnrichedFourdnCollection type.
    """
    # Arrange
    item = {
        "accession": "4DNEXTEST001",
        "crosslinking_temperature": 25.0,
        "crosslinking_time": 10.0,
        "ligation_temperature": 25.0,
        "ligation_volume": 0.12,
        "ligation_time": 360.0,
        "digestion_temperature": 37.0,
        "digestion_time": 960.0,
        "average_fragment_size": 300.0,
    }

    # Act
    result = parse_experiment_metadata(item)

    # Assert
    assert result == {
        "crosslinking_temperature": "25.0",
        "crosslinking_time": "10.0",
        "ligation_temperature": "25.0",
        "ligation_volume": "0.12",
        "ligation_time": "360.0",
        "digestion_temperature": "37.0",
        "digestion_time": "960.0",
        "average_fragment_size": "300.0",
    }


def test_parse_experiment_metadata_should_preserve_non_numeric_scalar_fields():
    """Test that non-numeric scalar protocol fields carry through unchanged.

    Given:
        A raw 4DN experiment item with string scalar protocol fields and a
        display_title.
    When:
        parse_experiment_metadata processes it.
    Then:
        It should preserve those string fields verbatim.
    """
    # Arrange
    item = {
        "accession": "4DNEXTEST002",
        "display_title": "in situ Hi-C on GM12878",
        "crosslinking_method": "1% Formaldehyde",
        "library_prep_kit": "NEBNext",
    }

    # Act
    result = parse_experiment_metadata(item)

    # Assert
    assert result == {
        "display_title": "in situ Hi-C on GM12878",
        "crosslinking_method": "1% Formaldehyde",
        "library_prep_kit": "NEBNext",
    }


def test_parse_experiment_metadata_should_extract_display_title_object_fields():
    """Test that object fields are reduced to their display_title token.

    Given:
        A raw 4DN experiment item whose experiment_type and digestion_enzyme
        are CV objects and whose targeted_factor is a list of BioFeature
        objects.
    When:
        parse_experiment_metadata processes it.
    Then:
        It should extract the display_title token from each object field and
        collect the targeted_factor titles into a list.
    """
    # Arrange
    item = {
        "accession": "4DNEXTEST003",
        "experiment_type": {"display_title": "in situ Hi-C"},
        "digestion_enzyme": {"display_title": "DpnII"},
        "targeted_factor": [{"display_title": "CTCF protein"}],
    }

    # Act
    result = parse_experiment_metadata(item)

    # Assert
    assert result == {
        "experiment_type": "in situ Hi-C",
        "digestion_enzyme": "DpnII",
        "targeted_factor": ["CTCF protein"],
    }


def test_parse_experiment_metadata_should_drop_empty_and_missing_fields():
    """Test that absent or empty fields are dropped from the entry.

    Given:
        A raw 4DN experiment item carrying only an accession and an
        empty-string protocol field.
    When:
        parse_experiment_metadata processes it.
    Then:
        It should return an empty entry — absent fields and empty strings
        are not stored.
    """
    # Arrange
    item = {"accession": "4DNEXTEST004", "crosslinking_method": ""}

    # Act
    result = parse_experiment_metadata(item)

    # Assert
    assert result == {}


def test_parse_experiment_metadata_should_keep_falsy_zero_numeric_field():
    """Test that a zero-valued numeric protocol field is kept and stringified.

    Given:
        A raw 4DN experiment item whose numeric protocol fields are 0 and
        0.0 — falsy but valid measurements that the guard (`!= ""`, not
        truthiness) must keep.
    When:
        parse_experiment_metadata processes it.
    Then:
        It should keep both fields, stringified to "0" and "0.0".
    """
    # Arrange
    item = {
        "accession": "4DNEXTEST005",
        "crosslinking_temperature": 0,
        "ligation_volume": 0.0,
    }

    # Act
    result = parse_experiment_metadata(item)

    # Assert
    assert result == {
        "crosslinking_temperature": "0",
        "ligation_volume": "0.0",
    }


def test_parse_experiment_metadata_should_store_non_numeric_number_field_raw():
    """Test that a number on a non-coerced scalar field is stored unchanged.

    Given:
        A raw 4DN experiment item whose crosslinking_method (outside the
        eight numeric protocol fields) carries a number.
    When:
        parse_experiment_metadata processes it.
    Then:
        It should store the raw number — only the eight numeric fields are
        coerced, mirroring the read-side validator scope.
    """
    # Arrange
    item = {"accession": "4DNEXTEST006", "crosslinking_method": 5}

    # Act
    result = parse_experiment_metadata(item)

    # Assert
    assert result == {"crosslinking_method": 5}


def test_parse_experiment_metadata_should_stringify_int_numeric_field():
    """Test that an int numeric protocol field is stored as a string.

    Given:
        A raw 4DN experiment item whose digestion_time is a bare int (960).
    When:
        parse_experiment_metadata processes it.
    Then:
        It should store digestion_time as the string "960".
    """
    # Arrange
    item = {"accession": "4DNEXTEST007", "digestion_time": 960}

    # Act
    result = parse_experiment_metadata(item)

    # Assert
    assert result == {"digestion_time": "960"}


def test_parse_experiment_metadata_should_drop_none_numeric_field():
    """Test that a None numeric protocol field is dropped, not stringified.

    Given:
        A raw 4DN experiment item whose crosslinking_temperature is None.
    When:
        parse_experiment_metadata processes it.
    Then:
        It should drop the field entirely rather than store the string
        "None".
    """
    # Arrange
    item = {"accession": "4DNEXTEST008", "crosslinking_temperature": None}

    # Act
    result = parse_experiment_metadata(item)

    # Assert
    assert result == {}


def test_parse_experiment_metadata_should_omit_targeted_factor_for_empty_list():
    """Test that an empty targeted_factor list omits the key.

    Given:
        A raw 4DN experiment item whose targeted_factor is an empty list.
    When:
        parse_experiment_metadata processes it.
    Then:
        The targeted_factor key should be omitted from the entry.
    """
    # Arrange
    item = {"accession": "4DNEXTEST009", "targeted_factor": []}

    # Act
    result = parse_experiment_metadata(item)

    # Assert
    assert result == {}


def test_parse_experiment_metadata_should_omit_targeted_factor_without_titles():
    """Test that a title-less targeted_factor list omits the key.

    Given:
        A raw 4DN experiment item whose targeted_factor entries carry no
        display_title.
    When:
        parse_experiment_metadata processes it.
    Then:
        The targeted_factor key should be omitted from the entry.
    """
    # Arrange
    item = {"accession": "4DNEXTEST010", "targeted_factor": [{"status": "released"}]}

    # Act
    result = parse_experiment_metadata(item)

    # Assert
    assert result == {}


def test_parse_experiment_metadata_should_extract_lab_display_title_token():
    """Test that the lab object field is reduced to its display_title token.

    Given:
        A raw 4DN experiment item whose lab is a CV object carrying the name
        under display_title.
    When:
        parse_experiment_metadata processes it.
    Then:
        It should extract the lab display_title token as a string.
    """
    # Arrange
    item = {"accession": "4DNEXTEST011", "lab": {"display_title": "Smith Lab"}}

    # Act
    result = parse_experiment_metadata(item)

    # Assert
    assert result == {"lab": "Smith Lab"}


def test_parse_experiment_metadata_should_omit_object_field_without_display_title():
    """Test that an object field lacking display_title is omitted.

    Given:
        A raw 4DN experiment item whose experiment_type object has no
        display_title key.
    When:
        parse_experiment_metadata processes it.
    Then:
        The experiment_type key should be omitted from the entry.
    """
    # Arrange
    item = {"accession": "4DNEXTEST012", "experiment_type": {"status": "released"}}

    # Act
    result = parse_experiment_metadata(item)

    # Assert
    assert result == {}


def test_parse_experiment_metadata_feeds_enriched_fourdn_collection_cleanly():
    """Test write→read parity: parsed floats reconstruct the model cleanly.

    Given:
        A raw 4DN experiment item with float protocol fields.
    When:
        EnrichedFourdnCollection is constructed from
        parse_experiment_metadata(item) (the dual-fix DRY contract).
    Then:
        It should construct without error and each protocol value should
        match the stringified parse output.
    """
    # Arrange
    item = {
        "accession": "4DNEXTEST013",
        "crosslinking_temperature": 25.0,
        "ligation_volume": 0.12,
        "digestion_time": 960.0,
    }

    # Act
    parsed = parse_experiment_metadata(item)
    result = EnrichedFourdnCollection(**parsed)

    # Assert
    assert result.crosslinking_temperature == parsed["crosslinking_temperature"]
    assert result.ligation_volume == parsed["ligation_volume"]
    assert result.digestion_time == parsed["digestion_time"]
    assert result.crosslinking_temperature == "25.0"
    assert result.ligation_volume == "0.12"
    assert result.digestion_time == "960.0"


class TestParseExperimentMetadata:
    @pytest.mark.parametrize("field_name", NUMERIC_PROTOCOL_FIELDS)
    @given(value=st.integers() | st.floats(allow_nan=False, allow_infinity=False))
    def test_pbt_001_each_numeric_field_stringified_in_output(
        self, field_name, value
    ):
        """Test that each of the eight fields is stringified on the write path.

        Given:
            Each of the eight numeric protocol fields, and any int or finite
            float value.
        When:
            parse_experiment_metadata processes an item carrying that field.
        Then:
            The output should carry the field as str(value).
        """
        # Arrange
        item = {"accession": "4DNEXPBT", field_name: value}

        # Act
        result = parse_experiment_metadata(item)

        # Assert
        assert result[field_name] == str(value)


class _FakeResponse:
    """Async-context-manager stand-in for an aiohttp response."""

    def __init__(self, status: int, payload: dict):
        self.status = status
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._payload


class _EchoSession:
    """Fake aiohttp session that records request URLs and echoes an ``@graph``
    for whatever accessions each URL asks for, drawing field values from
    ``field_map``. ``failures`` maps a zero-based call index to an exception to
    raise or a non-200 status to return, modelling a transient per-batch failure.
    """

    def __init__(self, field_map=None, failures=None):
        self._field_map = field_map or {}
        self._failures = failures or {}
        self.get_urls: list[str] = []
        self._calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, **kwargs):
        index = self._calls
        self._calls += 1
        self.get_urls.append(url)
        failure = self._failures.get(index)
        if isinstance(failure, Exception):
            raise failure
        status = failure if isinstance(failure, int) else 200
        accessions = _accession_params(url)
        graph = (
            [{"accession": acc, **self._field_map.get(acc, {})} for acc in accessions]
            if status == 200
            else []
        )
        return _FakeResponse(status, {"@graph": graph, "total": len(graph)})


def _accession_params(url: str) -> list[str]:
    """Return the accession filter values in a Search-API request URL."""
    return re.findall(r"[?&]accession=([^&]+)", url)


class _FixedGraphSession:
    """Fake aiohttp session that returns a fixed ``@graph`` for every request,
    independent of the requested accessions. Lets a test drive exact item shapes
    (an item missing ``accession``, an item with no mappable fields, a raw
    ``extra_files`` payload). ``failures`` maps a zero-based call index to an
    exception to raise or a non-200 status to return.
    """

    def __init__(self, graph, failures=None):
        self._graph = graph
        self._failures = failures or {}
        self.get_urls: list[str] = []
        self._calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, **kwargs):
        index = self._calls
        self._calls += 1
        self.get_urls.append(url)
        failure = self._failures.get(index)
        if isinstance(failure, Exception):
            raise failure
        status = failure if isinstance(failure, int) else 200
        graph = self._graph if status == 200 else []
        return _FakeResponse(status, {"@graph": graph, "total": len(graph)})


class TestFetchFileMetadataBulk:
    """Tests for fetch_file_metadata_bulk."""

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_batch_accessions_into_bounded_queries(
        self, mocker
    ):
        """Test that accessions are queried in bounded batches.

        Given:
            More accessions than the per-request batch size.
        When:
            fetch_file_metadata_bulk requests their metadata.
        Then:
            It should issue one accession-filtered query per batch, each within
            the batch size and without a deep-pagination ``from`` offset, together
            covering every accession exactly once.
        """
        # Arrange
        mocker.patch.object(fourdn, "_FILE_METADATA_BATCH_SIZE", 2)
        session = _EchoSession()
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)
        accessions = [f"4DNF{i:07d}" for i in range(5)]

        # Act
        await fourdn.fetch_file_metadata_bulk(accessions)

        # Assert
        assert len(session.get_urls) == 3  # ceil(5 / 2)
        assert all("from=" not in url for url in session.get_urls)
        assert all(0 < len(_accession_params(url)) <= 2 for url in session.get_urls)
        requested = [acc for url in session.get_urls for acc in _accession_params(url)]
        assert sorted(requested) == accessions

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_aggregate_entries_across_batches(
        self, mocker
    ):
        """Test that entries from every batch are aggregated.

        Given:
            Accessions spanning multiple request batches, each with metadata
            upstream.
        When:
            fetch_file_metadata_bulk requests their metadata.
        Then:
            It should return an entry for every accession, not just the first
            batch's.
        """
        # Arrange
        mocker.patch.object(fourdn, "_FILE_METADATA_BATCH_SIZE", 2)
        accessions = [f"4DNF{i:07d}" for i in range(5)]
        field_map = {acc: {"genome_assembly": "GRCh38"} for acc in accessions}
        session = _EchoSession(field_map=field_map)
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = await fourdn.fetch_file_metadata_bulk(accessions)

        # Assert
        assert set(result) == set(accessions)
        assert all(entry["genome_assembly"] == "GRCh38" for entry in result.values())

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_map_genome_assembly_and_track_fields(
        self, mocker
    ):
        """Test that direct and track_and_facet_info fields are mapped.

        Given:
            A file whose Search-API item carries genome_assembly, file_type, and
            track_and_facet_info fields.
        When:
            fetch_file_metadata_bulk requests its metadata.
        Then:
            It should map both the direct fields and the track_and_facet_info
            fields into the accession's entry.
        """
        # Arrange
        field_map = {
            "4DNFIMTTOWBN": {
                "genome_assembly": "GRCh38",
                "file_type": "conservative peaks",
                "track_and_facet_info": {
                    "condition": "untreated",
                    "biosource_name": "H1-hESC",
                },
            }
        }
        session = _EchoSession(field_map=field_map)
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = await fourdn.fetch_file_metadata_bulk(["4DNFIMTTOWBN"])

        # Assert
        entry = result["4DNFIMTTOWBN"]
        assert entry["genome_assembly"] == "GRCh38"
        assert entry["file_type"] == "conservative peaks"
        assert entry["condition"] == "untreated"
        assert entry["biosource_name"] == "H1-hESC"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "failure",
        [500, aiohttp.ClientError("boom")],
        ids=["http_500", "network_error"],
    )
    async def test_fetch_file_metadata_bulk_should_continue_when_a_batch_fails(
        self, mocker, failure
    ):
        """Test that a failed batch does not abort the whole fetch.

        Given:
            Two batches of accessions where the first request fails with an
            error status or a network error.
        When:
            fetch_file_metadata_bulk requests their metadata.
        Then:
            It should still return the second batch's entries and drop only the
            failed batch's accessions.
        """
        # Arrange
        mocker.patch.object(fourdn, "_FILE_METADATA_BATCH_SIZE", 2)
        accessions = [f"4DNF{i:07d}" for i in range(4)]
        field_map = {acc: {"genome_assembly": "GRCh38"} for acc in accessions}
        session = _EchoSession(field_map=field_map, failures={0: failure})
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = await fourdn.fetch_file_metadata_bulk(accessions)

        # Assert
        assert accessions[0] not in result
        assert accessions[1] not in result
        assert accessions[2] in result
        assert accessions[3] in result

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_return_empty_without_http_when_accessions_empty(
        self, mocker
    ):
        """Test that an empty accession list issues no request.

        Given:
            An empty iterable of accessions.
        When:
            fetch_file_metadata_bulk is awaited.
        Then:
            It should return an empty dict and issue no HTTP request.
        """
        # Arrange
        session = _EchoSession()
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = await fourdn.fetch_file_metadata_bulk([])

        # Assert
        assert result == {}
        assert session.get_urls == []

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_return_empty_when_accessions_all_falsy(
        self, mocker
    ):
        """Test that only-falsy accessions issue no request.

        Given:
            An iterable whose accessions are all falsy (empty strings).
        When:
            fetch_file_metadata_bulk is awaited.
        Then:
            It should filter them out, return an empty dict, and issue no request.
        """
        # Arrange
        session = _EchoSession()
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = await fourdn.fetch_file_metadata_bulk(["", ""])

        # Assert
        assert result == {}
        assert session.get_urls == []

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_deduplicate_repeated_accessions(
        self, mocker
    ):
        """Test that duplicate accessions are requested only once.

        Given:
            An input containing duplicate accessions.
        When:
            fetch_file_metadata_bulk requests their metadata.
        Then:
            It should request each distinct accession exactly once.
        """
        # Arrange
        session = _EchoSession(field_map={"4DNF0000001": {"genome_assembly": "GRCh38"}})
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = await fourdn.fetch_file_metadata_bulk(
            ["4DNF0000001", "4DNF0000001", "4DNF0000000"]
        )

        # Assert
        requested = [acc for url in session.get_urls for acc in _accession_params(url)]
        assert sorted(requested) == ["4DNF0000000", "4DNF0000001"]
        assert set(result) <= {"4DNF0000000", "4DNF0000001"}

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_build_correct_query_url(self, mocker):
        """Test the Search-API query URL for a batch.

        Given:
            A single accession.
        When:
            fetch_file_metadata_bulk requests its metadata.
        Then:
            It should issue one ``type=File`` accession-filtered query carrying the
            field set, ``limit``, and ``format``, with no ``from`` offset or
            per-subtype filter.
        """
        # Arrange
        session = _EchoSession()
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        await fourdn.fetch_file_metadata_bulk(["4DNFIMTTOWBN"])

        # Assert
        assert len(session.get_urls) == 1
        url = session.get_urls[0]
        assert url.startswith("https://data.4dnucleome.org/search/")
        assert "type=File" in url
        assert "type=FileProcessed" not in url
        assert "type=FileFastq" not in url
        assert _accession_params(url) == ["4DNFIMTTOWBN"]
        assert "limit=1" in url
        assert "format=json" in url
        assert "from=" not in url
        for field in (
            "accession",
            "genome_assembly",
            "file_type",
            "file_type_detailed",
            "track_and_facet_info",
            "extra_files",
        ):
            assert f"field={field}" in url

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_set_limit_to_batch_length(self, mocker):
        """Test that each query's limit matches its batch size.

        Given:
            More accessions than a small batch size, leaving a partial final batch.
        When:
            fetch_file_metadata_bulk requests their metadata.
        Then:
            Each query's limit should equal that batch's accession count.
        """
        # Arrange
        mocker.patch.object(fourdn, "_FILE_METADATA_BATCH_SIZE", 2)
        session = _EchoSession()
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)
        accessions = [f"4DNF{i:07d}" for i in range(3)]

        # Act
        await fourdn.fetch_file_metadata_bulk(accessions)

        # Assert
        assert len(session.get_urls) == 2
        assert "limit=2" in session.get_urls[0]
        assert len(_accession_params(session.get_urls[0])) == 2
        assert "limit=1" in session.get_urls[1]
        assert len(_accession_params(session.get_urls[1])) == 1

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_issue_single_batch_when_count_equals_batch_size(
        self, mocker
    ):
        """Test the batch boundary at exactly the batch size.

        Given:
            Exactly batch-size accessions.
        When:
            fetch_file_metadata_bulk requests their metadata.
        Then:
            It should issue exactly one query.
        """
        # Arrange
        mocker.patch.object(fourdn, "_FILE_METADATA_BATCH_SIZE", 3)
        session = _EchoSession()
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)
        accessions = [f"4DNF{i:07d}" for i in range(3)]

        # Act
        await fourdn.fetch_file_metadata_bulk(accessions)

        # Assert
        assert len(session.get_urls) == 1

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_issue_two_batches_when_count_is_batch_size_plus_one(
        self, mocker
    ):
        """Test the batch boundary one past the batch size.

        Given:
            One more than batch-size accessions.
        When:
            fetch_file_metadata_bulk requests their metadata.
        Then:
            It should issue two queries, the second carrying a single accession.
        """
        # Arrange
        mocker.patch.object(fourdn, "_FILE_METADATA_BATCH_SIZE", 3)
        session = _EchoSession()
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)
        accessions = [f"4DNF{i:07d}" for i in range(4)]

        # Act
        await fourdn.fetch_file_metadata_bulk(accessions)

        # Assert
        assert len(session.get_urls) == 2
        assert len(_accession_params(session.get_urls[1])) == 1

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_skip_graph_item_missing_accession(
        self, mocker
    ):
        """Test that a graph item without an accession is skipped.

        Given:
            A response graph containing an item with no accession alongside a valid
            item.
        When:
            fetch_file_metadata_bulk requests the metadata.
        Then:
            It should skip the accession-less item and keep the valid one.
        """
        # Arrange
        graph = [
            {"genome_assembly": "GRCh38"},
            {"accession": "4DNFIMTTOWBN", "genome_assembly": "GRCh38"},
        ]
        session = _FixedGraphSession(graph)
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = await fourdn.fetch_file_metadata_bulk(["4DNFIMTTOWBN"])

        # Assert
        assert set(result) == {"4DNFIMTTOWBN"}

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_omit_accession_when_item_has_no_mappable_fields(
        self, mocker
    ):
        """Test that an item with no mappable fields is dropped.

        Given:
            A response item with an accession but no metadata fields (a FASTQ-style
            item without a genome assembly).
        When:
            fetch_file_metadata_bulk requests its metadata.
        Then:
            It should omit that accession from the result.
        """
        # Arrange
        session = _FixedGraphSession([{"accession": "4DNFFASTQ01"}])
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = await fourdn.fetch_file_metadata_bulk(["4DNFFASTQ01"])

        # Assert
        assert result == {}

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_map_file_type_detailed_and_all_track_fields(
        self, mocker
    ):
        """Test that file_type_detailed and every track field are mapped.

        Given:
            An item carrying file_type_detailed and all six track_and_facet_info
            sub-fields.
        When:
            fetch_file_metadata_bulk requests its metadata.
        Then:
            It should lift file_type_detailed and every track sub-field into the
            entry.
        """
        # Arrange
        track = {
            "condition": "untreated",
            "biosource_name": "H1-hESC",
            "dataset": "ds1",
            "experiment_type": "ChIP-seq",
            "assay_info": "info",
            "replicate_info": "Biorep 1 Techrep 1",
        }
        graph = [
            {
                "accession": "4DNFIMTTOWBN",
                "file_type_detailed": "conservative peaks (bigbed)",
                "track_and_facet_info": track,
            }
        ]
        session = _FixedGraphSession(graph)
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = await fourdn.fetch_file_metadata_bulk(["4DNFIMTTOWBN"])

        # Assert
        entry = result["4DNFIMTTOWBN"]
        assert entry["file_type_detailed"] == "conservative peaks (bigbed)"
        for key, value in track.items():
            assert entry[key] == value

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_skip_falsy_track_fields(self, mocker):
        """Test that falsy track sub-fields are omitted.

        Given:
            An item whose track_and_facet_info mixes truthy and falsy sub-fields.
        When:
            fetch_file_metadata_bulk requests its metadata.
        Then:
            It should keep only the truthy sub-fields.
        """
        # Arrange
        graph = [
            {
                "accession": "4DNFIMTTOWBN",
                "track_and_facet_info": {
                    "condition": "untreated",
                    "biosource_name": "",
                    "dataset": None,
                },
            }
        ]
        session = _FixedGraphSession(graph)
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = await fourdn.fetch_file_metadata_bulk(["4DNFIMTTOWBN"])

        # Assert
        entry = result["4DNFIMTTOWBN"]
        assert entry["condition"] == "untreated"
        assert "biosource_name" not in entry
        assert "dataset" not in entry

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_pass_extra_files_through_parser(
        self, mocker
    ):
        """Test that extra_files are normalized via parse_extra_files.

        Given:
            An item whose extra_files list carries a raw sidecar entry.
        When:
            fetch_file_metadata_bulk requests its metadata.
        Then:
            Its entry's extra_files should equal parse_extra_files of the raw list.
        """
        # Arrange
        raw_extra = [
            {"href": "/x.pairs.gz.px2", "file_format": "pairs_px2", "file_size": 5}
        ]
        session = _FixedGraphSession(
            [{"accession": "4DNFIMTTOWBN", "extra_files": raw_extra}]
        )
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = await fourdn.fetch_file_metadata_bulk(["4DNFIMTTOWBN"])

        # Assert
        assert result["4DNFIMTTOWBN"]["extra_files"] == parse_extra_files(raw_extra)

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_omit_extra_files_when_none(self, mocker):
        """Test that a missing extra_files list yields no extra_files key.

        Given:
            An item with a mappable field but no extra_files.
        When:
            fetch_file_metadata_bulk requests its metadata.
        Then:
            Its entry should have no extra_files key.
        """
        # Arrange
        session = _FixedGraphSession(
            [{"accession": "4DNFIMTTOWBN", "genome_assembly": "GRCh38"}]
        )
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = await fourdn.fetch_file_metadata_bulk(["4DNFIMTTOWBN"])

        # Assert
        assert "extra_files" not in result["4DNFIMTTOWBN"]

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_omit_accession_absent_from_graph(
        self, mocker
    ):
        """Test that an accession with no upstream item is omitted.

        Given:
            A requested accession that the upstream graph does not return.
        When:
            fetch_file_metadata_bulk requests its metadata.
        Then:
            It should still issue the request but omit that accession without error.
        """
        # Arrange
        session = _FixedGraphSession([])
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = await fourdn.fetch_file_metadata_bulk(["4DNFMISSING1"])

        # Assert
        assert result == {}
        assert len(session.get_urls) == 1

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_return_empty_when_all_batches_fail(
        self, mocker
    ):
        """Test that all-failing batches return empty without raising.

        Given:
            Two batches whose requests both fail (a status error and a network
            error).
        When:
            fetch_file_metadata_bulk requests their metadata.
        Then:
            It should return an empty dict and not raise.
        """
        # Arrange
        mocker.patch.object(fourdn, "_FILE_METADATA_BATCH_SIZE", 2)
        accessions = [f"4DNF{i:07d}" for i in range(4)]
        session = _EchoSession(failures={0: 500, 1: aiohttp.ClientError("boom")})
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = await fourdn.fetch_file_metadata_bulk(accessions)

        # Assert
        assert result == {}

    @pytest.mark.asyncio
    async def test_fetch_file_metadata_bulk_should_continue_when_middle_batch_fails(
        self, mocker
    ):
        """Test that a failed middle batch does not drop later batches.

        Given:
            Three single-accession batches where the middle request fails.
        When:
            fetch_file_metadata_bulk requests their metadata.
        Then:
            It should keep the first and third batches and drop only the middle.
        """
        # Arrange
        mocker.patch.object(fourdn, "_FILE_METADATA_BATCH_SIZE", 1)
        accessions = [f"4DNF{i:07d}" for i in range(3)]
        field_map = {acc: {"genome_assembly": "GRCh38"} for acc in accessions}
        session = _EchoSession(field_map=field_map, failures={1: 500})
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = await fourdn.fetch_file_metadata_bulk(accessions)

        # Assert
        assert accessions[0] in result
        assert accessions[1] not in result
        assert accessions[2] in result

    @settings(
        max_examples=50,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        accessions=st.lists(
            st.from_regex(r"4DNF[A-Z0-9]{7}", fullmatch=True), max_size=20
        )
    )
    def test_pbt_001_request_union_equals_deduped_input(self, mocker, accessions):
        """Test that every distinct accession is requested exactly once.

        Given:
            Any list of 4DN accessions (possibly with duplicates).
        When:
            fetch_file_metadata_bulk requests their metadata.
        Then:
            The union of accessions across all issued queries should equal the
            deduped, non-empty input.
        """
        # Arrange
        mocker.patch.object(fourdn, "_FILE_METADATA_BATCH_SIZE", 3)
        mocker.patch.object(fourdn.asyncio, "sleep", mocker.AsyncMock())
        session = _EchoSession()
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        asyncio.run(fourdn.fetch_file_metadata_bulk(accessions))

        # Assert
        requested = {acc for url in session.get_urls for acc in _accession_params(url)}
        assert requested == {acc for acc in accessions if acc}

    @settings(
        max_examples=50,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        accessions=st.lists(
            st.from_regex(r"4DNF[A-Z0-9]{7}", fullmatch=True), min_size=1, max_size=20
        )
    )
    def test_pbt_002_batches_partition_input_and_are_size_bounded(
        self, mocker, accessions
    ):
        """Test that batches partition the input within the size bound.

        Given:
            Any non-empty list of 4DN accessions and a fixed batch size.
        When:
            fetch_file_metadata_bulk requests their metadata.
        Then:
            Each accession should appear in exactly one query, every query should
            hold between one and the batch size, and the query count should be the
            ceiling of the deduped count over the batch size.
        """
        # Arrange
        batch_size = 3
        mocker.patch.object(fourdn, "_FILE_METADATA_BATCH_SIZE", batch_size)
        mocker.patch.object(fourdn.asyncio, "sleep", mocker.AsyncMock())
        session = _EchoSession()
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        asyncio.run(fourdn.fetch_file_metadata_bulk(accessions))

        # Assert
        per_url = [_accession_params(url) for url in session.get_urls]
        flat = [acc for params in per_url for acc in params]
        deduped = {acc for acc in accessions if acc}
        assert len(flat) == len(set(flat))
        assert set(flat) == deduped
        assert all(1 <= len(params) <= batch_size for params in per_url)
        assert len(session.get_urls) == math.ceil(len(deduped) / batch_size)

    @settings(
        max_examples=50,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        accessions=st.lists(
            st.from_regex(r"4DNF[A-Z0-9]{7}", fullmatch=True), max_size=20
        )
    )
    def test_pbt_003_result_keys_subset_of_deduped_input(self, mocker, accessions):
        """Test that result keys never exceed the requested accessions.

        Given:
            Any list of 4DN accessions, each with upstream metadata.
        When:
            fetch_file_metadata_bulk requests their metadata.
        Then:
            The result keys should be a subset of the deduped, non-empty input.
        """
        # Arrange
        mocker.patch.object(fourdn, "_FILE_METADATA_BATCH_SIZE", 3)
        mocker.patch.object(fourdn.asyncio, "sleep", mocker.AsyncMock())
        field_map = {acc: {"genome_assembly": "GRCh38"} for acc in accessions if acc}
        session = _EchoSession(field_map=field_map)
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        result = asyncio.run(fourdn.fetch_file_metadata_bulk(accessions))

        # Assert
        assert set(result) <= {acc for acc in accessions if acc}

    @settings(
        max_examples=50,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        accessions=st.lists(
            st.from_regex(r"4DNF[A-Z0-9]{7}", fullmatch=True), min_size=1, max_size=20
        )
    )
    def test_pbt_004_no_url_contains_from_offset(self, mocker, accessions):
        """Test that deep-pagination offsets are never emitted.

        Given:
            Any non-empty list of 4DN accessions.
        When:
            fetch_file_metadata_bulk requests their metadata.
        Then:
            No issued query should contain a ``from`` offset.
        """
        # Arrange
        mocker.patch.object(fourdn, "_FILE_METADATA_BATCH_SIZE", 3)
        mocker.patch.object(fourdn.asyncio, "sleep", mocker.AsyncMock())
        session = _EchoSession()
        mocker.patch.object(fourdn.aiohttp, "ClientSession", return_value=session)

        # Act
        asyncio.run(fourdn.fetch_file_metadata_bulk(accessions))

        # Assert
        assert all("from=" not in url for url in session.get_urls)
