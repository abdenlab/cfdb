"""Tests for 4DN enrichment helpers."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from cfdb.models import NUMERIC_PROTOCOL_FIELDS, EnrichedFourdnCollection
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
