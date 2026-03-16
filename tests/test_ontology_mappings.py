"""Tests for ENCODE file format mapping in ontology_mappings."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from cfdb.services.ontology_mappings import get_file_format


def test_get_file_format_with_simple_format():
    """Test simple format string lookup.

    Given:
        A simple format string that exactly matches a dictionary key.
    When:
        get_file_format is called with "fastq".
    Then:
        It should return the EDAM FASTQ term.
    """
    result = get_file_format("fastq")

    assert result == {"id": "format:1930", "name": "FASTQ"}


def test_get_file_format_with_compound_bigbed():
    """Test compound format string fallback to first token for bigBed.

    Given:
        A compound format string "bigBed narrowPeak" with no exact match.
    When:
        get_file_format is called.
    Then:
        It should use the first token "bigBed" as a fallback lookup.
    """
    result = get_file_format("bigBed narrowPeak")

    assert result == {"id": "format:3004", "name": "bigBed"}


def test_get_file_format_with_compound_bed():
    """Test compound format string fallback to first token for bed.

    Given:
        A compound format string "bed bed3+" with no exact match.
    When:
        get_file_format is called.
    Then:
        It should use the first token "bed" as a fallback lookup.
    """
    result = get_file_format("bed bed3+")

    assert result == {"id": "format:3003", "name": "BED"}


def test_get_file_format_with_compound_bigwig():
    """Test compound format string fallback to first token for bigWig.

    Given:
        A compound format string "bigWig bed3+" with no exact match.
    When:
        get_file_format is called.
    Then:
        It should use the first token "bigWig" as a fallback lookup.
    """
    result = get_file_format("bigWig bed3+")

    assert result == {"id": "format:3006", "name": "bigWig"}


def test_get_file_format_with_exact_compound_key():
    """Test exact compound key takes precedence over first-token fallback.

    Given:
        A compound format string "bed narrowPeak" that has an exact match.
    When:
        get_file_format is called.
    Then:
        It should return the exact match NarrowPeak instead of the first-token fallback BED.
    """
    result = get_file_format("bed narrowPeak")

    assert result == {"id": "format:3613", "name": "NarrowPeak"}


def test_get_file_format_with_starch_format():
    """Test starch format maps to BED.

    Given:
        The format string "starch" (BEDOPS compressed BED archive).
    When:
        get_file_format is called.
    Then:
        It should return the EDAM BED term.
    """
    result = get_file_format("starch")

    assert result == {"id": "format:3003", "name": "BED"}


def test_get_file_format_with_tagalign_case_insensitive():
    """Test case-insensitive tagAlign lookup.

    Given:
        The format string "tagAlign" with mixed case.
    When:
        get_file_format is called.
    Then:
        It should perform a case-insensitive lookup and return the EDAM BED term.
    """
    result = get_file_format("tagAlign")

    assert result == {"id": "format:3003", "name": "BED"}


def test_get_file_format_with_biginteract_format():
    """Test bigInteract format maps to bigBed.

    Given:
        The format string "bigInteract" (a bigBed variant).
    When:
        get_file_format is called.
    Then:
        It should return the EDAM bigBed term.
    """
    result = get_file_format("bigInteract")

    assert result == {"id": "format:3004", "name": "bigBed"}


def test_get_file_format_with_h5ad_format():
    """Test h5ad format maps to HDF5.

    Given:
        The format string "h5ad" (AnnData HDF5 format).
    When:
        get_file_format is called.
    Then:
        It should return the EDAM HDF5 term.
    """
    result = get_file_format("h5ad")

    assert result == {"id": "format:3590", "name": "HDF5"}


def test_get_file_format_with_empty_string():
    """Test empty format string returns None.

    Given:
        An empty format string.
    When:
        get_file_format is called.
    Then:
        It should return None.
    """
    result = get_file_format("")

    assert result is None


def test_get_file_format_with_unknown_format():
    """Test unrecognized format string returns None.

    Given:
        An unrecognized format string "xyzzy".
    When:
        get_file_format is called.
    Then:
        It should return None.
    """
    result = get_file_format("xyzzy")

    assert result is None


def test_get_file_format_with_unknown_compound():
    """Test unrecognized compound format string returns None.

    Given:
        An unrecognized compound format string "xyzzy foo".
    When:
        get_file_format is called.
    Then:
        It should return None because neither the full string nor the first token matches.
    """
    result = get_file_format("xyzzy foo")

    assert result is None


@given(st.text())
def test_get_file_format_return_type_invariant(format_string):
    """Test return type is always None or a dict with id and name keys.

    Given:
        An arbitrary text input.
    When:
        get_file_format is called.
    Then:
        It should return None or a dict containing "id" and "name" keys.
    """
    result = get_file_format(format_string)

    if result is not None:
        assert isinstance(result, dict)
        assert "id" in result
        assert "name" in result
