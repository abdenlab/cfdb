"""Tests for ENCODE file format mapping in ontology_mappings."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cfdb.services.ontology_mappings import (
    FILE_FORMAT_TO_EDAM,
    MINTED_FORMAT_PREFIX,
    get_data_type,
    get_file_format,
)
from cfdb.workflows.processors.bam import BamIndexProcessor
from cfdb.workflows.processors.registry import default_registry
from cfdb.workflows.processors.tabix import TabixIntervalProcessor

#: The minted subset of the format table. Extracted so the property tests
#: below share one definition and can size their example budgets from it --
#: these domains are small closed tables, so exhausting them is both cheaper
#: and stricter than the default budget.
MINTED_ENTRIES = [
    (key, value)
    for key, value in FILE_FORMAT_TO_EDAM.items()
    if value["id"].startswith(MINTED_FORMAT_PREFIX)
]


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


def test_get_file_format_should_return_a_minted_term_for_biginteract():
    """Test bigInteract resolves to its own term rather than to bigBed.

    Given:
        The format string "bigInteract", a bigBed variant whose trailing
        columns encode interaction endpoints.
    When:
        get_file_format is called.
    Then:
        It should return a minted term distinct from the EDAM bigBed term,
        so the interaction is not silently served as a plain interval.
    """
    # Act
    result = get_file_format("bigInteract")

    # Assert
    assert result == {"id": "cfdb:biginteract", "name": "bigInteract"}
    assert result != get_file_format("bigBed")


def test_get_file_format_should_return_a_minted_term_for_bedpe():
    """Test bedpe resolves to its own term rather than to BED.

    Given:
        The format string "bedpe", whose columns are chrom1/start1/end1/
        chrom2/start2/end2 rather than a single interval.
    When:
        get_file_format is called.
    Then:
        It should return a minted term distinct from the EDAM BED term.
    """
    # Act
    result = get_file_format("bedpe")

    # Assert
    assert result == {"id": "cfdb:bedpe", "name": "bedpe"}
    assert result != get_file_format("bed")


def _wired_registry():
    """Build the registry the API serves from.

    Mirrors the registrations in ``cfdb.api.main``'s lifespan, so an
    assertion about what is claimed holds against the real deployment
    rather than a hand-picked pair of classes.
    """
    registry = default_registry()
    registry.register(BamIndexProcessor())
    registry.register(TabixIntervalProcessor())
    return registry


@pytest.mark.parametrize("encode_format", ["bedpe", "bigInteract"])
def test_get_file_format_should_keep_paired_formats_out_of_the_tabix_pipeline(
    encode_format,
):
    """Test the paired-interval formats are claimed by no processor.

    Given:
        A format whose records pair two loci, which the BED tabix pipeline
        would index by the first locus alone -- committing an artifact that
        looks successful and is wrong -- and the registry the API wires.
    When:
        A file document carrying that format is looked up.
    Then:
        No processor should claim it, so a request streams the raw
        upstream file instead of a mangled index.
    """
    # Arrange
    file_meta = {"file_format": get_file_format(encode_format)}

    # Act
    processor = _wired_registry().lookup_for(file_meta)

    # Assert
    assert processor is None


def test_get_file_format_should_still_route_plain_bed_to_the_tabix_pipeline():
    """Test the minting did not cost the formats that were routed correctly.

    A positive control for the assertion above: "no processor claims it"
    would also hold if the registry were empty or lookup were broken.

    Given:
        A plain BED format and the registry the API wires.
    When:
        A file document carrying it is looked up.
    Then:
        The tabix interval processor should claim it.
    """
    # Arrange
    file_meta = {"file_format": get_file_format("bed")}

    # Act
    processor = _wired_registry().lookup_for(file_meta)

    # Assert
    assert isinstance(processor, TabixIntervalProcessor)


@settings(max_examples=len(MINTED_ENTRIES))
@given(entry=st.sampled_from(MINTED_ENTRIES))
def test_get_file_format_should_derive_a_minted_id_from_its_key(entry):
    """Test a minted term's id matches the format it is keyed under.

    A typo'd mint -- "cfdb:biginterract" -- is invisible to every other
    assertion, since nothing else compares the id to anything. Asserted
    through the accessor rather than against the table directly, so the
    property covers what production reads.

    Given:
        Any format whose table entry carries a minted id.
    When:
        get_file_format is called with its key.
    Then:
        The returned id should be the prefix followed by the key.
    """
    # Arrange
    key, _ = entry

    # Act
    term = get_file_format(key)

    # Assert
    assert term["id"] == f"{MINTED_FORMAT_PREFIX}{key}"


@settings(max_examples=len(FILE_FORMAT_TO_EDAM))
@given(entry=st.sampled_from(sorted(FILE_FORMAT_TO_EDAM.items())))
def test_get_file_format_should_return_only_edam_or_minted_ids(entry):
    """Test the accessor admits only two kinds of identifier.

    The minted prefix is the mechanism that keeps an unrepresented format
    from being aliased onto a term that means something else. A third id
    shape would sit outside it silently.

    Given:
        Any format in the table.
    When:
        get_file_format is called with its key.
    Then:
        The returned id should be an EDAM format term or a minted token,
        and its name should be non-empty.
    """
    # Arrange
    key, _ = entry

    # Act
    term = get_file_format(key)

    # Assert
    assert term["id"].startswith("format:") or term["id"].startswith(
        MINTED_FORMAT_PREFIX
    )
    assert term["name"]


@settings(max_examples=len(MINTED_ENTRIES))
@given(entry=st.sampled_from(MINTED_ENTRIES))
def test_get_file_format_should_not_let_a_minted_term_borrow_an_edam_name(entry):
    """Test a minted term cannot be defeated by reusing an EDAM name.

    Routing keys on the format *name*, not the id, so
    ``{"id": "cfdb:x", "name": "BED"}`` would mint an id and still drop
    the file into the BED pipeline -- the exact outcome minting exists to
    prevent.

    Given:
        Any format whose table entry carries a minted id.
    When:
        get_file_format is called with its key.
    Then:
        The returned name should be shared with no EDAM entry and claimed
        by no processor.
    """
    # Arrange
    key, _ = entry
    edam_names = {
        other["name"]
        for other in FILE_FORMAT_TO_EDAM.values()
        if not other["id"].startswith(MINTED_FORMAT_PREFIX)
    }

    # Act
    term = get_file_format(key)

    # Assert
    assert term["name"] not in edam_names
    assert _wired_registry().lookup_for({"file_format": term}) is None


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


@pytest.mark.parametrize(
    "output_type, expected",
    [
        (
            "candidate Cis-Regulatory Elements",
            {"id": "data:1255", "name": "Sequence features"},
        ),
        ("elements reference", {"id": "data:1255", "name": "Sequence features"}),
        ("element gene links", {"id": "data:0006", "name": "Data"}),
        (
            "thresholded element gene links",
            {"id": "data:0006", "name": "Data"},
        ),
        ("thresholded links", {"id": "data:0006", "name": "Data"}),
    ],
)
def test_get_data_type_should_map_every_annotation_output_type(output_type, expected):
    """Test the annotation Output type domain resolves to its documented term.

    These five are the complete Output type domain of the two ingested
    annotation types, verified against the live TSVs; without them every
    annotation file would carry a null data_type. Asserted by exact term
    rather than merely non-null, so a copy-paste that pointed cCREs at the
    generic Data term fails here.

    Given:
        An Output type value published by an ingested annotation type.
    When:
        get_data_type is called.
    Then:
        It should return that value's documented CV term.
    """
    # Act
    result = get_data_type(output_type)

    # Assert
    assert result == expected


# Unlike the sampled-table properties above, this domain is unbounded, so
# the budget is doing real work rather than exhausting a closed set.
@settings(max_examples=200)
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
