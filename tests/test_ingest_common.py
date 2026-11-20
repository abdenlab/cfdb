"""Tests for the ingest helpers shared by the REST-API DCC loaders."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cfdb.services.ingest_common import (
    COMPRESSION_SUFFIX_TO_EDAM,
    UNCOMPRESSED,
    UNMAPPABLE_COMPRESSION_SUFFIXES,
    derive_compression_format,
    timeout_from_env,
)

DOWNLOAD_URL = "https://www.encodeproject.org/files/ENCFF123ABC/@@download/{name}"

# Every value the derivation is allowed to produce, for closed-domain
# assertions.
DERIVED_VALUES = set(COMPRESSION_SUFFIX_TO_EDAM.values()) | {UNCOMPRESSED, None}

#: A variable name no other test or setting uses, so the timeout tests own it.
TIMEOUT_ENV = "CFDB_TEST_INGEST_TIMEOUT_SECONDS"


def test_timeout_from_env_should_return_the_default_when_unset(monkeypatch):
    """Test an unset or empty variable falls back to the default.

    Given:
        The variable unset, then set to an empty string.
    When:
        timeout_from_env resolves it.
    Then:
        It should return the default both times.
    """
    # Arrange
    monkeypatch.delenv(TIMEOUT_ENV, raising=False)

    # Act
    unset = timeout_from_env(TIMEOUT_ENV, 42)
    monkeypatch.setenv(TIMEOUT_ENV, "")
    empty = timeout_from_env(TIMEOUT_ENV, 42)

    # Assert
    assert (unset, empty) == (42, 42)


def test_timeout_from_env_should_parse_a_positive_integer(monkeypatch):
    """Test a positive integer value overrides the default.

    Given:
        The variable set to "90".
    When:
        timeout_from_env resolves it.
    Then:
        It should return 90.
    """
    # Arrange
    monkeypatch.setenv(TIMEOUT_ENV, "90")

    # Act
    value = timeout_from_env(TIMEOUT_ENV, 42)

    # Assert
    assert value == 90


@pytest.mark.parametrize(
    ("raw", "message"),
    [("ninety", "not a valid integer"), ("0", "must be > 0"), ("-5", "must be > 0")],
)
def test_timeout_from_env_should_raise_when_the_value_is_unusable(
    monkeypatch, raw, message
):
    """Test a malformed or non-positive budget fails loudly.

    aiohttp reads a non-positive total as "no timeout", so accepting one
    would turn a misconfiguration into an unbounded request.

    Given:
        The variable set to a non-integer, zero, or a negative value.
    When:
        timeout_from_env resolves it.
    Then:
        It should raise a ValueError naming the variable and the defect.
    """
    # Arrange
    monkeypatch.setenv(TIMEOUT_ENV, raw)

    # Act & assert
    with pytest.raises(ValueError, match=message) as exc_info:
        timeout_from_env(TIMEOUT_ENV, 42)
    assert TIMEOUT_ENV in str(exc_info.value)


def test_compression_suffix_tables_should_hold_the_verified_edam_terms():
    """Test the suffix vocabulary against literal, externally verified IDs.

    Given:
        The EDAM terms resolved against OLS4: format:3989 is "GZIP format"
        (extensions gz, gzip), format:3615 is "bgzip", and format:3990 is
        "AVI" rather than the bzip2 term it resembles.
    When:
        The published suffix tables are compared to those literals.
    Then:
        They should match exactly, so a mistyped or dropped term cannot ship
        unnoticed.
    """
    # Act & assert
    assert COMPRESSION_SUFFIX_TO_EDAM == {
        ".gz": "format:3989",
        ".gzip": "format:3989",
        ".tgz": "format:3989",
        ".bgz": "format:3615",
    }
    assert UNMAPPABLE_COMPRESSION_SUFFIXES == (
        ".bz2",
        ".xz",
        ".zst",
        ".zip",
        ".starch",
    )


def test_derive_compression_format_should_return_gzip_term_when_url_ends_in_gz():
    """Test the canonical ENCODE download URL for a gzipped file.

    Given:
        The real ENCODE download URL shape, whose basename ends in ".bed.gz"
        and whose path contains an "@@download" segment.
    When:
        derive_compression_format is called.
    Then:
        It should return the EDAM gzip term ID.
    """
    # Arrange
    url = DOWNLOAD_URL.format(name="ENCFF123ABC.bed.gz")

    # Act
    result = derive_compression_format(url)

    # Assert
    assert result == "format:3989"


@pytest.mark.parametrize(
    ("suffix", "term_id"), sorted(COMPRESSION_SUFFIX_TO_EDAM.items())
)
def test_derive_compression_format_should_return_the_mapped_term_for_every_suffix(
    suffix, term_id
):
    """Test that every suffix in the mapping table is reachable.

    Given:
        A filename ending in each suffix the module publishes.
    When:
        derive_compression_format is called.
    Then:
        It should return that suffix's EDAM term ID.
    """
    # Act
    result = derive_compression_format(f"ENCFF123ABC.bed{suffix}")

    # Assert
    assert result == term_id


def test_derive_compression_format_should_return_bgzip_term_when_suffix_is_bgz():
    """Test that the gzip suffix does not shadow the bgzip suffix.

    Given:
        A filename ending in ".bgz", which contains the letters "gz".
    When:
        derive_compression_format is called.
    Then:
        It should return the bgzip term rather than the plain gzip term.
    """
    # Act
    result = derive_compression_format("ENCFF123ABC.bed.bgz")

    # Assert
    assert result == "format:3615"


def test_derive_compression_format_should_agree_on_tgz_and_tar_gz():
    """Test that the two spellings of a gzipped tar agree.

    Given:
        The same artifact named ".tar.gz" and named ".tgz".
    When:
        derive_compression_format is called on each.
    Then:
        Both should return the gzip term, since the compression is identical.
    """
    # Act
    long_form = derive_compression_format("ENCFF1.tar.gz")
    short_form = derive_compression_format("ENCFF1.tgz")

    # Assert
    assert long_form == short_form == "format:3989"


@pytest.mark.parametrize(
    "name", ["ENCFF1.BED.GZ", "ENCFF1.bed.Gz", "ENCFF1.BED.BGZ", "ENCFF1.TGZ"]
)
def test_derive_compression_format_should_ignore_case_of_the_suffix(name):
    """Test that suffix matching is case insensitive.

    Given:
        A filename whose compression suffix is upper or mixed case.
    When:
        derive_compression_format is called.
    Then:
        It should return the same term as the lowercase spelling.
    """
    # Act
    result = derive_compression_format(name)

    # Assert
    assert result == derive_compression_format(name.lower())


def test_derive_compression_format_should_use_the_basename_when_the_query_holds_a_path():
    """Test that a query string containing a slash cannot become the basename.

    Given:
        A gzipped download URL whose query string embeds a path ending in a
        different, uncompressed filename.
    When:
        derive_compression_format is called.
    Then:
        It should return the gzip term from the real basename.
    """
    # Act
    result = derive_compression_format("https://h/ENCFF1.bed.gz?redirect=/x/y.bam")

    # Assert
    assert result == "format:3989"


@pytest.mark.parametrize(
    "url", ["https://h/ENCFF1.bed?download=y.gz", "https://h/ENCFF1.bed#part.gz"]
)
def test_derive_compression_format_should_ignore_a_suffix_outside_the_basename(url):
    """Test that a query string or fragment cannot fabricate a suffix.

    Given:
        A URL for an uncompressed file whose query string or fragment ends in
        a compression suffix.
    When:
        derive_compression_format is called.
    Then:
        It should report no compression.
    """
    # Act
    result = derive_compression_format(url)

    # Assert
    assert result == UNCOMPRESSED


@pytest.mark.parametrize("name", ["sample#1.bed.gz", "sample?1.bed.gz"])
def test_derive_compression_format_should_keep_url_punctuation_in_a_bare_filename(name):
    """Test that a bare filename is not truncated at a '#' or '?'.

    Given:
        A gzipped bare filename containing a character that would delimit a
        query string or fragment in a URL.
    When:
        derive_compression_format is called.
    Then:
        It should still return the gzip term, since URL syntax does not apply
        to a name that is not a URL.
    """
    # Act
    result = derive_compression_format(name)

    # Assert
    assert result == "format:3989"


@pytest.mark.parametrize(
    "name", ["ENCFF1.gz.bed", "ENCFF1.bed.gz.txt", "gzfile.bed", "ENCFF1.zip.bam"]
)
def test_derive_compression_format_should_report_uncompressed_when_suffix_is_not_final(
    name,
):
    """Test that a compression token away from the end does not match.

    Given:
        A filename in which a compression token appears somewhere other than
        the final suffix.
    When:
        derive_compression_format is called.
    Then:
        It should report no compression.
    """
    # Act
    result = derive_compression_format(name)

    # Assert
    assert result == UNCOMPRESSED


@pytest.mark.parametrize(
    "name",
    ["ENCFF1.bed", "ENCFF1.bam", "ENCFF1.bigWig", "ENCFF1.bigBed", "ENCFF1.tar"],
)
def test_derive_compression_format_should_report_uncompressed_when_no_suffix_matches(
    name,
):
    """Test the sentinel for names carrying no extrinsic compression.

    Given:
        Filenames for formats the corpus carries, including BAM, bigWig and
        bigBed, whose bytes are internally compressed but whose file_format
        already names that container.
    When:
        derive_compression_format is called.
    Then:
        It should return the empty-string sentinel, which reports the absence
        of compression beyond file_format rather than uncompressed bytes.
    """
    # Act
    result = derive_compression_format(name)

    # Assert
    assert result == UNCOMPRESSED
    assert result is not None


@pytest.mark.parametrize("suffix", UNMAPPABLE_COMPRESSION_SUFFIXES)
def test_derive_compression_format_should_return_none_when_compression_is_unmappable(
    suffix,
):
    """Test that a compressed file EDAM cannot name is not called uncompressed.

    Given:
        A filename ending in a compression suffix no EDAM term expresses.
    When:
        derive_compression_format is called.
    Then:
        It should return None rather than the uncompressed sentinel.
    """
    # Act
    result = derive_compression_format(f"ENCFF1.bed{suffix}")

    # Assert
    assert result is None


@pytest.mark.parametrize(
    "value", [None, "", "   ", "https://h/files/ENCFF1/@@download/", "https://h/"]
)
def test_derive_compression_format_should_return_none_when_no_basename_is_available(
    value,
):
    """Test that a name-less input yields no determination.

    Given:
        An absent, blank, or directory-style value carrying no filename.
    When:
        derive_compression_format is called.
    Then:
        It should return None, since there is no evidence either way.
    """
    # Act
    result = derive_compression_format(value)

    # Assert
    assert result is None


@given(st.none() | st.text())
@settings(max_examples=300)
def test_derive_compression_format_should_return_a_value_from_the_closed_vocabulary(
    value,
):
    """Test that the derived value never escapes its vocabulary.

    Given:
        Any text, or no value at all, as a filename or URL.
    When:
        derive_compression_format is called.
    Then:
        It should return a mapped EDAM term, the uncompressed sentinel, or
        None, and never raise.
    """
    # Act
    result = derive_compression_format(value)

    # Assert
    assert result in DERIVED_VALUES


@given(
    name=st.text(min_size=1).filter(
        lambda s: s == s.strip() and not (set(s) & set("/?#"))
    ),
    host=st.text(alphabet=st.characters(whitelist_categories=("Ll", "Nd")), min_size=1),
    query=st.text().filter(lambda s: not (set(s) & set("?#"))),
    fragment=st.text().filter(lambda s: "#" not in s),
)
@settings(max_examples=200)
def test_derive_compression_format_should_ignore_url_decoration(
    name, host, query, fragment
):
    """Test that wrapping a filename in a URL does not change the answer.

    Given:
        Any filename free of the URL-reserved characters that would make it
        mean something else inside a URL, plus any host, query string and
        fragment to decorate it with.
    When:
        derive_compression_format is called on the bare name and on the
        decorated URL.
    Then:
        Both should yield the same value.
    """
    # Arrange
    url = f"https://{host}/p/{name}?{query}#{fragment}"

    # Act
    bare_result = derive_compression_format(name)
    url_result = derive_compression_format(url)

    # Assert
    assert bare_result == url_result


@given(
    stem=st.text().filter(lambda s: "/" not in s),
    suffix=st.sampled_from(sorted(COMPRESSION_SUFFIX_TO_EDAM)),
)
@settings(max_examples=200)
def test_derive_compression_format_should_classify_any_stem_with_a_known_suffix(
    stem, suffix
):
    """Test that no stem can suppress a recognized compression suffix.

    Given:
        Any stem free of path separators, followed by one of the mapped
        compression suffixes.
    When:
        derive_compression_format is called.
    Then:
        It should return a mapped EDAM term rather than the uncompressed
        sentinel or None.
    """
    # Act
    result = derive_compression_format(stem + suffix)

    # Assert
    assert result in set(COMPRESSION_SUFFIX_TO_EDAM.values())
