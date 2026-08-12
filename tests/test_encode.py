"""Tests for the ENCODE metadata fetch, compression derivation and transform."""

from __future__ import annotations

import asyncio
import logging

import aiohttp
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cfdb.accessions import normalize_accession
from cfdb.services import encode as encode_module
from cfdb.services.encode import (
    COMPRESSION_SUFFIX_TO_EDAM,
    UNCOMPRESSED,
    UNMAPPABLE_COMPRESSION_SUFFIXES,
    derive_compression_format,
    fetch_encode_metadata,
    transform_to_c2m2,
)

#: The variable the metadata fetch reads its budget from. Named here rather
#: than imported so the tests pin the operator-facing contract: renaming it in
#: the module should fail these tests, not silently follow along.
TIMEOUT_ENV = "ENCODE_METADATA_TIMEOUT_SECONDS"

DOWNLOAD_URL = "https://www.encodeproject.org/files/ENCFF123ABC/@@download/{name}"

# Every value the derivation is allowed to produce, for closed-domain
# assertions.
DERIVED_VALUES = set(COMPRESSION_SUFFIX_TO_EDAM.values()) | {UNCOMPRESSED, None}


def _encode_row(**overrides) -> dict:
    """Return a minimal but realistic ENCODE metadata TSV row."""
    row = {
        "File accession": "ENCFF123ABC",
        "File format": "bed narrowPeak",
        "File download URL": DOWNLOAD_URL.format(name="ENCFF123ABC.bed.gz"),
        "Output type": "peaks",
        "Assay": "TF ChIP-seq",
        "Size": "1024",
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "File Status": "released",
        "Experiment date released": "2020-01-01",
    }
    row.update(overrides)
    return row


class _FakeContent:
    """Async-iterable stand-in for ``response.content``.

    Yields each line as bytes, the way aiohttp delivers a streamed body, so
    the parser's own decode and newline handling are exercised rather than
    bypassed. ``fail_after`` raises once that many lines have been yielded,
    modelling a stream that dies partway.
    """

    def __init__(self, lines: list[str], fail_after: int | None = None):
        self._lines = lines
        self._fail_after = fail_after

    async def __aiter__(self):  # pragma: no cover - delegated to __anext__
        for index, line in enumerate(self._lines):
            if self._fail_after is not None and index == self._fail_after:
                raise asyncio.TimeoutError
            yield f"{line}\n".encode()


class _FakeResponse:
    """Async-context-manager stand-in for a streamed aiohttp response."""

    def __init__(self, status: int, content: _FakeContent):
        self.status = status
        self.content = content

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _StreamingSession:
    """Fake aiohttp session that serves one streamed TSV body.

    Records the kwargs of every ``get`` -- unlike the 4DN fakes, which
    discard them -- because the request's timeout is the behavior under
    test. ``error`` raises instead of responding, modelling a network
    failure before any body arrives.
    """

    def __init__(self, lines=None, status=200, fail_after=None, error=None):
        self._lines = lines or []
        self._status = status
        self._fail_after = fail_after
        self._error = error
        self.get_kwargs: list[dict] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, **kwargs):
        self.get_kwargs.append(kwargs)
        if self._error is not None:
            raise self._error
        return _FakeResponse(
            self._status, _FakeContent(self._lines, self._fail_after)
        )


def _tsv(*rows: str) -> list[str]:
    """Return a header line plus the given data lines."""
    return ["File accession\tFile format", *rows]


@pytest.mark.asyncio
async def test_fetch_encode_metadata_should_bound_the_request_by_the_configured_timeout(
    mocker, monkeypatch
):
    """Test the request budget is the configured timeout, not a short literal.

    A 600-second total aborted the sync around 230,000 of ~810,000 rows: the
    budget covers the whole streamed body, and every row is inserted as it
    streams, so the clock tracks insert throughput rather than latency. Since
    the DCC is cleared before reloading, the abort left the corpus smaller
    than it started.

    Given:
        A streamed metadata response and no timeout override.
    When:
        fetch_encode_metadata drains it.
    Then:
        It should have requested with a budget of at least an hour.
    """
    # Arrange
    monkeypatch.delenv(TIMEOUT_ENV, raising=False)
    session = _StreamingSession(lines=_tsv("ENCFF1\tbed"))
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    [row async for row in fetch_encode_metadata()]

    # Assert
    assert session.get_kwargs[0]["timeout"].total >= 3600


@pytest.mark.asyncio
async def test_fetch_encode_metadata_should_honor_the_timeout_from_the_environment(
    mocker, monkeypatch
):
    """Test an operator can raise the budget without a deploy.

    Asserted through the fetch rather than against the parser alone, so the
    whole chain is covered: the variable is read, parsed, and reaches the
    request. A parser test would pass even if the value never got that far.

    Given:
        The timeout variable set to two hours.
    When:
        fetch_encode_metadata drains a response.
    Then:
        It should bound the request by that value rather than the default.
    """
    # Arrange
    monkeypatch.setenv(TIMEOUT_ENV, "7200")
    session = _StreamingSession(lines=_tsv("ENCFF1\tbed"))
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    [row async for row in fetch_encode_metadata()]

    # Assert
    assert session.get_kwargs[0]["timeout"].total == 7200


@pytest.mark.asyncio
async def test_fetch_encode_metadata_should_fall_back_to_the_default_when_unset(
    mocker, monkeypatch
):
    """Test the default applies when no override is present.

    Given:
        No timeout variable in the environment.
    When:
        fetch_encode_metadata drains a response.
    Then:
        It should bound the request by the one-hour default.
    """
    # Arrange
    monkeypatch.delenv(TIMEOUT_ENV, raising=False)
    session = _StreamingSession(lines=_tsv("ENCFF1\tbed"))
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    [row async for row in fetch_encode_metadata()]

    # Assert
    assert session.get_kwargs[0]["timeout"].total == 3600


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value", ["not-a-number", "0", "-1"], ids=["malformed", "zero", "negative"]
)
async def test_fetch_encode_metadata_should_reject_an_unusable_timeout(
    mocker, monkeypatch, value
):
    """Test a misconfigured budget fails the sync rather than the API.

    aiohttp treats a non-positive total as no timeout at all, so a typo that
    parsed to zero would turn a bounded request into an unbounded one --
    failing in the direction hardest to notice. Raising here rather than at
    import keeps a bad knob from taking down every read the API serves.

    Given:
        The timeout variable set to a non-integer, zero, or a negative.
    When:
        fetch_encode_metadata is drained.
    Then:
        It should raise ValueError naming the variable, without issuing a
        request.
    """
    # Arrange
    monkeypatch.setenv(TIMEOUT_ENV, value)
    session = _StreamingSession(lines=_tsv("ENCFF1\tbed"))
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act & assert
    with pytest.raises(ValueError, match=TIMEOUT_ENV):
        [row async for row in fetch_encode_metadata()]

    assert session.get_kwargs == []


@pytest.mark.asyncio
async def test_fetch_encode_metadata_should_yield_a_row_per_data_line(mocker):
    """Test the header is consumed and each remaining line becomes a row.

    Pins that the double is faithful to the parser: without it the timeout
    test above could pass against a stub that never exercised the loop.

    Given:
        A streamed body of a header plus two data lines.
    When:
        fetch_encode_metadata drains it.
    Then:
        It should yield one dict per data line, keyed by column name.
    """
    # Arrange
    session = _StreamingSession(lines=_tsv("ENCFF1\tbed", "ENCFF2\tbam"))
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    rows = [row async for row in fetch_encode_metadata()]

    # Assert
    assert rows == [
        {"File accession": "ENCFF1", "File format": "bed"},
        {"File accession": "ENCFF2", "File format": "bam"},
    ]


@pytest.mark.asyncio
async def test_fetch_encode_metadata_should_yield_nothing_when_only_a_header(
    mocker,
):
    """Test a header-only body is an empty corpus, not a malformed one.

    Given:
        A response carrying the header line and no data lines.
    When:
        fetch_encode_metadata drains it.
    Then:
        It should yield no rows and complete without raising, so an empty
        upstream is distinguishable from a broken response.
    """
    # Arrange
    session = _StreamingSession(lines=_tsv())
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    rows = [row async for row in fetch_encode_metadata()]

    # Assert
    assert rows == []


@pytest.mark.asyncio
async def test_fetch_encode_metadata_should_skip_blank_lines(mocker):
    """Test a blank line does not become an empty row.

    A real TSV ends with a trailing newline, so the stream's final line is
    routinely empty; parsing it would yield a row whose every column is
    empty and whose accession is missing.

    Given:
        A body with a blank line between data lines and a trailing blank.
    When:
        fetch_encode_metadata drains it.
    Then:
        It should yield only the populated rows.
    """
    # Arrange
    session = _StreamingSession(
        lines=["File accession\tFile format", "ENCFF1\tbed", "", "ENCFF2\tbam", ""]
    )
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    rows = [row async for row in fetch_encode_metadata()]

    # Assert
    assert rows == [
        {"File accession": "ENCFF1", "File format": "bed"},
        {"File accession": "ENCFF2", "File format": "bam"},
    ]


@pytest.mark.asyncio
async def test_fetch_encode_metadata_should_strip_crlf_line_endings(mocker):
    """Test a carriage return does not contaminate the last column.

    ENCODE serves the TSV over HTTP and the body may use CRLF. A stray
    trailing "\\r" would attach to every row's final field, so a format of
    "bed" would silently become "bed\\r" and never match a CV lookup.

    Given:
        A body whose lines end with CRLF.
    When:
        fetch_encode_metadata drains it.
    Then:
        It should yield values with no carriage return.
    """
    # Arrange
    session = _StreamingSession(
        lines=["File accession\tFile format\r", "ENCFF1\tbed\r"]
    )
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    rows = [row async for row in fetch_encode_metadata()]

    # Assert
    assert rows == [{"File accession": "ENCFF1", "File format": "bed"}]


@pytest.mark.asyncio
async def test_fetch_encode_metadata_should_report_how_far_it_got_when_it_times_out(
    mocker, caplog
):
    """Test a timeout names the row count it reached.

    The row count is the one fact that separates "the budget is too small"
    from "the endpoint is down", and the previous handler caught only
    ClientError, so a timeout surfaced as a bare traceback saying neither.

    Given:
        A stream that times out after two data lines.
    When:
        fetch_encode_metadata drains it.
    Then:
        It should log the count reached and re-raise TimeoutError unchanged,
        so the caller's handling does not depend on this logging.
    """
    # Arrange
    session = _StreamingSession(
        lines=_tsv("ENCFF1\tbed", "ENCFF2\tbam", "ENCFF3\tbed"), fail_after=3
    )
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act & assert
    with caplog.at_level(logging.ERROR), pytest.raises(asyncio.TimeoutError):
        [row async for row in fetch_encode_metadata()]

    assert "timed out after 2 rows" in caplog.text


@pytest.mark.asyncio
async def test_fetch_encode_metadata_should_raise_when_the_response_is_not_ok(
    mocker,
):
    """Test a non-200 aborts rather than yielding an empty corpus.

    Given:
        A metadata endpoint returning HTTP 503.
    When:
        fetch_encode_metadata is drained.
    Then:
        It should raise naming the status, so the sync fails instead of
        clearing the DCC and reloading nothing.
    """
    # Arrange
    session = _StreamingSession(status=503)
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act & assert
    with pytest.raises(Exception, match="503"):
        [row async for row in fetch_encode_metadata()]


@pytest.mark.asyncio
async def test_fetch_encode_metadata_should_wrap_a_network_error(mocker):
    """Test a transport failure is reported as such.

    Given:
        A session whose request raises aiohttp.ClientError.
    When:
        fetch_encode_metadata is drained.
    Then:
        It should raise naming the network error.
    """
    # Arrange
    session = _StreamingSession(error=aiohttp.ClientError("connection reset"))
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act & assert
    with pytest.raises(Exception, match="network error"):
        [row async for row in fetch_encode_metadata()]


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


def test_transform_to_c2m2_should_set_the_gzip_term_when_the_download_url_is_gzipped():
    """Test that a gzipped row carries gzip alongside the uncompressed format.

    Given:
        An ENCODE row for a narrowPeak file published as ".bed.gz".
    When:
        transform_to_c2m2 is called.
    Then:
        It should set compression_format to the gzip term while file_format
        stays the uncompressed EDAM format.
    """
    # Arrange
    row = _encode_row()

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc["compression_format"] == "format:3989"
    assert doc["file_format"] == {"id": "format:3613", "name": "NarrowPeak"}


def test_transform_to_c2m2_should_report_uncompressed_when_the_url_carries_no_suffix():
    """Test the sentinel on a row whose download URL names a plain file.

    Given:
        An ENCODE row whose download URL ends in ".bam".
    When:
        transform_to_c2m2 is called.
    Then:
        It should set compression_format to the uncompressed sentinel.
    """
    # Arrange
    row = _encode_row(
        **{"File download URL": DOWNLOAD_URL.format(name="ENCFF123ABC.bam")}
    )

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc["compression_format"] == UNCOMPRESSED


@pytest.mark.parametrize(
    "url",
    [
        DOWNLOAD_URL.format(name="ENCFF123ABC.bed.starch"),
        "https://www.encodeproject.org/files/ENCFF123ABC/@@download/",
        "",
    ],
    ids=["unmappable-suffix", "no-filename", "no-url"],
)
def test_transform_to_c2m2_should_omit_compression_format_when_undetermined(url):
    """Test that an undetermined compression is left absent, not stored as null.

    Given:
        An ENCODE row whose download URL ends in a compression suffix EDAM
        cannot name, carries no filename, or is missing entirely.
    When:
        transform_to_c2m2 is called.
    Then:
        The document should omit compression_format, so an undetermined file
        never surfaces as a null the compressionFormat filter cannot select.
    """
    # Arrange
    row = _encode_row(**{"File download URL": url})

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert "compression_format" not in doc


def test_transform_to_c2m2_should_not_derive_from_the_synthesized_filename():
    """Test that a filename the transform invented is not treated as evidence.

    Given:
        An ENCODE row with a file format but no download URL, so the filename
        is synthesized from the accession and can carry no suffix.
    When:
        transform_to_c2m2 is called.
    Then:
        It should leave compression_format absent rather than claim the file
        is uncompressed on the strength of a name it made up.
    """
    # Arrange
    row = _encode_row(**{"File download URL": "", "File format": "bed"})

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc["access_url"] is None
    assert doc["filename"] == "ENCFF123ABC.bed"
    assert "compression_format" not in doc


def test_transform_to_c2m2_should_return_none_when_the_row_has_no_accession():
    """Test that the accession guard still short-circuits the transformation.

    Given:
        A row with a gzipped download URL but no file accession.
    When:
        transform_to_c2m2 is called.
    Then:
        It should return None without deriving anything.
    """
    # Arrange
    row = _encode_row(**{"File accession": "   "})

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc is None


@given(st.text())
@settings(max_examples=200)
def test_transform_to_c2m2_should_not_raise_for_any_download_url(url):
    """Test that no download URL can abort an ENCODE sync.

    Given:
        Any text in the download URL column of an otherwise valid row.
    When:
        transform_to_c2m2 is called.
    Then:
        It should return a document whose compression_format, when present,
        is drawn from the closed vocabulary, without raising.
    """
    # Arrange
    row = _encode_row(**{"File download URL": url})

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc.get("compression_format", UNCOMPRESSED) in DERIVED_VALUES


def test_transform_to_c2m2_should_set_accession_id_on_the_file():
    """Test that the ENCODE file accession lands on accession_id.

    Given:
        An ENCODE row carrying a File accession.
    When:
        transform_to_c2m2 is called.
    Then:
        It should set accession_id to that accession, giving ENCODE the same
        cross-DCC query field 4DN gets from its persistent_id.
    """
    # Arrange
    row = _encode_row()

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc["accession_id"] == "ENCFF123ABC"


def test_transform_to_c2m2_should_set_accession_id_on_the_experiment_collection():
    """Test that the experiment accession lands on the collection.

    Given:
        An ENCODE row naming an Experiment accession, and a Biosample term
        name -- which the collection block is gated on, so the accession
        alone would build no collection at all.
    When:
        transform_to_c2m2 is called.
    Then:
        It should set accession_id on the built collection, so a collection
        accession filter resolves for ENCODE as it does for 4DN.
    """
    # Arrange
    row = _encode_row(
        **{"Experiment accession": "ENCSR918ZSJ", "Biosample term name": "K562"}
    )

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc["collections"][0]["accession_id"] == "ENCSR918ZSJ"


def test_transform_to_c2m2_should_not_set_accession_id_on_a_biosample_collection():
    """Test that the synthesized fallback collection gets no accession.

    Given:
        An ENCODE row with no Experiment accession, so the collection is keyed
        on the biosample term instead.
    When:
        transform_to_c2m2 is called.
    Then:
        It should leave accession_id unset on that collection, since it names
        no ENCODE experiment and a fabricated accession would be wrong.
    """
    # Arrange
    row = _encode_row(**{"Biosample term name": "K562"})

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc["collections"]
    assert "accession_id" not in doc["collections"][0]


def test_transform_to_c2m2_should_not_reintroduce_padding_on_the_accession_id():
    """Test that the builder does not re-pad an already-stripped accession.

    The stripping itself happens upstream in ``_nonempty``, which every
    accession cell is read through, so this does not pin normalize_accession's
    whitespace handling -- deleting .strip() from it leaves this test green.
    That contract is pinned in tests/test_accessions.py. What this covers is
    the builder: that nothing between the cell and the stored field puts the
    padding back.

    Given:
        A row whose File accession carries surrounding whitespace, as a
        hand-edited TSV cell can.
    When:
        transform_to_c2m2 is called.
    Then:
        It should store the stripped accession.
    """
    # Arrange
    row = _encode_row(**{"File accession": "  ENCFF123ABC  "})

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc["accession_id"] == "ENCFF123ABC"


def test_transform_to_c2m2_should_fold_accession_id_without_rewriting_local_id():
    """Test that the two fields legitimately disagree in case.

    Given:
        A row whose File accession is published in lower case.
    When:
        transform_to_c2m2 is called.
    Then:
        It should fold accession_id for matching while leaving local_id as
        published, since local_id is the DCC's own identifier and rewriting
        it would change the document's key.
    """
    # Arrange
    row = _encode_row(**{"File accession": "encff123abc"})

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc["accession_id"] == "ENCFF123ABC"
    assert doc["local_id"] == "encff123abc"


def test_transform_to_c2m2_should_fold_the_experiment_collection_accession():
    """Test that the collection accession is folded like the file's.

    The upper-casing is the load-bearing half: the padding was already
    removed upstream by ``_nonempty``, so only the case change is evidence
    that the collection branch routes through the shared fold rather than
    storing the cell as published.

    Given:
        A row whose Experiment accession is lower-cased and padded, with a
        biosample term present so the collection is built at all.
    When:
        transform_to_c2m2 is called.
    Then:
        It should store the stripped, upper-cased experiment accession.
    """
    # Arrange
    row = _encode_row(
        **{
            "Experiment accession": " encsr918zsj ",
            "Biosample term name": "K562",
        }
    )

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc["collections"][0]["accession_id"] == "ENCSR918ZSJ"


def test_transform_to_c2m2_should_build_no_collection_without_a_biosample_term():
    """Test that an experiment accession alone yields no collection.

    The whole collection block is gated on the biosample term name, so a
    row shaped like an ENCODE annotation or reference contributes no
    collection and its experiment accession is not queryable anywhere.
    Pre-existing behavior, pinned here because the accession field turns it
    from a cosmetic gap into a data-completeness question.

    Given:
        A row carrying an Experiment accession but no Biosample term name.
    When:
        transform_to_c2m2 is called.
    Then:
        It should produce an empty collections list.
    """
    # Arrange
    row = _encode_row(**{"Experiment accession": "ENCSR918ZSJ"})

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc["collections"] == []


@given(
    accession=st.text(
        alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
        min_size=1,
        max_size=16,
    ),
    pad=st.text(alphabet=" \t", max_size=3),
)
@settings(max_examples=100)
def test_transform_to_c2m2_should_store_the_accession_case_folded(accession, pad):
    """Test that the builder routes the accession through the shared fold.

    Both sides of the assertion call normalize_accession on the same input,
    so this pins that transform_to_c2m2 does not *bypass* the shared fold
    -- not which direction that fold goes. It cannot detect a change of
    fold direction, because the expectation moves with it: inverting
    normalize_accession to .lower() fails 37 tests elsewhere and leaves
    this one green. The direction is pinned against literals by
    ...should_fold_accession_id_without_rewriting_local_id below and by the
    query-side round trip in tests/test_inputs.py.

    Given:
        Any File accession in arbitrary casing with arbitrary padding.
    When:
        transform_to_c2m2 is called.
    Then:
        accession_id should equal the folded local_id.
    """
    # Arrange
    row = _encode_row(**{"File accession": f"{pad}{accession}{pad}"})

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc["accession_id"] == normalize_accession(doc["local_id"])
