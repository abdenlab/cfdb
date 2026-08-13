"""Tests for the ENCODE metadata fetch, compression derivation and transform."""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import parse_qs, urlsplit

import aiohttp
import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from cfdb.accessions import normalize_accession
from cfdb.models import FileMetadataModel
from cfdb.services import encode as encode_module
from cfdb.services.encode import (
    COMPRESSION_SUFFIX_TO_EDAM,
    UNCOMPRESSED,
    UNMAPPABLE_COMPRESSION_SUFFIXES,
    annotation_types_from_env,
    derive_compression_format,
    fetch_encode_annotation_metadata,
    fetch_encode_metadata,
    transform_annotation_to_c2m2,
    transform_to_c2m2,
)

#: The variable the metadata fetch reads its budget from. Named here rather
#: than imported so the tests pin the operator-facing contract: renaming it in
#: the module should fail these tests, not silently follow along.
TIMEOUT_ENV = "ENCODE_METADATA_TIMEOUT_SECONDS"

#: The variable bounding which annotation types are ingested. Named here for
#: the same reason as TIMEOUT_ENV.
ANNOTATION_TYPES_ENV = "ENCODE_ANNOTATION_TYPES"

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

    def __init__(self, lines, fail_after: int | None = None):
        # Any iterable, not just a list: a body large enough to trip the
        # per-50000-row progress log would otherwise have to be
        # materialized in memory just to be thrown away line by line.
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

    Records the URL and kwargs of every ``get`` -- unlike the 4DN fakes,
    which discard them -- because the request's timeout and the query it
    carries are both behaviors under test. ``error`` raises instead of
    responding, modelling a network failure before any body arrives.
    """

    def __init__(self, lines=None, status=200, fail_after=None, error=None):
        self._lines = lines or []
        self._status = status
        self._fail_after = fail_after
        self._error = error
        self.get_kwargs: list[dict] = []
        self.get_urls: list[str] = []
        #: Times ``__aexit__`` ran. The stream owns its session inside an
        #: ``async with``, so this is the only observable signal that an
        #: abandoned or failed stream released it rather than leaving it to
        #: the garbage collector.
        self.exit_count = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.exit_count += 1
        return False

    def get(self, url, **kwargs):
        self.get_urls.append(url)
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
async def test_fetch_encode_metadata_should_request_only_the_deadline_that_remains(
    mocker, monkeypatch
):
    """Test a shared deadline bounds the request, not a fresh full budget.

    A sync runs one stream per configured annotation type plus one for
    experiments, all inside the cutover lock that gates the read surface.
    Granting each its own hour would multiply the outage by the phase count.

    Given:
        A deadline half a minute out, against an hour-long budget.
    When:
        fetch_encode_metadata drains a response.
    Then:
        It should bound the request by the remaining time, not the budget.
    """
    # Arrange
    monkeypatch.delenv(TIMEOUT_ENV, raising=False)
    session = _StreamingSession(lines=_tsv("ENCFF1\tbed"))
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )
    deadline = asyncio.get_running_loop().time() + 30

    # Act
    [row async for row in fetch_encode_metadata(deadline=deadline)]

    # Assert
    assert session.get_kwargs[0]["timeout"].total <= 30


@pytest.mark.asyncio
async def test_fetch_encode_metadata_should_refuse_a_spent_deadline(
    mocker, monkeypatch
):
    """Test an exhausted budget stops the fetch instead of going unbounded.

    aiohttp reads a non-positive total as "no timeout", so handing it the
    remainder of a spent budget would turn the exhausted case into an
    unbounded request -- the failure the budget exists to prevent.

    Given:
        A deadline that has already passed.
    When:
        fetch_encode_metadata is drained.
    Then:
        It should raise TimeoutError without opening a request.
    """
    # Arrange
    monkeypatch.delenv(TIMEOUT_ENV, raising=False)
    session = _StreamingSession(lines=_tsv("ENCFF1\tbed"))
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )
    deadline = asyncio.get_running_loop().time() - 1

    # Act & assert
    with pytest.raises(asyncio.TimeoutError, match="budget was already spent"):
        [row async for row in fetch_encode_metadata(deadline=deadline)]

    assert session.get_kwargs == []


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
    # Names its stream: with several streams per sync, a timeout message
    # that does not say which one died is not actionable.
    assert "experiment" in caplog.text


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

    On the experiment path the whole collection block is gated on the
    biosample term name, so such a row contributes no collection and its
    accession is queryable nowhere. Long-standing behavior, pinned here as
    the other half of ``require_biosample``: the annotation path passes
    False and deliberately does build the collection (see
    ...should_build_a_dataset_without_a_biosample), so this test is what
    stops that choice from silently leaking onto experiments.

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


# ---------------------------------------------------------------------------
# Annotation ingest
# ---------------------------------------------------------------------------

#: Download URL of the real released cCRE file the annotation fixture is
#: modelled on.
ANNOTATION_DOWNLOAD_URL = (
    "https://www.encodeproject.org/files/ENCFF845ITU/@@download/ENCFF845ITU.bigBed"
)

#: Text an environment variable can actually hold: ``os.environ`` rejects an
#: embedded NUL outright and cannot encode a lone surrogate. Generating
#: either tests the harness rather than the allowlist parser.
ENV_SAFE_TEXT = st.text(
    alphabet=st.characters(exclude_characters="\x00", codec="utf-8"),
    max_size=80,
)


def _annotation_row(**overrides) -> dict:
    """Return a realistic ENCODE annotation metadata TSV row.

    The full 32-column annotation header, verified against the live TSVs of
    both ingested types, with values from a released cCRE file -- so the
    fixture carries the shapes the corpus actually contains: an empty Assay
    term name, a multi-valued Targets, a non-human organism.
    """
    row = {
        "File accession": "ENCFF845ITU",
        "File format": "bigBed bed9+",
        "Output type": "candidate Cis-Regulatory Elements",
        "Assay term name": "",
        "Dataset accession": "ENCSR026KJM",
        "Annotation type": "candidate Cis-Regulatory Elements",
        "Software used": "",
        "Encyclopedia Version": "ENCODE v1",
        "Biosample term id": "UBERON:0002048",
        "Biosample term name": "lung",
        "Biosample type": "tissue",
        "Life stage": "postnatal",
        "Age": "0",
        "Age units": "day",
        "Organism": "Mus musculus",
        "Targets": "H3K4me3-mouse, CTCF-mouse",
        "Dataset date released": "2017-09-12",
        "Project": "ENCODE",
        "Lab": "Zhiping Weng, UMass",
        "md5sum": "5ff392dcde69f8ec512ea381928674d9",
        "dbxrefs": "",
        "File download URL": ANNOTATION_DOWNLOAD_URL,
        "Assembly": "mm10",
        "Controlled by": "",
        "File Status": "released",
        "Derived from": "/files/ENCFF728HFF/",
        "S3 URL": "https://encode-public.s3.amazonaws.com/2017/ENCFF845ITU.bigBed",
        "Azure URL": "",
        "Size": "13743825",
        "Audit WARNING": "",
        "Audit NOT_COMPLIANT": "",
        "Audit ERROR": "",
    }
    row.update(overrides)
    return row


def test_annotation_types_from_env_should_return_a_bounded_default_when_unset(
    monkeypatch,
):
    """Test the allowlist is never implicitly the whole annotation space.

    ENCODE publishes 580,910 annotation datasets, 86% of them footprints.
    Defaulting to all of them would be a corpus-scale mistake to undo, so
    the default has to be an explicit, small set -- asserted by equality,
    not by "non-empty and not footprints", which would also pass if a third
    type were quietly added to the default.

    Given:
        No ENCODE_ANNOTATION_TYPES in the environment.
    When:
        annotation_types_from_env is called.
    Then:
        It should return exactly the two documented default types.
    """
    # Arrange
    monkeypatch.delenv(ANNOTATION_TYPES_ENV, raising=False)

    # Act
    types = annotation_types_from_env()

    # Assert
    assert types == (
        "candidate Cis-Regulatory Elements",
        "element gene regulatory interaction predictions",
    )


def test_annotation_types_from_env_should_honor_an_override(monkeypatch):
    """Test which types are ingested is configuration, not a code change.

    Given:
        ENCODE_ANNOTATION_TYPES naming two types.
    When:
        annotation_types_from_env is called.
    Then:
        It should return exactly those two, in order.
    """
    # Arrange
    monkeypatch.setenv(ANNOTATION_TYPES_ENV, "chromatin state,footprints")

    # Act
    types = annotation_types_from_env()

    # Assert
    assert types == ("chromatin state", "footprints")


def test_annotation_types_from_env_should_strip_padding_and_drop_blanks(monkeypatch):
    """Test a human-written list is read the way it was meant.

    Given:
        An override written with spaces after the commas and a trailing
        comma.
    When:
        annotation_types_from_env is called.
    Then:
        It should return the trimmed values with no empty entry.
    """
    # Arrange
    monkeypatch.setenv(ANNOTATION_TYPES_ENV, " chromatin state , footprints ,")

    # Act
    types = annotation_types_from_env()

    # Assert
    assert types == ("chromatin state", "footprints")


def test_annotation_types_from_env_should_disable_ingest_when_set_empty(monkeypatch):
    """Test an operator can turn the annotation path off without a deploy.

    Given:
        ENCODE_ANNOTATION_TYPES set to an empty value -- distinct from
        unset, which yields the default allowlist.
    When:
        annotation_types_from_env is called.
    Then:
        It should return no types at all.
    """
    # Arrange
    monkeypatch.setenv(ANNOTATION_TYPES_ENV, "")

    # Act
    types = annotation_types_from_env()

    # Assert
    assert types == ()


def test_annotation_types_from_env_should_warn_when_set_empty(monkeypatch, caplog):
    """Test an accidentally empty allowlist is visible rather than inferable.

    The neighbouring timeout variable reads an empty value as "unset, use
    the default" and this one reads it as "ingest nothing" -- a difference
    an operator has no reason to expect. Empty values arrive by accident
    routinely, from an unset CloudFormation parameter to a docker-compose
    expansion, and the only other trace is an absence in the per-phase log.

    Given:
        ENCODE_ANNOTATION_TYPES set to an empty value.
    When:
        annotation_types_from_env is called.
    Then:
        It should warn, naming the variable and the disabled ingest.
    """
    # Arrange
    monkeypatch.setenv(ANNOTATION_TYPES_ENV, "")

    # Act
    with caplog.at_level(logging.WARNING, logger=encode_module.logger.name):
        annotation_types_from_env()

    # Assert
    assert ANNOTATION_TYPES_ENV in caplog.text
    assert "annotation ingest is disabled" in caplog.text


def test_annotation_types_from_env_should_not_warn_when_unset(monkeypatch, caplog):
    """Test the default allowlist is not reported as a disabled ingest.

    Given:
        No ENCODE_ANNOTATION_TYPES in the environment.
    When:
        annotation_types_from_env is called.
    Then:
        It should emit no warning.
    """
    # Arrange
    monkeypatch.delenv(ANNOTATION_TYPES_ENV, raising=False)

    # Act
    with caplog.at_level(logging.WARNING, logger=encode_module.logger.name):
        annotation_types_from_env()

    # Assert
    assert caplog.text == ""


@pytest.mark.asyncio
async def test_fetch_encode_annotation_metadata_should_query_the_requested_type(
    mocker,
):
    """Test the request selects annotations of one type, not experiments.

    Given:
        A streamed metadata response and an annotation type containing the
        spaces and capitals ENCODE's vocabulary actually uses.
    When:
        fetch_encode_annotation_metadata drains it.
    Then:
        It should have requested released Annotations with the type
        URL-encoded rather than interpolated raw, and carry no other
        parameter.
    """
    # Arrange
    session = _StreamingSession(lines=_tsv("ENCFF1\tbed"))
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    [
        row
        async for row in fetch_encode_annotation_metadata(
            "candidate Cis-Regulatory Elements"
        )
    ]

    # Assert
    url = session.get_urls[0]
    parsed = urlsplit(url)
    assert parsed.path == "/metadata/"
    # Equality, not substring containment: a substring check passes against
    # a URL that also carries a stray or duplicated parameter.
    assert parse_qs(parsed.query) == {
        "type": ["Annotation"],
        "status": ["released"],
        "annotation_type": ["candidate Cis-Regulatory Elements"],
    }
    assert " " not in url


@pytest.mark.asyncio
async def test_fetch_encode_annotation_metadata_should_yield_a_row_per_data_line(
    mocker,
):
    """Test the annotation stream parses the way the experiment one does.

    Given:
        A streamed annotation TSV with a header and two data lines.
    When:
        fetch_encode_annotation_metadata drains it.
    Then:
        It should yield one dict per data line, keyed by column name.
    """
    # Arrange
    session = _StreamingSession(lines=_tsv("ENCFF1\tbed", "ENCFF2\tbigBed"))
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    rows = [row async for row in fetch_encode_annotation_metadata("chromatin state")]

    # Assert
    assert rows == [
        {"File accession": "ENCFF1", "File format": "bed"},
        {"File accession": "ENCFF2", "File format": "bigBed"},
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "session_kwargs",
    [
        {"status": 503},
        {"error": aiohttp.ClientError("connection reset")},
        {"lines": _tsv("ENCFF1\tbed"), "fail_after": 1},
    ],
    ids=["http-error", "network-error", "timeout"],
)
async def test_fetch_encode_annotation_metadata_should_release_the_session_on_failure(
    mocker, session_kwargs
):
    """Test a failed stream does not leave its HTTP session open.

    The stream owns an aiohttp session inside its own context manager, and
    the sync isolates phase failures with a broad except that abandons the
    generator mid-iteration. With one stream per sync that was academic;
    with one per annotation type plus the experiment, a leak per failure
    accumulates in a long-lived API process.

    Given:
        An annotation stream that fails by HTTP status, by network error,
        or by timeout.
    When:
        It is drained and the error escapes.
    Then:
        The session should have been exited in every case.
    """
    # Arrange
    session = _StreamingSession(**session_kwargs)
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    with pytest.raises((Exception, asyncio.TimeoutError)):
        [row async for row in fetch_encode_annotation_metadata("chromatin state")]

    # Assert
    assert session.exit_count == 1


@pytest.mark.asyncio
async def test_fetch_encode_annotation_metadata_should_release_the_session_when_abandoned(
    mocker,
):
    """Test a stream abandoned part-way still releases its session.

    This is the shape the sync's phase isolation produces: the consumer
    stops iterating because something else raised, not because the stream
    ended.

    Given:
        A streamed annotation body of several rows.
    When:
        The consumer takes one row and closes the generator.
    Then:
        The session should have been exited.
    """
    # Arrange
    session = _StreamingSession(lines=_tsv("ENCFF1\tbed", "ENCFF2\tbed"))
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )
    stream = fetch_encode_annotation_metadata("chromatin state")

    # Act
    async for _ in stream:
        break
    await stream.aclose()

    # Assert
    assert session.exit_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "session_kwargs, expectation",
    [
        ({"status": 503}, "503"),
        ({"error": aiohttp.ClientError("connection reset")}, "network error"),
    ],
    ids=["http-error", "network-error"],
)
async def test_fetch_encode_annotation_metadata_should_name_its_stream_when_it_fails(
    mocker, session_kwargs, expectation
):
    """Test a failure message identifies which stream produced it.

    With several streams per sync, a message that does not say which one
    died leaves an operator to guess between the experiment corpus and any
    of the configured annotation types.

    Given:
        An annotation stream for a named type that fails.
    When:
        It is drained.
    Then:
        The error should name both the failure and the annotation type.
    """
    # Arrange
    session = _StreamingSession(**session_kwargs)
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act & assert
    with pytest.raises(Exception, match=r"annotation\[chromatin state\]"):
        [row async for row in fetch_encode_annotation_metadata("chromatin state")]

    with pytest.raises(Exception, match=expectation):
        [row async for row in fetch_encode_annotation_metadata("chromatin state")]


@pytest.mark.asyncio
async def test_fetch_encode_annotation_metadata_should_name_its_stream_on_timeout(
    mocker, caplog
):
    """Test a timed-out annotation stream names its type and its progress.

    Given:
        An annotation stream that times out after one data row.
    When:
        It is drained.
    Then:
        The log should name the annotation type alongside the row count.
    """
    # Arrange
    session = _StreamingSession(
        lines=_tsv("ENCFF1\tbed", "ENCFF2\tbed"), fail_after=2
    )
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act & assert
    with caplog.at_level(logging.ERROR), pytest.raises(asyncio.TimeoutError):
        [row async for row in fetch_encode_annotation_metadata("chromatin state")]

    assert "annotation[chromatin state]" in caplog.text
    assert "timed out after 1 rows" in caplog.text


@pytest.mark.asyncio
async def test_fetch_encode_annotation_metadata_should_not_let_a_type_inject_parameters(
    mocker,
):
    """Test a configured value cannot smuggle extra query parameters.

    The annotation type is operator-supplied configuration interpolated
    into a URL. Were it not encoded, a value containing an ampersand could
    append parameters -- widening status past released, or flipping the
    type back to Experiment.

    Given:
        An annotation type containing URL metacharacters.
    When:
        The stream is drained.
    Then:
        The query should still carry exactly three parameters with the
        intended type and status.
    """
    # Arrange
    hostile = "x&status=deleted&type=Experiment"
    session = _StreamingSession(lines=_tsv("ENCFF1\tbed"))
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    [row async for row in fetch_encode_annotation_metadata(hostile)]

    # Assert
    assert parse_qs(urlsplit(session.get_urls[0]).query) == {
        "type": ["Annotation"],
        "status": ["released"],
        "annotation_type": [hostile],
    }


@pytest.mark.asyncio
async def test_fetch_encode_metadata_should_request_the_released_experiment_corpus(
    mocker,
):
    """Test the experiment URL is unchanged by the annotation work.

    The annotation fetch builds its query with urlencode while this one
    keeps a hand-built literal. The obvious future tidy-up is to unify
    them, which would change escaping or parameter order with nothing else
    to catch it.

    Given:
        A streamed metadata response.
    When:
        fetch_encode_metadata drains it.
    Then:
        It should have requested exactly the released-experiment URL.
    """
    # Arrange
    session = _StreamingSession(lines=_tsv("ENCFF1\tbed"))
    mocker.patch.object(
        encode_module.aiohttp, "ClientSession", return_value=session
    )

    # Act
    [row async for row in fetch_encode_metadata()]

    # Assert
    assert session.get_urls[0] == (
        "https://www.encodeproject.org/metadata/?type=Experiment&status=released"
    )


@pytest.mark.parametrize("raw", ["   ", ",,,", " , , ", "\t"])
def test_annotation_types_from_env_should_disable_ingest_when_all_entries_blank(
    monkeypatch, raw
):
    """Test a whitespace-only allowlist disables ingest rather than widening it.

    A blank entry surviving into the allowlist would request
    ``annotation_type=``, which the portal is liable to read as unfiltered
    -- 580,910 datasets, the exact outcome the allowlist exists to prevent.

    Given:
        An override consisting only of separators and whitespace.
    When:
        annotation_types_from_env is called.
    Then:
        It should return no types at all.
    """
    # Arrange
    monkeypatch.setenv(ANNOTATION_TYPES_ENV, raw)

    # Act
    types = annotation_types_from_env()

    # Assert
    assert types == ()


def test_annotation_types_from_env_should_collapse_a_repeated_type(monkeypatch):
    """Test a repeated entry yields one ingest phase, not two.

    Given:
        An override naming the same type twice with differing padding.
    When:
        annotation_types_from_env is called.
    Then:
        It should return that type once, keeping first-occurrence order.
    """
    # Arrange
    monkeypatch.setenv(ANNOTATION_TYPES_ENV, "alpha, beta ,alpha")

    # Act
    types = annotation_types_from_env()

    # Assert
    assert types == ("alpha", "beta")


@given(annotation_type=ENV_SAFE_TEXT.filter(lambda s: s.strip()))
# monkeypatch is function-scoped and so is not reset between generated
# examples. Safe here: every example overwrites the same single variable
# rather than accumulating state, and the fixture still restores the real
# environment once at the end.
@settings(
    max_examples=100,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_annotation_types_from_env_should_round_trip_any_single_type(
    monkeypatch, annotation_type
):
    """Test any comma-free type survives configuration unchanged.

    ENCODE's vocabulary is case- and space-significant, so a well-meaning
    normalization would silently stop matching upstream.

    Given:
        Any non-blank text containing no comma.
    When:
        It is set as the allowlist and read back.
    Then:
        It should come back as its own stripped form, unaltered otherwise.
    """
    # Arrange
    assume("," not in annotation_type)
    monkeypatch.setenv(ANNOTATION_TYPES_ENV, annotation_type)

    # Act
    types = annotation_types_from_env()

    # Assert
    assert types == (annotation_type.strip(),)


@given(raw=ENV_SAFE_TEXT)
@settings(
    max_examples=200,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_annotation_types_from_env_should_never_yield_a_blank_entry(monkeypatch, raw):
    """Test no configuration can produce an unfiltered annotation query.

    Given:
        Any text at all, including comma-and-whitespace soup.
    When:
        annotation_types_from_env is called.
    Then:
        Every returned entry should be non-blank and already stripped.
    """
    # Arrange
    monkeypatch.setenv(ANNOTATION_TYPES_ENV, raw)

    # Act
    types = annotation_types_from_env()

    # Assert
    assert all(entry and entry == entry.strip() for entry in types)


def test_transform_annotation_to_c2m2_should_map_the_renamed_columns():
    """Test the renamed columns reach the fields their twins do.

    The annotation TSV publishes the same data as the experiment TSV under
    different column names. A missed rename is silent -- the field simply
    ends up unset -- so each one is pinned.

    Given:
        An annotation row carrying the renamed columns.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        Each value should land where its experiment-named twin would.
    """
    # Arrange
    row = _annotation_row()

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert doc["collections"][0]["local_id"] == "ENCSR026KJM"  # Dataset accession
    assert doc["genome_assembly"] == "mm10"  # Assembly
    assert doc["creation_time"] == "2017-09-12"  # Dataset date released
    assert doc["extra"]["encode"]["s3_uri"].endswith("ENCFF845ITU.bigBed")  # S3 URL
    assert doc["extra"]["encode"]["organism"] == "Mus musculus"  # Organism


def test_transform_annotation_to_c2m2_should_map_the_assay_term_name_rename():
    """Test Assay term name is read as the experiment path reads Assay.

    Given:
        An annotation row whose Assay term name is populated, as the
        interaction-prediction type's rows are.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        The dataset should carry it as its experiment_type.
    """
    # Arrange
    row = _annotation_row(**{"Assay term name": "DNase-seq"})

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert doc["collections"][0]["experiment_type"] == "DNase-seq"


def test_transform_annotation_to_c2m2_should_make_annotation_type_queryable():
    """Test the field that gives an annotation its meaning is stored.

    Without it a client can only find cCRE files by string-matching
    filenames, which is the gap the annotation ingest exists to close.

    Given:
        A cCRE annotation row.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        annotation_type should be set on both the file and its dataset.
    """
    # Arrange
    row = _annotation_row()

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    expected = "candidate Cis-Regulatory Elements"
    assert doc["extra"]["encode"]["annotation_type"] == expected
    assert doc["collections"][0]["extra"]["encode"]["annotation_type"] == expected


def test_transform_annotation_to_c2m2_should_store_the_dataset_only_fields():
    """Test the dataset-scoped annotation-only columns are preserved.

    Given:
        An annotation row naming its encyclopedia version and the software
        that produced it.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        Both should be stored on the dataset.
    """
    # Arrange
    row = _annotation_row(
        **{
            "Software used": "ABC-Enhancer-Gene-Prediction",
            "Encyclopedia Version": "ENCODE v4",
        }
    )

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    dataset_extra = doc["collections"][0]["extra"]["encode"]
    assert dataset_extra["software_used"] == "ABC-Enhancer-Gene-Prediction"
    assert dataset_extra["encyclopedia_version"] == "ENCODE v4"


def test_transform_annotation_to_c2m2_should_carry_targets_as_experiment_target():
    """Test Targets reuses the existing scalar rather than a parallel field.

    Given:
        An annotation row whose Targets column lists several targets.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        The dataset's experiment_target should hold the value verbatim.
    """
    # Arrange
    row = _annotation_row(**{"Targets": "H3K4me3-mouse, CTCF-mouse"})

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert doc["collections"][0]["experiment_target"] == "H3K4me3-mouse, CTCF-mouse"


def test_transform_annotation_to_c2m2_should_carry_donor_traits_on_the_biosample():
    """Test the age columns are preserved as published.

    They are kept as strings, not parsed: the released corpus contains
    "2-4" and "unknown" alongside decimals, which is also why they cannot
    go to Subject.age_at_sampling.

    Given:
        An annotation row whose Age is a range rather than a number.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        All three should sit on the biosample, verbatim.
    """
    # Arrange
    row = _annotation_row(
        **{"Life stage": "embryonic", "Age": "2-4", "Age units": "week"}
    )

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    biosample_extra = doc["collections"][0]["biosamples"][0]["extra"]["encode"]
    assert biosample_extra["life_stage"] == "embryonic"
    assert biosample_extra["age"] == "2-4"
    assert biosample_extra["age_units"] == "week"


def test_transform_annotation_to_c2m2_should_leave_experiment_only_fields_unset():
    """Test fields the annotation TSV cannot supply are absent, not invented.

    Given:
        An annotation row, whose TSV publishes none of the library,
        replicate, genetic-modification or analysis columns.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        Those fields should be absent rather than filled with a derived or
        default value.
    """
    # Arrange
    row = _annotation_row()

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    for absent in (
        "genome_annotation",
        "output_type_detail",
        "biological_replicates",
        "technical_replicates",
    ):
        assert absent not in doc
    file_extra = doc["extra"]["encode"]
    for absent in (
        "read_length",
        "mapped_read_length",
        "run_type",
        "paired_end",
        "paired_with",
        "index_of",
        "file_analysis_title",
        "file_analysis_status",
    ):
        assert absent not in file_extra
    biosample_extra = doc["collections"][0]["biosamples"][0]["extra"]["encode"]
    assert not any(key.startswith("library_") for key in biosample_extra)
    assert "biosample_genetic_modifications" not in biosample_extra
    assert "analyte_class" not in doc["collections"][0]


def test_transform_annotation_to_c2m2_should_build_no_subject_without_a_donor():
    """Test no donor is fabricated for a TSV that names none.

    The annotation TSV has no Donor(s) column at all, so there is nothing
    to key a Subject on.

    Given:
        An annotation row.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        Both the dataset and its biosample should carry no subjects.
    """
    # Arrange
    row = _annotation_row()

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    collection = doc["collections"][0]
    assert collection["subjects"] == []
    assert collection["biosamples"][0]["subjects"] == []


def test_transform_annotation_to_c2m2_should_link_to_the_annotations_path():
    """Test the dataset's persistent_id resolves.

    ENCODE serves annotations and experiments under different paths, so
    reusing the experiment path would mint a link that 404s.

    Given:
        An annotation row with a dataset accession.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        The dataset's persistent_id should point at /annotations/.
    """
    # Arrange
    row = _annotation_row()

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert doc["collections"][0]["persistent_id"] == (
        "https://www.encodeproject.org/annotations/ENCSR026KJM/"
    )


def test_transform_annotation_to_c2m2_should_build_a_dataset_without_a_biosample():
    """Test a dataset accession stays queryable with no biosample term.

    48 of the released cCRE files name no biosample term. Gating the
    dataset on one -- as the experiment path does -- would leave 24 dataset
    accessions unreachable.

    Given:
        An annotation row with a dataset accession but no biosample term.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        It should still build the dataset -- addressable, linkable and
        labelled -- carrying no biosamples.
    """
    # Arrange
    row = _annotation_row(
        **{"Biosample term name": "", "Biosample term id": "", "Biosample type": ""}
    )

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    collection = doc["collections"][0]
    assert collection["local_id"] == "ENCSR026KJM"
    assert collection["accession_id"] == "ENCSR026KJM"
    assert collection["biosamples"] == []
    # Addressable and labelled, not merely present: a collection a client
    # cannot resolve or filter is not what those 24 datasets needed.
    assert collection["persistent_id"] == (
        "https://www.encodeproject.org/annotations/ENCSR026KJM/"
    )
    assert (
        collection["extra"]["encode"]["annotation_type"]
        == "candidate Cis-Regulatory Elements"
    )


def test_transform_annotation_to_c2m2_should_fold_the_dataset_accession():
    """Test the dataset accession is folded the way filters are.

    Given:
        An annotation row whose dataset accession is lower-cased.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        accession_id should be folded while local_id keeps the raw value.
    """
    # Arrange
    row = _annotation_row(**{"Dataset accession": "encsr026kjm"})

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    collection = doc["collections"][0]
    assert collection["local_id"] == "encsr026kjm"
    assert collection["accession_id"] == normalize_accession("encsr026kjm")


def test_transform_annotation_to_c2m2_should_return_none_without_an_accession():
    """Test a row with no file accession is skipped rather than inserted.

    Given:
        An annotation row whose File accession is blank.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        It should return None.
    """
    # Arrange
    row = _annotation_row(**{"File accession": "   "})

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert doc is None


def test_transform_annotation_to_c2m2_should_keep_bedpe_out_of_the_bed_format():
    """Test a paired-interval annotation file is not labelled plain BED.

    A bedpe labelled BED is routed into the tabix BED pipeline, which
    indexes the first mate and silently drops the second.

    Given:
        An annotation row published as bedpe.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        Its file_format should name bedpe rather than BED.
    """
    # Arrange
    row = _annotation_row(**{"File format": "bedpe"})

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert doc["file_format"]["name"] == "bedpe"


def test_transform_annotation_to_c2m2_should_populate_the_required_c2m2_fields():
    """Test annotation documents are as complete as experiment documents.

    Given:
        An annotation row.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        The fields a client needs to locate and fetch the file should all
        be populated, on the same code paths the experiment ingest uses.
    """
    # Arrange
    row = _annotation_row()

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert doc["dcc"]["dcc_abbreviation"] == "ENCODE"
    assert doc["file_format"] == {"id": "format:3004", "name": "bigBed"}
    # The exact term, so deleting the annotation entries from
    # OUTPUT_TYPE_TO_EDAM fails at the ingest boundary too, not only in the
    # ontology unit test.
    assert doc["data_type"] == {"id": "data:1255", "name": "Sequence features"}
    assert doc["access_url"] == ANNOTATION_DOWNLOAD_URL
    assert doc["md5"] == "5ff392dcde69f8ec512ea381928674d9"
    assert doc["size_in_bytes"] == 13743825


@given(name=st.text(min_size=0, max_size=40))
@settings(max_examples=100)
def test_transform_annotation_to_c2m2_should_not_raise_for_any_download_url(name):
    """Test the annotation path is total over download URLs.

    Given:
        Any download filename, including empty and punctuation-only ones.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        It should return a document rather than raising.
    """
    # Arrange
    row = _annotation_row(**{"File download URL": DOWNLOAD_URL.format(name=name)})

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert doc is not None


def test_transform_to_c2m2_should_link_to_the_experiments_path():
    """Test an experiment dataset's persistent_id resolves.

    The URL path became a caller-supplied argument when the annotation
    ingest landed. ENCODE serves the two dataset kinds under different
    paths, so passing the wrong one would give all ~27,000 experiment
    collections a link that 404s -- and the annotation side of the same
    interpolation is pinned while this one was not.

    Given:
        An experiment row with an accession and a biosample term.
    When:
        transform_to_c2m2 is called.
    Then:
        The collection's persistent_id should point at /experiments/.
    """
    # Arrange
    row = _encode_row(
        **{"Experiment accession": "ENCSR918ZSJ", "Biosample term name": "K562"}
    )

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc["collections"][0]["persistent_id"] == (
        "https://www.encodeproject.org/experiments/ENCSR918ZSJ/"
    )


@pytest.mark.parametrize(
    "term_id, expected",
    [
        ("UBERON:0002048", {"id": "UBERON:0002048", "name": "lung"}),
        ("", None),
    ],
    ids=["with-term-id", "without-term-id"],
)
def test_transform_to_c2m2_should_derive_anatomy_from_the_biosample_term(
    term_id, expected
):
    """Test anatomy tracks the term id while the collection tracks the name.

    The guard producing anatomy was rewritten when the annotation path
    landed, and nothing asserted the field on either path -- deleting the
    anatomy block outright would have failed no test.

    Given:
        An experiment row with a biosample term name, with and without a
        term id.
    When:
        transform_to_c2m2 is called.
    Then:
        Anatomy should appear on both the biosample and the collection
        when the term id is present and on neither when it is not, with
        the collection built either way.
    """
    # Arrange
    row = _encode_row(
        **{
            "Experiment accession": "ENCSR918ZSJ",
            "Biosample term name": "lung",
            "Biosample term id": term_id,
        }
    )

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    collection = doc["collections"][0]
    assert collection["biosamples"][0].get("anatomy") == expected
    assert collection.get("anatomy") == ([expected] if expected else None)


def test_transform_to_c2m2_should_not_stamp_annotation_fields_on_an_experiment():
    """Test the annotation-only post-processing stays on its own path.

    Given:
        An experiment row that happens to carry an Annotation type column.
    When:
        transform_to_c2m2 is called.
    Then:
        No annotation_type should appear on the file or its collection.
    """
    # Arrange
    row = _encode_row(
        **{
            "Experiment accession": "ENCSR918ZSJ",
            "Biosample term name": "K562",
            "Annotation type": "candidate Cis-Regulatory Elements",
        }
    )

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert "annotation_type" not in doc.get("extra", {}).get("encode", {})
    assert "annotation_type" not in doc["collections"][0].get("extra", {}).get(
        "encode", {}
    )


def test_transform_to_c2m2_should_keep_bedpe_out_of_the_bed_format():
    """Test the remap reaches the experiment corpus, not only annotations.

    ENCODE already publishes .bedpe under type=Experiment, and those files
    were the original motivation for not calling a paired-interval format
    BED.

    Given:
        An experiment row published as bedpe.
    When:
        transform_to_c2m2 is called.
    Then:
        Its file_format should name bedpe rather than BED.
    """
    # Arrange
    row = _encode_row(**{"File format": "bedpe"})

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc["file_format"]["name"] == "bedpe"


def test_transform_to_c2m2_should_mirror_the_assembly_under_the_dcc_namespace():
    """Test extra.encode.assembly carries the value the schema promises.

    The field was declared and published in the SDL but never written, so
    a client reaching for it -- the natural move once multi-assembly cCREs
    made assembly a filter people use -- matched nothing at all.

    Given:
        An experiment row naming an assembly.
    When:
        transform_to_c2m2 is called.
    Then:
        extra.encode.assembly should mirror the top-level genome_assembly.
    """
    # Arrange
    row = _encode_row(**{"File assembly": "GRCh38"})

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc["genome_assembly"] == "GRCh38"
    assert doc["extra"]["encode"]["assembly"] == "GRCh38"


def test_transform_annotation_to_c2m2_should_mirror_the_assembly_under_the_dcc_namespace():
    """Test extra.encode.assembly carries the value the schema promises.

    The annotation TSV names the column "Assembly" rather than "File
    assembly", so the two transforms reach the same field by different
    routes and each needs its own pin.

    Given:
        An annotation row naming an assembly.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        extra.encode.assembly should mirror the top-level genome_assembly.
    """
    # Arrange
    row = _annotation_row(**{"Assembly": "GRCh38"})

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert doc["genome_assembly"] == "GRCh38"
    assert doc["extra"]["encode"]["assembly"] == "GRCh38"


def test_transform_annotation_to_c2m2_should_produce_a_valid_file_document():
    """Test the emitted document is one the read path can deserialize.

    The ingest writes plain dicts and the GraphQL resolver reads them back
    through FileMetadataModel on every row, so a key the model does not
    declare is dropped silently -- the filter would exist and match
    nothing. Nothing else joins the two halves.

    Given:
        A full annotation row.
    When:
        The emitted document is validated as a FileMetadataModel.
    Then:
        It should validate and expose all the annotation fields on their
        enriched models.
    """
    # Arrange
    row = _annotation_row(**{"Software used": "ABC-Enhancer-Gene-Prediction"})

    # Act
    model = FileMetadataModel(**transform_annotation_to_c2m2(row))

    # Assert
    assert model.extra.encode.annotation_type == "candidate Cis-Regulatory Elements"
    assert model.extra.encode.organism == "Mus musculus"
    assert model.extra.encode.assembly == "mm10"
    dataset = model.collections[0]
    assert dataset.extra.encode.annotation_type == (
        "candidate Cis-Regulatory Elements"
    )
    assert dataset.extra.encode.software_used == "ABC-Enhancer-Gene-Prediction"
    assert dataset.extra.encode.encyclopedia_version == "ENCODE v1"
    biosample = dataset.biosamples[0]
    assert biosample.extra.encode.life_stage == "postnatal"
    assert biosample.extra.encode.age == "0"
    assert biosample.extra.encode.age_units == "day"


def test_transform_annotation_to_c2m2_should_omit_an_empty_dataset_extra():
    """Test a dataset with nothing to enrich carries no empty extra dict.

    An empty ``{"encode": {}}`` would surface as a null facet in any
    distinct-value enumeration over the dataset fields.

    Given:
        An annotation row whose every dataset-level source is blank.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        The dataset should carry no extra key at all.
    """
    # Arrange
    row = _annotation_row(
        **{
            "Project": "",
            "dbxrefs": "",
            "Annotation type": "",
            "Software used": "",
            "Encyclopedia Version": "",
        }
    )

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert "extra" not in doc["collections"][0]


def test_transform_annotation_to_c2m2_should_skip_a_blank_annotation_type():
    """Test a blank annotation type is absent rather than empty.

    Given:
        An annotation row with no Annotation type but a populated
        Encyclopedia Version.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        Neither the file nor the dataset should carry annotation_type,
        while the encyclopedia version still lands.
    """
    # Arrange
    row = _annotation_row(**{"Annotation type": "  "})

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert "annotation_type" not in doc["extra"]["encode"]
    dataset_extra = doc["collections"][0]["extra"]["encode"]
    assert "annotation_type" not in dataset_extra
    assert dataset_extra["encyclopedia_version"] == "ENCODE v1"


def test_transform_annotation_to_c2m2_should_enrich_a_biosample_keyed_dataset():
    """Test the locally synthesized dataset still carries its annotation fields.

    Reachable on the annotation path because the collection is no longer
    gated on the biosample term: a row with a biosample but no dataset
    accession falls back to a ``biosample:``-keyed collection.

    Given:
        An annotation row with a biosample term but no dataset accession.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        The fallback dataset should carry annotation_type and
        experiment_target, and no fabricated accession or persistent id.
    """
    # Arrange
    row = _annotation_row(**{"Dataset accession": ""})

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    dataset = doc["collections"][0]
    assert dataset["local_id"] == "biosample:lung"
    assert dataset["extra"]["encode"]["annotation_type"] == (
        "candidate Cis-Regulatory Elements"
    )
    assert dataset["experiment_target"] == "H3K4me3-mouse, CTCF-mouse"
    assert "accession_id" not in dataset
    assert "persistent_id" not in dataset


def test_transform_annotation_to_c2m2_should_still_label_a_file_with_no_dataset():
    """Test a file with nothing to group it by is still classified.

    Given:
        An annotation row with neither a dataset accession nor a biosample
        term.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        It should build no collection while the file still carries its
        annotation type.
    """
    # Arrange
    row = _annotation_row(
        **{
            "Dataset accession": "",
            "Biosample term name": "",
            "Biosample term id": "",
        }
    )

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert doc["collections"] == []
    assert doc["extra"]["encode"]["annotation_type"] == (
        "candidate Cis-Regulatory Elements"
    )


def test_transform_annotation_to_c2m2_should_not_assay_type_an_empty_assay():
    """Test the shape every released cCRE row actually has.

    All 12,448 of them publish an empty Assay term name, so this is the
    dominant annotation shape rather than an edge case.

    Given:
        An annotation row whose Assay term name is empty.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        The dataset should be built with no experiment_type and the file
        with no assay_type, rather than either being defaulted.
    """
    # Arrange
    row = _annotation_row()

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert "assay_type" not in doc
    assert "experiment_type" not in doc["collections"][0]


def test_transform_annotation_to_c2m2_should_pass_a_multi_valued_assay_through():
    """Test a comma-joined assay list is preserved rather than dropped.

    Some interaction-prediction rows name several assays in one column.
    No OBI term matches the joined string, which is correct -- but the
    string itself is still the best available description.

    Given:
        An annotation row whose Assay term name lists four assays.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        experiment_type should hold it verbatim while assay_type stays
        absent.
    """
    # Arrange
    row = _annotation_row(
        **{"Assay term name": "ChIP-seq, RNA-seq, HiC, ATAC-seq"}
    )

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert doc["collections"][0]["experiment_type"] == (
        "ChIP-seq, RNA-seq, HiC, ATAC-seq"
    )
    assert "assay_type" not in doc


def test_transform_annotation_to_c2m2_should_drop_donor_traits_with_no_biosample():
    """Test the donor traits go nowhere when there is no biosample to hold them.

    They are biosample-scoped, so a row with no biosample term has no
    honest destination for them short of inventing one. Pinned rather than
    fixed: no released annotation row carries both, so nothing is lost
    today -- but if ENCODE starts publishing that combination this test is
    what makes the loss visible instead of silent.

    Given:
        An annotation row naming a life stage and age but no biosample
        term.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        The dataset should be built with no biosamples and the traits
        should appear nowhere in the document.
    """
    # Arrange
    row = _annotation_row(
        **{
            "Biosample term name": "",
            "Biosample term id": "",
            "Life stage": "embryonic",
            "Age": "10.5",
            "Age units": "week",
        }
    )

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert doc["collections"][0]["biosamples"] == []
    assert "embryonic" not in repr(doc)
    assert "10.5" not in repr(doc)


def test_transform_annotation_to_c2m2_should_keep_biginteract_out_of_bigbed():
    """Test a bigInteract annotation file is not labelled plain bigBed.

    Extracting it as a bigBed and indexing its leading columns is
    range-coherent but degrades the interaction to an interval.

    Given:
        An annotation row published as bigInteract.
    When:
        transform_annotation_to_c2m2 is called.
    Then:
        Its file_format should name bigInteract rather than bigBed.
    """
    # Arrange
    row = _annotation_row(**{"File format": "bigInteract"})

    # Act
    doc = transform_annotation_to_c2m2(row)

    # Assert
    assert doc["file_format"]["name"] == "bigInteract"


def test_transform_to_c2m2_should_store_the_organism_on_the_file():
    """Test the experiment path records organism where annotations do.

    Both TSVs publish the same datum under different names. Populating it
    from both keeps the filter meaning the same thing corpus-wide rather
    than matching annotation files only.

    Given:
        An experiment row naming its Biosample organism.
    When:
        transform_to_c2m2 is called.
    Then:
        The file should carry that organism.
    """
    # Arrange
    row = _encode_row(
        **{"Biosample organism": "Homo sapiens", "Biosample term name": "K562"}
    )

    # Act
    doc = transform_to_c2m2(row)

    # Assert
    assert doc["extra"]["encode"]["organism"] == "Homo sapiens"
