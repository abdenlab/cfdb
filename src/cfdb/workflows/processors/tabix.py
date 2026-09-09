"""Text-interval processor — streaming decompress + sort + bgzip + tabix.

Handles: VCF, GFF, GFF3, GTF, BED, BroadPeak, NarrowPeak, bigBed.

Pipeline by format, expressed as a single streaming shell pipeline so no
intermediate data is buffered in memory or materialized on disk beyond
what the tools themselves require:

- **BED / BroadPeak / NarrowPeak / GFF / GFF3** — ``zcat -f`` handles gz
  or plain input, piped into locale-safe memory-capped sort and bgzip.
- **VCF** — two-pass over a decompressed intermediate (separate the ``##``
  header block from the data block, then sort only the body), re-joined
  via a brace group and piped into bgzip. Two passes are required so
  tabix receives its headers at the top of the file.
- **GTF** — decompress to an intermediate, then ``gffread -E`` converts
  to GFF3, piped through sort + bgzip.
- **bigBed** — binary format; ``bigBedToBed`` emits BED on stdout,
  piped through sort + bgzip.

All sort invocations are memory-capped via ``-S`` (see
``CFDB_SORT_MEMORY_CAP`` in :mod:`cfdb.workflows`) and spill to the
per-job workdir via ``-T`` when they exceed the cap. ``LC_ALL=C`` makes
collation byte-wise and independent of the worker's locale.

Outputs:

- ``data``: the bgzipped sorted text file.
- ``index``: the ``.tbi`` tabix index.

Partial-commit recovery: each stage checks the cache before running.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from cfdb.workflows import SORT_MEMORY_CAP, SORT_PARALLEL
from cfdb.workflows.cache import CacheBackend
from cfdb.workflows.events import Complete, StageComplete, WorkflowEvent
from cfdb.workflows.fetcher import download_source
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.base import Processor
from cfdb.workflows.processors.tools import (
    _terminate_process_group,
    copy_from_cache,
    format_name,
    run_argv,
    run_shell,
    shell_quote,
)

__all__ = ["TabixIntervalProcessor", "download_source"]


#: gzip / bgzip stream magic (RFC 1952). bgzip output is gzip-compatible
#: and shares this prefix, so it is accepted too.
_GZIP_MAGIC = b"\x1f\x8b"

#: Leading magic bytes of non-gzip binary serializations that ``zcat -f``
#: cannot decompress. Matched explicitly so a mislabeled compressed file
#: (e.g. a BEDOPS ``starch`` archive mapped to ``BED`` upstream — issue
#: #69) is rejected by its magic rather than incidentally by the UTF-8
#: text test below.
_BINARY_MAGICS: tuple[bytes, ...] = (
    b"\xca\x5c\xad\xe5",  # BEDOPS starch
    b"BZh",  # bzip2
    b"\xfd7zXZ\x00",  # xz
    b"\x28\xb5\x2f\xfd",  # zstd
    b"PK\x03\x04",  # zip
)

#: How many leading source bytes to inspect when sniffing the encoding.
#: A handful would suffice for the magic checks; 512 is generous headroom
#: that also lets the UTF-8 text test see a representative plaintext span.
_SOURCE_SNIFF_BYTES = 512


def _source_looks_processable(prefix: bytes) -> bool:
    """Return True if ``prefix`` is gzip-compressed or decodes as text.

    The tabix text-interval pipeline assumes ``zcat -f`` yields plain
    text, so a source must be either a gzip/bgzip member (magic
    ``1f 8b``) or already plaintext. A gzip member is trusted on its
    magic alone — ``zcat -f`` is single-pass, so a *doubly*-compressed
    source (e.g. ``gzip(starch)``) is out of scope and would still slip
    through. Everything else is treated as a binary serialization the
    pipeline would silently mangle — most notably a BEDOPS ``starch``
    archive (magic ``ca5cade5``), which the upstream ontology mapper
    labels ``BED`` even though ``zcat -f`` cannot decompress it (see
    issue #69). Known non-gzip compression magics are rejected
    explicitly via ``_BINARY_MAGICS``; any remaining binary is caught by
    the NUL / non-UTF-8 text test.

    An empty prefix returns True: the genuinely-empty-source case is
    caught later by the zero-data-line guard in ``_stage_prepare``.

    The text test is "decodes as UTF-8 with no NUL byte". It is
    deliberately conservative: legitimately-text latin-1/CP1252 BED would
    be rejected (failing the job rather than risking a mangled artifact),
    which is acceptable because the upstream interval corpus is ASCII. A
    multi-byte UTF-8 character can be split at the sniff boundary, so up
    to three trailing bytes (a UTF-8 character is at most 4 bytes) are
    trimmed before concluding the prefix is not text.
    """
    if not prefix:
        return True
    if prefix.startswith(_GZIP_MAGIC):
        return True
    if any(prefix.startswith(magic) for magic in _BINARY_MAGICS):
        return False
    if b"\x00" in prefix:
        return False
    for trim in range(4):
        candidate = prefix if trim == 0 else prefix[:-trim]
        if not candidate:
            break
        try:
            candidate.decode("utf-8")
        except UnicodeDecodeError:
            continue
        return True
    return False


def _read_prefix(path: Path, n: int = _SOURCE_SNIFF_BYTES) -> bytes:
    """Read up to ``n`` leading bytes of ``path``."""
    with path.open("rb") as fh:
        return fh.read(n)


# Format → tabix preset. Both GFF and GFF3 map onto the same preset; the
# upstream ontology mapper emits ``GFF`` for ``.gff`` and ``GFF3`` for
# ``.gff3`` so both must be accepted here to avoid silent bypasses.
_TABIX_PRESET = {
    "VCF": "vcf",
    "GFF": "gff",
    "GFF3": "gff",
    "GTF": "gff",
    "BED": "bed",
    "BroadPeak": "bed",
    "NarrowPeak": "bed",
    "bigBed": "bed",
}

# Sort key arguments per preset, applied to the data block of each format.
_SORT_ARGS = {
    "vcf": ["-k1,1", "-k2,2n"],
    "gff": ["-k1,1", "-k4,4n"],
    "bed": ["-k1,1", "-k2,2n"],
}

#: Formats whose source is binary by design and is validated by the tool
#: that consumes it (``bigBedToBed`` verifies the bigBed magic and exits
#: non-zero on bad input, which ``pipefail`` surfaces before any
#: ``cache.put``). These bypass the gzip/text source sniff — applying it
#: would false-reject every legitimate (binary) bigBed.
_SELF_VALIDATING_FORMATS = frozenset({"bigBed"})


class TabixIntervalProcessor(Processor):
    """Handle plain-text genomic interval formats and produce a tabix index."""

    processor_id = "tabix-interval"

    # v2: the source-encoding guard (issue #69) changed which sources will
    # ever be committed, so re-key all tabix artifacts — a poisoned v1
    # ``data`` entry (committed before the guard existed) becomes a cache
    # miss, re-enters _stage_prepare, and is rejected instead of served.
    processor_version = 2
    supported_formats = frozenset(_TABIX_PRESET.keys())
    artifact_kinds = (ArtifactKind.DATA, ArtifactKind.INDEX)

    async def run(
        self,
        file_meta: dict[str, Any],
        workdir: Path,
        cache: CacheBackend,
    ) -> AsyncIterator[WorkflowEvent]:
        workdir.mkdir(parents=True, exist_ok=True)

        fmt = format_name(file_meta) or ""
        preset = _TABIX_PRESET[fmt]
        sort_args = _SORT_ARGS[preset]

        data_key = self.cache_key_for(file_meta, ArtifactKind.DATA)
        index_key = self.cache_key_for(file_meta, ArtifactKind.INDEX)

        # Stage 1 — produce the bgzipped sorted text artifact.
        if await cache.head(data_key) is None:
            bgz_path = await self._stage_prepare(file_meta, fmt, sort_args, workdir)
            await cache.put(data_key, bgz_path)
        yield StageComplete(kind=ArtifactKind.DATA, key=data_key)

        # Stage 2 — produce the tabix index.
        if await cache.head(index_key) is None:
            bgz_local = workdir / "out.bgz"
            if not bgz_local.exists():
                await copy_from_cache(cache, data_key, bgz_local)
            tbi_path = await self._stage_index(bgz_local, preset)
            await cache.put(index_key, tbi_path)
        yield StageComplete(kind=ArtifactKind.INDEX, key=index_key)

        yield Complete(
            artifacts={
                ArtifactKind.DATA.value: data_key,
                ArtifactKind.INDEX.value: index_key,
            }
        )

    async def _stage_prepare(
        self,
        file_meta: dict[str, Any],
        fmt: str,
        sort_args: list[str],
        workdir: Path,
    ) -> Path:
        """Produce ``{workdir}/out.bgz`` — sorted, bgzipped text."""
        source = workdir / "source.raw"
        await download_source(file_meta, source)

        # Reject a non-gzip/non-text source before any processing or
        # cache commit. Every format here flows through ``zcat -f`` and
        # must be gzip or plain text (see issue #69 — BEDOPS starch
        # archives are mislabeled ``BED`` upstream). Self-validating
        # binary formats (bigBed) are exempt: their tool validates the
        # input and fails the pipeline under ``pipefail`` before any
        # ``cache.put``, so they need no sniff and would be false-rejected
        # by it.
        if fmt not in _SELF_VALIDATING_FORMATS:
            await self._assert_source_processable(source, fmt, file_meta)

        tmp_dir = workdir / "sort_tmp"
        tmp_dir.mkdir(exist_ok=True)
        bgz_path = workdir / "out.bgz"

        sort_cmd = (
            f"LC_ALL=C sort --parallel={SORT_PARALLEL} "
            f"-S {shell_quote(SORT_MEMORY_CAP)} -T {shell_quote(tmp_dir)} "
            f"{' '.join(sort_args)}"
        )

        if fmt == "VCF":
            # VCF requires a two-pass split: the ``##`` header block must
            # stay at the top of the file, then data lines follow in
            # sorted order. We decompress to an intermediate so both
            # passes read from a stable file.
            decompressed = workdir / "decompressed.vcf"
            await run_shell(
                f"zcat -f {shell_quote(source)} > {shell_quote(decompressed)}"
            )
            # We need to tolerate ``grep`` exit-1 (no match — header-only
            # or data-only file) without losing visibility into ``sort``
            # OOMs, disk-full, or signal kills. The naive
            # ``grep ... || true`` would swallow both. Each arm is run
            # through a wrapper that exits 0 only when grep itself
            # returned 0 or 1; any other failure (including downstream
            # ``sort``/``bgzip`` failures in the same pipeline) is
            # propagated, leaving ``pipefail`` free to surface a real
            # error before the truncated bytes ever reach
            # ``cache.put``.
            grep_header = (
                f"sh -c 'grep \"^#\" \"$1\"; rc=$?; "
                f"[ $rc -le 1 ] && exit 0 || exit $rc' _ {shell_quote(decompressed)}"
            )
            grep_body = (
                f"sh -c 'grep -v \"^#\" \"$1\"; rc=$?; "
                f"[ $rc -le 1 ] && exit 0 || exit $rc' _ {shell_quote(decompressed)}"
            )
            cmd = (
                f"{{ {grep_header}; "
                f"{grep_body} | {sort_cmd}; }} "
                f"| bgzip -c > {shell_quote(bgz_path)}"
            )
        elif fmt == "GTF":
            # gffread reads from a file; decompress first so gffread
            # always sees plain text regardless of upstream compression.
            decompressed = workdir / "decompressed.gtf"
            await run_shell(
                f"zcat -f {shell_quote(source)} > {shell_quote(decompressed)}"
            )
            cmd = (
                f"gffread -E -o- {shell_quote(decompressed)} "
                f"| {sort_cmd} "
                f"| bgzip -c > {shell_quote(bgz_path)}"
            )
        elif fmt == "bigBed":
            # bigBed is binary and self-indexed; bigBedToBed writes BED
            # to stdout for downstream piping.
            cmd = (
                f"bigBedToBed {shell_quote(source)} stdout "
                f"| {sort_cmd} "
                f"| bgzip -c > {shell_quote(bgz_path)}"
            )
        else:
            # BED, BroadPeak, NarrowPeak, GFF, GFF3 — single streaming
            # pipeline from source through zcat, sort, and bgzip.
            cmd = (
                f"zcat -f {shell_quote(source)} "
                f"| {sort_cmd} "
                f"| bgzip -c > {shell_quote(bgz_path)}"
            )

        await run_shell(cmd)
        # Reject degenerate inputs (header-only / all-comments / empty)
        # before committing the artifact to the content-addressed cache.
        # If we let the stage-1 put land, stage-2 ``tabix -p`` would fail
        # on a no-records bgz; every retry would skip stage 1 (cache
        # hit) and re-fail stage 2, leaving ``/index`` 5xx-ing forever.
        # Counting non-comment lines via ``zcat | grep`` is fast and
        # works for every supported format. Header-detection rules:
        # VCF/GFF/GTF use ``#`` for headers; BED-family files don't
        # have headers in the upstream-published shape we ingest, so a
        # zero-data-line count means a truly empty source.
        data_lines = await self._count_data_lines(bgz_path, fmt)
        if data_lines == 0:
            raise RuntimeError(
                f"Refusing to commit empty {fmt} artifact for "
                f"workdir={workdir}: zero data lines after preprocessing. "
                "Upstream file is likely header-only / all-comments. "
                "Fix at the source or route through a sort-aware processor."
            )
        return bgz_path

    async def _assert_source_processable(
        self, source: Path, fmt: str, file_meta: dict[str, Any]
    ) -> None:
        """Reject a non-gzip/non-text source before committing anything.

        Reads the leading bytes of the freshly-downloaded ``source`` and
        raises ``RuntimeError`` when they are neither gzip nor text, so a
        binary serialization the ``zcat -f`` pipeline cannot decompress
        (e.g. a BEDOPS ``starch`` archive mislabeled ``BED`` upstream)
        fails the job cleanly *before* any ``cache.put`` rather than
        committing a corrupt artifact. Callers skip this for ``bigBed``,
        whose binary source is handled by ``bigBedToBed``.
        """
        prefix = await asyncio.to_thread(_read_prefix, source)
        if _source_looks_processable(prefix):
            return
        local_id = file_meta.get("local_id")
        dcc = file_meta.get("dcc")
        raise RuntimeError(
            f"Refusing to process {fmt} source for dcc={dcc!r} "
            f"local_id={local_id!r}: leading bytes {prefix[:8].hex()} are "
            "neither gzip nor text. An unsupported serialization (e.g. "
            "BEDOPS starch) was likely mapped onto a text-interval format "
            "upstream; failing before commit so no corrupt artifact is "
            "cached."
        )

    async def _count_data_lines(self, bgz_path: Path, fmt: str) -> int:
        """Return the count of non-header data lines in ``bgz_path``.

        Lines beginning with ``#`` are treated as header/comment for
        every format we currently support; BED-family files have no
        ``#``-prefixed lines in the wild so the count == total non-empty
        lines.

        Raises ``RuntimeError`` when the bgzip decompression itself fails
        (e.g., truncated or corrupt artifact) — surfacing the real cause
        instead of silently returning ``0`` and emitting a misleading
        "empty artifact" error downstream.
        """
        # ``bgzip -d -c`` rather than ``zcat`` because BSD ``zcat`` (the
        # default on macOS dev hosts) only handles ``.Z`` files; htslib's
        # ``bgzip`` is always available alongside ``tabix`` in the same
        # workflow image, so it's the portable decompressor here.
        #
        # Scoping ``|| true`` to a brace group around ``grep`` means
        # ``grep`` exiting 1 (no matching lines — i.e. file is all
        # ``#``-comments) is tolerated, but a non-zero exit from
        # ``bgzip`` still surfaces via ``pipefail``.
        cmd = (
            f"bgzip -d -c {shell_quote(bgz_path)} "
            f"| {{ grep -v -c '^#' || true; }}"
        )
        proc = await asyncio.create_subprocess_exec(
            "bash",
            "-o",
            "pipefail",
            "-c",
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        try:
            stdout, stderr = await proc.communicate()
        except BaseException:
            await _terminate_process_group(proc)
            raise
        if proc.returncode != 0:
            raise RuntimeError(
                f"bgzip pipeline failed for {bgz_path} "
                f"({proc.returncode}): {stderr.decode(errors='replace')}"
            )
        try:
            return int(stdout.decode().strip() or "0")
        except ValueError:
            return 0

    async def _stage_index(self, bgz_local: Path, preset: str) -> Path:
        """Run tabix on a bgzipped artifact, returning the ``.tbi`` path."""
        await run_argv(["tabix", "-p", preset, str(bgz_local)])
        tbi = bgz_local.parent / (bgz_local.name + ".tbi")
        if not tbi.exists():
            raise RuntimeError(f"tabix did not produce {tbi}")
        return tbi
