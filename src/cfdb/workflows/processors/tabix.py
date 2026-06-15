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


class TabixIntervalProcessor(Processor):
    """Handle plain-text genomic interval formats and produce a tabix index."""

    processor_version = 1
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
