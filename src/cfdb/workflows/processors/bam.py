"""BAM / SAM processor — index pre-sorted BAMs, convert + sort + index SAMs.

DCC outputs in this corpus (ENCODE, 4DN, HuBMAP) all publish BAMs that
have already been coordinate-sorted by the upstream alignment pipeline.
Re-sorting them on cache miss wastes CPU, bandwidth (download a copy of
the upstream-served bytes), and storage (cache a duplicate of the same
bytes). For BAMs we therefore:

- ``download`` the source BAM,
- verify the header carries ``@HD ... SO:coordinate``, and
- run only ``samtools index`` to produce the BAI sidecar.

Only the index is committed to cache; the data artifact is *not*
cached. ``/data`` falls through to direct streaming from the upstream
``access_url`` via the router's existing pass-through path (the same
path used by CSV/TSV/bigWig). If a future DCC ships unsorted BAMs they
fail loudly here so operators can route them through a sort-aware
processor.

SAMs always need conversion + sort + index — there is no pre-converted
upstream artifact to fall back on, so both data and index are cached
under content-addressed keys, as before. The SAM path uses
``samtools view -bS | samtools sort`` as a single streaming pipeline so
no intermediate BAM is materialized.

Partial-commit recovery: stage-2 index failures leave the sorted-BAM
stage-1 artifact in cache (SAM path only — BAMs have no stage-1
artifact to recover). On retry the SAM processor reuses the cached
sorted BAM rather than re-running the conversion+sort.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from cfdb.workflows import SAMTOOLS_MEMORY_CAP, SAMTOOLS_THREADS
from cfdb.workflows.cache import CacheBackend
from cfdb.workflows.events import Complete, StageComplete, WorkflowEvent
from cfdb.workflows.fetcher import download_source, peek_decompressed_prefix
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.base import Processor
from cfdb.workflows.processors.tools import (
    copy_from_cache,
    format_name,
    run_argv,
    run_shell,
    shell_quote,
)

__all__ = [
    "BamIndexProcessor",
    "download_source",
    "read_bam_header",
    "validate_sam_header",
]

#: SAM header records that always carry >= 2 TAB-separated fields. A
#: ``@CO`` comment is free text and may legitimately hold no tab, so it
#: is excluded from the delimiter check.
_SAM_MULTIFIELD_RECORDS = ("@HD", "@SQ", "@RG", "@PG")


def validate_sam_header(header_text: str) -> None:
    """Reject space-delimited (legacy/corrupt) SAM headers up front.

    samtools requires TAB-delimited header records. Legacy modENCODE-era
    SAMs whose header tabs were expanded to spaces parse-fail deep inside
    ``samtools view`` with an opaque ``[main_samview] fail to read the
    header``. Detect the space-delimited shape here so the workflow fails
    fast with an actionable message instead of after downloading and
    half-running the convert+sort. ``@HD``/``@SQ``/``@RG``/``@PG`` records
    always carry at least two fields, so one without a TAB is the tell.
    No-op for well-formed headers and for headerless input (samtools
    surfaces that case on its own).
    """
    for line in header_text.splitlines():
        if line[:3] in _SAM_MULTIFIELD_RECORDS and "\t" not in line:
            raise RuntimeError(
                "SAM header is space-delimited, not TAB-delimited "
                f"({line[:60]!r}). samtools cannot read it — this is a "
                "legacy modENCODE-era file whose header tabs were expanded "
                "to spaces. Re-publish with a TAB-delimited header (keeping "
                "spaces inside tag values) or route it through a "
                "header-repair step."
            )


async def read_bam_header(bam_path: Path) -> str:
    """Run ``samtools view -H`` against ``bam_path`` and return stdout.

    Module-level so tests can ``mocker.patch.object(bam_module,
    "read_bam_header", ...)`` to bypass the real subprocess call. The
    common ``run_argv`` helper discards stdout (it only checks the
    return code), so this routine spells out the subprocess call
    directly. ``start_new_session=True`` + killpg-on-cancellation
    matches the discipline in ``tools.run_argv`` / ``tools.run_shell``.
    """
    from cfdb.workflows.processors.tools import _terminate_process_group

    proc = await asyncio.create_subprocess_exec(
        "samtools",
        "view",
        "-H",
        str(bam_path),
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
            f"samtools view -H exited {proc.returncode}: "
            f"{stderr.decode(errors='replace')}"
        )
    return stdout.decode(errors="replace")


class BamIndexProcessor(Processor):
    """Index pre-sorted BAMs; convert + sort + index SAMs.

    Per-format artifact set:

    - **BAM**: only ``ArtifactKind.INDEX``. The data path falls through
      to upstream streaming.
    - **SAM**: both ``ArtifactKind.DATA`` (sorted BAM) and
      ``ArtifactKind.INDEX``.
    """

    processor_version = 2
    supported_formats = frozenset({"BAM", "SAM"})
    #: Class-level default. Real per-file advertisement comes from
    #: :meth:`artifact_kinds_produced`, which inspects ``file_meta``.
    artifact_kinds = (ArtifactKind.DATA, ArtifactKind.INDEX)

    def artifact_kinds_produced(
        self, file_meta: dict[str, Any] | None = None
    ) -> tuple[ArtifactKind, ...]:
        """Return ``(INDEX,)`` for BAM, ``(DATA, INDEX)`` for SAM.

        BAMs are assumed to be coordinate-sorted by the upstream DCC
        and so produce only the BAI; ``/data`` streams the upstream
        bytes directly. SAMs always require the convert+sort pipeline,
        so the resulting sorted BAM is cached as the data artifact.
        """
        fmt = format_name(file_meta) if file_meta is not None else None
        if fmt == "SAM":
            return (ArtifactKind.DATA, ArtifactKind.INDEX)
        return (ArtifactKind.INDEX,)

    async def run(
        self,
        file_meta: dict[str, Any],
        workdir: Path,
        cache: CacheBackend,
    ) -> AsyncIterator[WorkflowEvent]:
        workdir.mkdir(parents=True, exist_ok=True)

        fmt = format_name(file_meta) or ""
        if fmt == "SAM":
            async for event in self._run_sam(file_meta, workdir, cache):
                yield event
            return
        async for event in self._run_bam(file_meta, workdir, cache):
            yield event

    async def _run_bam(
        self,
        file_meta: dict[str, Any],
        workdir: Path,
        cache: CacheBackend,
    ) -> AsyncIterator[WorkflowEvent]:
        """Index a pre-sorted BAM, yielding stage_complete + complete events.

        Validates the source's ``@HD ... SO:coordinate`` line before
        indexing and raises ``RuntimeError`` otherwise — surfacing a
        clear failure when an unexpected unsorted BAM arrives, rather
        than silently producing a broken index.
        """
        index_key = self.cache_key_for(file_meta, ArtifactKind.INDEX)

        if await cache.head(index_key) is None:
            source_path = workdir / "source.bam"
            await download_source(file_meta, source_path)
            await self._verify_sorted(source_path)
            bai_path = await self._stage_index(source_path)
            await cache.put(index_key, bai_path)
        yield StageComplete(kind=ArtifactKind.INDEX, key=index_key)
        yield Complete(artifacts={ArtifactKind.INDEX.value: index_key})

    async def _run_sam(
        self,
        file_meta: dict[str, Any],
        workdir: Path,
        cache: CacheBackend,
    ) -> AsyncIterator[WorkflowEvent]:
        """Convert + sort + index a SAM, yielding per-stage events.

        Caches both the sorted BAM and its BAI. Stage-2 (index) failure
        leaves the stage-1 sorted BAM in cache; on retry stage-1 is
        skipped and only the index is re-run.
        """
        data_key = self.cache_key_for(file_meta, ArtifactKind.DATA)
        index_key = self.cache_key_for(file_meta, ArtifactKind.INDEX)

        # Stage 1 — convert + sort if not already cached.
        if await cache.head(data_key) is None:
            sorted_bam = await self._stage_convert_and_sort(file_meta, workdir)
            await cache.put(data_key, sorted_bam)
        yield StageComplete(kind=ArtifactKind.DATA, key=data_key)

        # Stage 2 — produce the BAI from the sorted BAM.
        if await cache.head(index_key) is None:
            sorted_bam_local = workdir / "sorted.bam"
            if not sorted_bam_local.exists():
                await copy_from_cache(cache, data_key, sorted_bam_local)
            bai_path = await self._stage_index(sorted_bam_local)
            await cache.put(index_key, bai_path)
        yield StageComplete(kind=ArtifactKind.INDEX, key=index_key)

        yield Complete(
            artifacts={
                ArtifactKind.DATA.value: data_key,
                ArtifactKind.INDEX.value: index_key,
            }
        )

    async def _stage_convert_and_sort(
        self, file_meta: dict[str, Any], workdir: Path
    ) -> Path:
        """Convert SAM to BAM and coordinate-sort, in a single pipeline."""
        # Fail fast on legacy space-delimited SAM headers: peek only the
        # leading (header) bytes and reject before downloading and
        # convert+sorting the whole file, turning samtools' opaque
        # "fail to read the header" into an actionable error.
        prefix = await peek_decompressed_prefix(file_meta)
        validate_sam_header(prefix.decode("utf-8", errors="replace"))

        source_path = workdir / "source.sam"
        await download_source(file_meta, source_path)
        sorted_path = workdir / "sorted.bam"
        tmp_prefix = workdir / "samtools_tmp"
        cmd = (
            f"samtools view -bS {shell_quote(source_path)} "
            f"| samtools sort -@ {SAMTOOLS_THREADS} "
            f"-m {shell_quote(SAMTOOLS_MEMORY_CAP)} "
            f"-T {shell_quote(tmp_prefix)} -o {shell_quote(sorted_path)} -"
        )
        await run_shell(cmd)
        return sorted_path

    async def _verify_sorted(self, bam_path: Path) -> None:
        """Raise ``RuntimeError`` if ``bam_path`` is not coordinate-sorted.

        Fast path: if the BAM has an ``@HD`` header line carrying
        ``SO:coordinate`` we trust it and return immediately. This is
        the common case for DCC-published BAMs.

        Recovery path: some DCC BAMs omit the ``@HD`` header entirely
        but are still coordinate-sorted in practice. Rather than
        rejecting them outright, scan the first ~1000 mapped alignments
        and verify positions are monotonically non-decreasing within
        each reference. Only escalate to a hard failure if the scan
        disagrees.
        """
        header = await read_bam_header(bam_path)
        first_line = header.split("\n", 1)[0] if header else ""
        if first_line.startswith("@HD") and "SO:coordinate" in first_line:
            return

        # No usable @HD or wrong SO value — fall back to a scan of the
        # first records. Skip unmapped reads (-F 4) because their RNAME
        # / POS columns can sort anywhere and confuse the monotonic
        # check on otherwise-valid files.
        if await self._records_appear_sorted(bam_path):
            return

        raise RuntimeError(
            f"BAM at {bam_path} is not coordinate-sorted "
            f"(@HD line: {first_line!r}); cfdb's BamIndexProcessor "
            "expects DCC-published BAMs to be pre-sorted. Route this "
            "file through a sort-aware processor or fix the upstream "
            "publication."
        )

    async def _records_appear_sorted(
        self, bam_path: Path, sample: int = 1000
    ) -> bool:
        """Return True if the first N mapped reads are coordinate-sorted.

        Streams ``samtools view -F 4 file | head -n N`` and checks that
        within each reference the POS column never decreases. Fast
        enough for a verification step (10–100ms on local disk).
        """
        from cfdb.workflows.processors.tools import _terminate_process_group

        # ``set -o pipefail`` makes the pipeline's exit status reflect
        # the first failing stage, not just ``head``. Without it, a
        # corrupt or truncated BAM that crashes ``samtools view``
        # mid-stream still lets ``head`` exit 0, and this function
        # would silently return True for unverifiable input.
        proc = await asyncio.create_subprocess_shell(
            (
                "set -o pipefail; "
                f"samtools view -F 4 {shell_quote(bam_path)} | head -n {sample}"
            ),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            executable="/bin/bash",
            start_new_session=True,
        )
        try:
            stdout, _stderr = await proc.communicate()
        except BaseException:
            await _terminate_process_group(proc)
            raise
        # samtools may exit non-zero when ``head`` closes the pipe early
        # (SIGPIPE 141). Treat that as success since we got the bytes we
        # wanted; any other non-zero return is a hard failure.
        if proc.returncode not in (0, 141):
            return False
        current_ref: str | None = None
        last_pos = -1
        for line in stdout.decode(errors="replace").splitlines():
            cols = line.split("\t")
            if len(cols) < 4:
                continue
            ref = cols[2]
            try:
                pos = int(cols[3])
            except ValueError:
                return False
            if ref != current_ref:
                current_ref = ref
                last_pos = pos
                continue
            if pos < last_pos:
                return False
            last_pos = pos
        return True

    async def _stage_index(self, sorted_bam: Path) -> Path:
        """Produce a ``.bai`` alongside ``sorted_bam`` and return its path."""
        await run_argv(
            ["samtools", "index", "-@", str(SAMTOOLS_THREADS), str(sorted_bam)]
        )
        bai_path = sorted_bam.parent / (sorted_bam.name + ".bai")
        if not bai_path.exists():
            raise RuntimeError(f"samtools index did not produce {bai_path}")
        return bai_path
