"""Smoke test for the preprocessing/indexing pipeline across all formats.

Generates a deterministic, non-empty sample per supported format and
runs it through its processor end-to-end with real samtools / htslib
tools on PATH. Per-format the script:

- Builds a sample via ``tests.integration.fixtures.make_samples``.
- Monkey-patches ``download_source`` so the processor pulls from the
  local fixture rather than touching the network.
- Drives ``Processor.run`` to completion, capturing the event stream.
- Verifies the expected artifact kinds landed in the cache and are
  non-empty.

Passthrough formats (CSV / TSV / bigWig) don't have a processor; for
those the script confirms ``ProcessorRegistry.lookup_for`` resolves to
``PassthroughProcessor`` and that ``needs_processing`` returns False.

Run with: uv run python scripts/smoke_all_formats.py
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import gzip
import hashlib
import json
import random
import shutil
import sys
import tempfile
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.processors import bam as bam_module
from cfdb.workflows.processors import passthrough as passthrough_module
from cfdb.workflows.processors import tabix as tabix_module
from cfdb.workflows.processors.bam import BamIndexProcessor
from cfdb.workflows.processors.passthrough import PassthroughProcessor
from cfdb.workflows.processors.registry import ProcessorRegistry
from cfdb.workflows.processors.tabix import TabixIntervalProcessor

# The fixture builders live under tests/ so the smoke script reuses
# the same deterministic samples the integration suite exercises.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tests.integration.fixtures.make_samples import (  # noqa: E402
    SampleFile,
    make_bam,
    make_bed_gz,
    make_bigbed,
    make_broadpeak_gz,
    make_gff3_gz,
    make_gtf_gz,
    make_narrowpeak_gz,
    make_sam,
    make_vcf_gz,
)


REQUIRED_TOOLS = ("samtools", "bgzip", "tabix", "zcat")
OPTIONAL_TOOLS = ("gffread", "bedToBigBed", "bigBedToBed")

_GFF_V2_CHROMS = ("chr1", "chr2", "chr3", "chrX")
_GFF_V2_CHROM_SIZE = 1_000_000


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def make_gff_v2_gz(root: Path) -> SampleFile:
    """Emit a gzipped GFF v2 (200 features, ``tag "value";`` attribute syntax).

    Distinct from ``make_gff3_gz`` — exercises the ``GFF`` alias path
    through the tabix processor (vs the ``GFF3`` alias). Both map to
    the same ``gff`` preset in production; this fixture verifies the
    alias dispatch with non-empty data.
    """
    rng = random.Random(99)  # local seed; not derived from make_samples.SEED
    path = root / "sample.gff.gz"
    lines = []
    for i in range(200):
        chrom = rng.choice(_GFF_V2_CHROMS)
        start = rng.randint(1, _GFF_V2_CHROM_SIZE - 5000)
        end = start + rng.randint(500, 5000)
        strand = rng.choice(["+", "-"])
        # GFF v2 attribute syntax — semicolon-separated tag-value pairs
        # with quoted values, like GTF and unlike GFF3 (tag=value).
        lines.append(
            f"{chrom}\tcfdb\texon\t{start}\t{end}\t.\t{strand}\t0\t"
            f'gene_id "GENE{i:04d}"; transcript_id "TX{i:04d}";'
        )
    text = "\n".join(lines) + "\n"
    with gzip.open(path, "wt") as fh:
        fh.write(text)
    return SampleFile(path=path, md5=_md5(path), format="GFF")


def _check_required_tools() -> list[str]:
    return [t for t in REQUIRED_TOOLS if shutil.which(t) is None]


@dataclass
class SmokeResult:
    format: str
    status: str  # "pass", "fail", "skip"
    detail: str
    artifacts: dict[str, str] | None = None  # cache_key per kind ("data"/"index")


def _make_file_meta(sample: SampleFile, local_id: str) -> dict:
    return {
        "dcc": {"dcc_abbreviation": "encode"},
        "local_id": local_id,
        "md5": sample.md5,
        "access_url": sample.access_url,
        "file_format": {"name": sample.format},
    }


def _install_fake_download(module, fixture: Path) -> Callable[[], None]:
    original = module.download_source

    async def fake_download(_meta, dest):
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(fixture, dest)
        return dest

    module.download_source = fake_download
    return lambda: setattr(module, "download_source", original)


async def _drive_processor(
    proc,
    module,
    sample: SampleFile,
    workdir: Path,
    cache_root: Path,
    local_id: str,
) -> dict:
    """Run a processor end-to-end against a sample and return its 'complete' event."""
    restore = _install_fake_download(module, sample.path)
    try:
        events: list[dict] = []
        async for event in proc.run(
            _make_file_meta(sample, local_id),
            workdir,
            cache_root,
        ):
            events.append(event)
    finally:
        restore()

    complete = next((e for e in events if e["event"] == "complete"), None)
    if complete is None:
        raise RuntimeError(
            f"Processor for {sample.format} did not yield a complete event; "
            f"got {len(events)} events: {[e.get('event') for e in events]}"
        )
    return complete


async def _verify_artifacts(
    cache_root: Path,
    artifacts: dict[str, str],
    *,
    expect: tuple[str, ...],
) -> dict[str, int]:
    """Assert each expected artifact kind landed in the cache; return sizes."""
    cache = LocalFsCache(cache_root)
    sizes: dict[str, int] = {}
    for kind in expect:
        if kind not in artifacts:
            raise RuntimeError(
                f"Missing {kind!r} artifact in complete event; got keys {list(artifacts)}"
            )
        entry = await cache.head(artifacts[kind])
        if entry is None or entry.size == 0:
            raise RuntimeError(
                f"Artifact {kind!r} at {artifacts[kind]!r} is missing or empty "
                f"(entry={entry})"
            )
        sizes[kind] = entry.size

    extras = set(artifacts) - set(expect)
    if extras:
        raise RuntimeError(
            f"Unexpected extra artifact kinds: {extras} (expected only {expect})"
        )
    return sizes


# ---------------------------------------------------------------------------
# Per-format smoke runners.
# ---------------------------------------------------------------------------


async def _smoke_bam(workdir: Path, cache_root: Path, sample: SampleFile) -> SmokeResult:
    proc = BamIndexProcessor()
    complete = await _drive_processor(
        proc, bam_module, sample, workdir / "BAM", cache_root, "smoke-bam"
    )
    sizes = await _verify_artifacts(
        cache_root, complete["artifacts"], expect=("index",)
    )
    return SmokeResult(
        "BAM",
        "pass",
        f"BAI {sizes['index']}B (no data artifact for pre-sorted BAM)",
        artifacts=dict(complete["artifacts"]),
    )


async def _smoke_sam(workdir: Path, cache_root: Path, sample: SampleFile) -> SmokeResult:
    proc = BamIndexProcessor()
    complete = await _drive_processor(
        proc, bam_module, sample, workdir / "SAM", cache_root, "smoke-sam"
    )
    sizes = await _verify_artifacts(
        cache_root, complete["artifacts"], expect=("data", "index")
    )
    return SmokeResult(
        "SAM",
        "pass",
        f"sorted BAM {sizes['data']}B + BAI {sizes['index']}B",
        artifacts=dict(complete["artifacts"]),
    )


async def _smoke_tabix(
    fmt_label: str,
    workdir: Path,
    cache_root: Path,
    sample: SampleFile,
    *,
    skip_if_tool_missing: str | None = None,
) -> SmokeResult:
    if skip_if_tool_missing and shutil.which(skip_if_tool_missing) is None:
        return SmokeResult(
            fmt_label, "skip", f"{skip_if_tool_missing} not on PATH"
        )
    proc = TabixIntervalProcessor()
    complete = await _drive_processor(
        proc, tabix_module, sample, workdir / fmt_label, cache_root,
        f"smoke-{fmt_label.lower()}",
    )
    sizes = await _verify_artifacts(
        cache_root, complete["artifacts"], expect=("data", "index")
    )
    return SmokeResult(
        fmt_label,
        "pass",
        f"bgz {sizes['data']}B + tbi {sizes['index']}B",
        artifacts=dict(complete["artifacts"]),
    )


def _smoke_passthrough(fmt: str) -> SmokeResult:
    registry = ProcessorRegistry()
    registry.register(PassthroughProcessor())
    file_meta = {
        "dcc": {"dcc_abbreviation": "encode"},
        "local_id": f"smoke-{fmt.lower()}",
        "md5": "00000000000000000000000000000000",
        "access_url": f"https://example.invalid/x.{fmt.lower()}",
        "file_format": {"name": fmt},
    }
    proc = registry.lookup_for(file_meta)
    if not isinstance(proc, PassthroughProcessor):
        return SmokeResult(
            fmt, "fail", f"registry returned {type(proc).__name__}, expected PassthroughProcessor"
        )
    if proc.needs_processing(file_meta):
        return SmokeResult(
            fmt, "fail", "needs_processing returned True for passthrough format"
        )
    return SmokeResult(fmt, "pass", "registry resolves to PassthroughProcessor; no run")


# ---------------------------------------------------------------------------
# Driver.
# ---------------------------------------------------------------------------


async def main(out_dir: Path | None = None) -> int:
    missing = _check_required_tools()
    if missing:
        print(f"Required tools missing from PATH: {missing}", file=sys.stderr)
        return 2

    optional_missing = [t for t in OPTIONAL_TOOLS if shutil.which(t) is None]
    if optional_missing:
        print(f"Optional tools missing (some formats will skip): {optional_missing}")

    results: list[SmokeResult] = []
    manifest: list[dict] = []

    if out_dir is not None:
        out_dir = out_dir.resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        # Wipe stale artifacts from a prior run so the manifest reflects
        # only what this invocation produced.
        for child in out_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
        scope = contextlib.nullcontext(str(out_dir))
        keep_outputs = True
    else:
        scope = tempfile.TemporaryDirectory(prefix="cfdb-smoke-all-")
        keep_outputs = False

    with scope as tmp:
        root = Path(tmp)
        sample_root = root / "samples"
        cache_root = root / "cache"
        workdir_root = root / "work"
        sample_root.mkdir(exist_ok=True)
        cache_root.mkdir(exist_ok=True)
        workdir_root.mkdir(exist_ok=True)

        # Build samples up-front so each runner can fail-fast on its own
        # processor logic rather than on fixture generation.
        print("Generating samples...")
        sam_sample = make_sam(sample_root)
        bam_sample = make_bam(sample_root, sam_sample)
        vcf_sample = make_vcf_gz(sample_root)
        bed_sample = make_bed_gz(sample_root)
        gff_sample = make_gff_v2_gz(sample_root)
        gff3_sample = make_gff3_gz(sample_root)
        gtf_sample = make_gtf_gz(sample_root)
        np_sample = make_narrowpeak_gz(sample_root)
        bp_sample = make_broadpeak_gz(sample_root)
        bb_sample = make_bigbed(sample_root)  # may be None if bedToBigBed missing
        print(f"  BAM   {bam_sample.path.stat().st_size}B")
        print(f"  SAM   {sam_sample.path.stat().st_size}B")
        print(f"  VCF   {vcf_sample.path.stat().st_size}B (gzipped)")
        print(f"  BED   {bed_sample.path.stat().st_size}B (gzipped)")
        print(f"  GFF   {gff_sample.path.stat().st_size}B (gzipped, v2 syntax)")
        print(f"  GFF3  {gff3_sample.path.stat().st_size}B (gzipped)")
        print(f"  GTF   {gtf_sample.path.stat().st_size}B (gzipped)")
        print(f"  NP    {np_sample.path.stat().st_size}B (gzipped)")
        print(f"  BP    {bp_sample.path.stat().st_size}B (gzipped)")
        if bb_sample is not None:
            print(f"  bigBed {bb_sample.path.stat().st_size}B")
        else:
            print("  bigBed: bedToBigBed missing — will skip")

        # Per-format smoke runners.
        runners: list[tuple[str, Callable[[], Awaitable[SmokeResult]]]] = [
            ("BAM", lambda: _smoke_bam(workdir_root, cache_root, bam_sample)),
            ("SAM", lambda: _smoke_sam(workdir_root, cache_root, sam_sample)),
            ("VCF", lambda: _smoke_tabix("VCF", workdir_root, cache_root, vcf_sample)),
            ("BED", lambda: _smoke_tabix("BED", workdir_root, cache_root, bed_sample)),
            (
                "GFF",
                lambda: _smoke_tabix(
                    "GFF", workdir_root, cache_root, gff_sample
                ),
            ),
            (
                "GFF3",
                lambda: _smoke_tabix(
                    "GFF3", workdir_root, cache_root, gff3_sample
                ),
            ),
            (
                "GTF",
                lambda: _smoke_tabix(
                    "GTF",
                    workdir_root,
                    cache_root,
                    gtf_sample,
                    skip_if_tool_missing="gffread",
                ),
            ),
            (
                "NarrowPeak",
                lambda: _smoke_tabix(
                    "NarrowPeak", workdir_root, cache_root, np_sample
                ),
            ),
            (
                "BroadPeak",
                lambda: _smoke_tabix(
                    "BroadPeak", workdir_root, cache_root, bp_sample
                ),
            ),
        ]
        if bb_sample is not None:
            runners.append(
                (
                    "bigBed",
                    lambda: _smoke_tabix(
                        "bigBed",
                        workdir_root,
                        cache_root,
                        bb_sample,
                        skip_if_tool_missing="bigBedToBed",
                    ),
                )
            )
        else:
            results.append(
                SmokeResult("bigBed", "skip", "bedToBigBed not on PATH — no fixture")
            )

        for fmt, runner in runners:
            print(f"\n[{fmt}] running...")
            try:
                results.append(await runner())
            except Exception as exc:  # noqa: BLE001
                traceback.print_exc()
                results.append(SmokeResult(fmt, "fail", f"{type(exc).__name__}: {exc}"))

        # Passthrough sanity checks — no processor run, just registry contract.
        for fmt in ("CSV", "TSV", "bigWig"):
            print(f"\n[{fmt}] passthrough...")
            results.append(_smoke_passthrough(fmt))

        # Manifest for downstream consumers (the static-file server and
        # Gosling spec rendering). Only written when --out is given,
        # because the tempdir path is ephemeral.
        if keep_outputs:
            for r in results:
                if r.status == "pass" and r.artifacts:
                    manifest.append(
                        {
                            "format": r.format,
                            "artifacts": r.artifacts,
                        }
                    )
            manifest_path = Path(tmp) / "manifest.json"
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
            print(f"\nManifest: {manifest_path}")
            print(f"Cache:    {cache_root}")

    # Summary.
    print("\n" + "=" * 60)
    print("Smoke test summary:")
    print("=" * 60)
    width = max(len(r.format) for r in results)
    for r in results:
        marker = {"pass": "OK", "fail": "FAIL", "skip": "SKIP"}[r.status]
        print(f"  [{marker:4}] {r.format:<{width}}  {r.detail}")

    failed = [r for r in results if r.status == "fail"]
    if failed:
        print(f"\n{len(failed)} format(s) failed.")
        return 1
    print("\nAll formats passed (or were skipped for missing optional tools).")
    return 0


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help=(
            "Persist samples/, cache/, work/, and manifest.json under "
            "PATH instead of a tempdir. Required for the Gosling "
            "static-server workflow."
        ),
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(asyncio.run(main(out_dir=args.out)))
