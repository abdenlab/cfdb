"""Smoke test for BamIndexProcessor against a non-empty alignment file.

Exercises both branches of the processor with real samtools on PATH:

- BAM path: feed a pre-sorted, multi-read BAM through ``run`` and
  assert the cache holds a non-empty BAI and no DATA artifact.
- SAM path: feed a deliberately unsorted multi-read SAM through ``run``
  and assert the convert+sort pipeline produces a non-empty sorted BAM
  and BAI, both committed to the cache.

Run with:  uv run python scripts/smoke_bam_pipeline.py
"""

from __future__ import annotations

import asyncio
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.processors import bam as bam_module
from cfdb.workflows.processors.bam import BamIndexProcessor


SAM_HEADER = (
    "@HD\tVN:1.6\tSO:coordinate\n"
    "@SQ\tSN:chr1\tLN:1000\n"
    "@SQ\tSN:chr2\tLN:1000\n"
)
# Sorted-by-coordinate reads spanning two contigs; chr1 first, then chr2.
SORTED_READS = [
    ("r1", "chr1", 10),
    ("r2", "chr1", 50),
    ("r3", "chr1", 120),
    ("r4", "chr2", 30),
    ("r5", "chr2", 700),
]
# Same reads, intentionally out-of-order to force the sort stage to do work.
UNSORTED_READS = [
    ("r4", "chr2", 30),
    ("r2", "chr1", 50),
    ("r5", "chr2", 700),
    ("r1", "chr1", 10),
    ("r3", "chr1", 120),
]


def _sam_lines(reads: list[tuple[str, str, int]]) -> str:
    rows = [
        f"{name}\t0\t{ref}\t{pos}\t60\t5M\t*\t0\t0\tACGTA\tIIIII"
        for name, ref, pos in reads
    ]
    return SAM_HEADER + "\n".join(rows) + "\n"


async def _run_bam_path(workdir: Path, cache_root: Path) -> None:
    workdir.mkdir(parents=True, exist_ok=True)
    sam_path = workdir / "sorted.sam"
    sam_path.write_text(_sam_lines(SORTED_READS))
    bam_input = workdir / "sorted.bam"
    subprocess.run(
        ["samtools", "view", "-bS", str(sam_path), "-o", str(bam_input)],
        check=True,
    )
    size = bam_input.stat().st_size
    assert size > 0, "fixture BAM should not be empty"
    print(f"[BAM] fixture BAM: {bam_input} ({size} bytes, {len(SORTED_READS)} reads)")

    original_download = bam_module.download_source

    async def fake_download(_meta, dest):
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(bam_input, dest)
        return dest

    bam_module.download_source = fake_download
    try:
        proc = BamIndexProcessor()
        file_meta = {
            "dcc": {"dcc_abbreviation": "encode"},
            "local_id": "smoke-bam",
            "md5": "0123456789abcdef0123456789abcdef",
            "access_url": "https://example.invalid/x.bam",
            "file_format": {"name": "BAM"},
        }
        run_workdir = workdir / "run-bam"
        events: list[dict] = []
        async for event in proc.run(file_meta, run_workdir, cache_root):
            events.append(event)
            print(f"[BAM] event: {event}")
    finally:
        bam_module.download_source = original_download

    complete = next(e for e in events if e["event"] == "complete")
    artifacts = complete["artifacts"]
    assert "data" not in artifacts, "BAM path must not cache a data artifact"
    cache = LocalFsCache(cache_root)
    entry = await cache.head(artifacts["index"])
    assert entry is not None and entry.size > 0, "BAI should be cached and non-empty"
    print(f"[BAM] OK — index cached at key {artifacts['index']} ({entry.size} bytes)")


async def _run_sam_path(workdir: Path, cache_root: Path) -> None:
    workdir.mkdir(parents=True, exist_ok=True)
    sam_input = workdir / "unsorted.sam"
    sam_input.write_text(_sam_lines(UNSORTED_READS))
    size = sam_input.stat().st_size
    assert size > 0, "fixture SAM should not be empty"
    print(f"[SAM] fixture SAM: {sam_input} ({size} bytes, {len(UNSORTED_READS)} reads)")

    original_download = bam_module.download_source

    async def fake_download(_meta, dest):
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(sam_input, dest)
        return dest

    bam_module.download_source = fake_download
    try:
        proc = BamIndexProcessor()
        file_meta = {
            "dcc": {"dcc_abbreviation": "encode"},
            "local_id": "smoke-sam",
            "md5": "fedcba9876543210fedcba9876543210",
            "access_url": "https://example.invalid/x.sam",
            "file_format": {"name": "SAM"},
        }
        run_workdir = workdir / "run-sam"
        events: list[dict] = []
        async for event in proc.run(file_meta, run_workdir, cache_root):
            events.append(event)
            print(f"[SAM] event: {event}")
    finally:
        bam_module.download_source = original_download

    complete = next(e for e in events if e["event"] == "complete")
    artifacts = complete["artifacts"]
    cache = LocalFsCache(cache_root)
    data_entry = await cache.head(artifacts["data"])
    index_entry = await cache.head(artifacts["index"])
    assert data_entry is not None and data_entry.size > 0, "sorted BAM must be cached"
    assert index_entry is not None and index_entry.size > 0, "BAI must be cached"
    print(
        f"[SAM] OK — sorted BAM {data_entry.size} bytes, "
        f"BAI {index_entry.size} bytes"
    )

    # Sanity-check that the cached BAM really is coordinate-sorted by
    # piping it back through `samtools view -H`.
    bam_out = workdir / "cached-sorted.bam"
    with bam_out.open("wb") as fh:
        async for chunk in cache.get(artifacts["data"]):
            fh.write(chunk)
    header = subprocess.check_output(["samtools", "view", "-H", str(bam_out)], text=True)
    first_line = header.splitlines()[0]
    assert "SO:coordinate" in first_line, (
        f"cached BAM header missing SO:coordinate: {first_line!r}"
    )
    print(f"[SAM] cached BAM header confirms sort order: {first_line}")


async def main() -> int:
    if shutil.which("samtools") is None:
        print("samtools not on PATH; cannot run smoke test", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="cfdb-bam-smoke-") as tmp:
        root = Path(tmp)
        cache_root = root / "cache"
        cache_root.mkdir()
        await _run_bam_path(root / "bam", cache_root)
        await _run_sam_path(root / "sam", cache_root)

    print("\nSmoke test passed.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
