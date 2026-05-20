"""Deterministic sample-file generators for integration tests.

Each ``make_*`` builder emits a realistic, intentionally-unsorted sample
file in a test-supplied directory. Builders are deterministic (seeded
with a fixed value) so two independent runs produce byte-identical data
— which matters because content-addressed cache keys are derived from
the md5 of these files.

All builders shell out to the tools the preprocessing pipeline itself
requires (``samtools``, ``gzip``, ``bgzip``, ``bedToBigBed``). Callers
check for tool presence and skip gracefully when they're absent.
"""

from __future__ import annotations

import gzip
import hashlib
import random
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


SEED = 42
CHROMS = ("chr1", "chr2", "chr3", "chrX")
CHROM_SIZE = 1_000_000
READ_LEN = 100


@dataclass(frozen=True)
class SampleFile:
    """Handle to a generated sample file plus its md5."""

    path: Path
    md5: str
    format: str

    @property
    def access_url(self) -> str:
        """Return a placeholder HTTPS URL for file_meta construction.

        Integration tests stub ``download_source`` to copy the fixture
        file into the workdir; the URL is never actually fetched.
        """
        return f"https://example.invalid/{self.path.name}"


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def make_sam(root: Path) -> SampleFile:
    """Emit a 1000-read SAM with an unsorted coordinate layout."""
    rng = random.Random(SEED + 1)
    path = root / "sample.sam"
    lines = [
        "@HD\tVN:1.6\tSO:unsorted",
    ]
    for chrom in CHROMS:
        lines.append(f"@SQ\tSN:{chrom}\tLN:{CHROM_SIZE}")
    seq = "ACGT" * (READ_LEN // 4)
    qual = "I" * READ_LEN
    for i in range(1000):
        chrom = rng.choice(CHROMS)
        pos = rng.randint(1, CHROM_SIZE - READ_LEN)
        flag = 0
        mapq = 60
        cigar = f"{READ_LEN}M"
        lines.append(
            f"read{i:05d}\t{flag}\t{chrom}\t{pos}\t{mapq}\t{cigar}\t*\t0\t0\t{seq}\t{qual}"
        )
    text = "\n".join(lines) + "\n"
    path.write_text(text)
    return SampleFile(path=path, md5=_md5(path), format="SAM")


def make_bam(root: Path, sam: SampleFile) -> SampleFile:
    """Convert a SAM fixture into a coordinate-sorted BAM via samtools.

    DCC-published BAMs in cfdb's corpus (ENCODE, 4DN, HuBMAP) are
    coordinate-sorted by their alignment pipelines, and
    ``BamIndexProcessor`` assumes that invariant — it indexes BAMs
    in place without re-sorting and rejects unsorted inputs. Sort the
    fixture during generation so the integration suite exercises the
    realistic input shape.
    """
    if shutil.which("samtools") is None:
        raise RuntimeError("samtools required for BAM fixture generation")
    path = root / "sample.bam"
    unsorted = root / "sample.unsorted.bam"
    subprocess.run(
        ["samtools", "view", "-bS", str(sam.path), "-o", str(unsorted)],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["samtools", "sort", "-o", str(path), str(unsorted)],
        check=True,
        capture_output=True,
    )
    unsorted.unlink()
    return SampleFile(path=path, md5=_md5(path), format="BAM")


def make_vcf_gz(root: Path) -> SampleFile:
    """Emit a gzipped VCF with a full ## header block and unsorted records."""
    rng = random.Random(SEED + 2)
    path = root / "sample.vcf.gz"
    lines = [
        "##fileformat=VCFv4.2",
        "##source=cfdb-integration-fixtures",
    ]
    for chrom in CHROMS:
        lines.append(f"##contig=<ID={chrom},length={CHROM_SIZE}>")
    lines += [
        '##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">',
        '##FILTER=<ID=PASS,Description="Passed">',
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
    ]
    bases = "ACGT"
    for i in range(500):
        chrom = rng.choice(CHROMS)
        pos = rng.randint(1, CHROM_SIZE)
        ref = rng.choice(bases)
        alt = rng.choice([b for b in bases if b != ref])
        qual = rng.randint(10, 100)
        depth = rng.randint(5, 200)
        lines.append(
            f"{chrom}\t{pos}\trs{i:05d}\t{ref}\t{alt}\t{qual}\tPASS\tDP={depth}"
        )
    text = "\n".join(lines) + "\n"
    with gzip.open(path, "wt") as fh:
        fh.write(text)
    return SampleFile(path=path, md5=_md5(path), format="VCF")


def make_bed_gz(root: Path) -> SampleFile:
    """Emit a gzipped BED with 2000 unsorted intervals."""
    rng = random.Random(SEED + 3)
    path = root / "sample.bed.gz"
    intervals = []
    for i in range(2000):
        chrom = rng.choice(CHROMS)
        start = rng.randint(0, CHROM_SIZE - 1000)
        end = start + rng.randint(50, 1000)
        intervals.append(f"{chrom}\t{start}\t{end}\tfeat{i:04d}\t{rng.randint(0, 1000)}")
    text = "\n".join(intervals) + "\n"
    with gzip.open(path, "wt") as fh:
        fh.write(text)
    return SampleFile(path=path, md5=_md5(path), format="BED")


def make_gff3_gz(root: Path) -> SampleFile:
    """Emit a gzipped GFF3 with 200 features."""
    rng = random.Random(SEED + 4)
    path = root / "sample.gff3.gz"
    lines = ["##gff-version 3"]
    for i in range(200):
        chrom = rng.choice(CHROMS)
        start = rng.randint(1, CHROM_SIZE - 5000)
        end = start + rng.randint(500, 5000)
        strand = rng.choice(["+", "-"])
        lines.append(
            f"{chrom}\tcfdb\texon\t{start}\t{end}\t.\t{strand}\t0\t"
            f"ID=exon{i:04d};gene_name=GENE{i:04d}"
        )
    text = "\n".join(lines) + "\n"
    with gzip.open(path, "wt") as fh:
        fh.write(text)
    return SampleFile(path=path, md5=_md5(path), format="GFF3")


def make_gtf_gz(root: Path) -> SampleFile:
    """Emit a gzipped GTF with 200 features (GTF attribute syntax)."""
    rng = random.Random(SEED + 5)
    path = root / "sample.gtf.gz"
    lines = []
    for i in range(200):
        chrom = rng.choice(CHROMS)
        start = rng.randint(1, CHROM_SIZE - 5000)
        end = start + rng.randint(500, 5000)
        strand = rng.choice(["+", "-"])
        lines.append(
            f"{chrom}\tcfdb\texon\t{start}\t{end}\t.\t{strand}\t0\t"
            f'gene_id "GENE{i:04d}"; transcript_id "TX{i:04d}";'
        )
    text = "\n".join(lines) + "\n"
    with gzip.open(path, "wt") as fh:
        fh.write(text)
    return SampleFile(path=path, md5=_md5(path), format="GTF")


def make_narrowpeak_gz(root: Path) -> SampleFile:
    """Emit a gzipped narrowPeak (BED6+4) with 500 peaks."""
    rng = random.Random(SEED + 6)
    path = root / "sample.narrowPeak.gz"
    lines = []
    for i in range(500):
        chrom = rng.choice(CHROMS)
        start = rng.randint(0, CHROM_SIZE - 1000)
        end = start + rng.randint(100, 1000)
        score = rng.randint(0, 1000)
        strand = "."
        signal = round(rng.uniform(1.0, 50.0), 2)
        pvalue = round(rng.uniform(0.1, 20.0), 2)
        qvalue = round(rng.uniform(0.1, 20.0), 2)
        peak = rng.randint(0, end - start - 1)
        lines.append(
            f"{chrom}\t{start}\t{end}\tpeak{i:04d}\t{score}\t{strand}\t"
            f"{signal}\t{pvalue}\t{qvalue}\t{peak}"
        )
    text = "\n".join(lines) + "\n"
    with gzip.open(path, "wt") as fh:
        fh.write(text)
    return SampleFile(path=path, md5=_md5(path), format="NarrowPeak")


def make_broadpeak_gz(root: Path) -> SampleFile:
    """Emit a gzipped broadPeak (BED6+3) with 500 peaks."""
    rng = random.Random(SEED + 7)
    path = root / "sample.broadPeak.gz"
    lines = []
    for i in range(500):
        chrom = rng.choice(CHROMS)
        start = rng.randint(0, CHROM_SIZE - 2000)
        end = start + rng.randint(500, 2000)
        score = rng.randint(0, 1000)
        strand = "."
        signal = round(rng.uniform(1.0, 50.0), 2)
        pvalue = round(rng.uniform(0.1, 20.0), 2)
        qvalue = round(rng.uniform(0.1, 20.0), 2)
        lines.append(
            f"{chrom}\t{start}\t{end}\tpeak{i:04d}\t{score}\t{strand}\t"
            f"{signal}\t{pvalue}\t{qvalue}"
        )
    text = "\n".join(lines) + "\n"
    with gzip.open(path, "wt") as fh:
        fh.write(text)
    return SampleFile(path=path, md5=_md5(path), format="BroadPeak")


def make_empty_vcf(root: Path) -> SampleFile:
    """Emit a deterministic header-only VCF (no data records).

    The tabix pipeline must refuse to commit an empty artifact, so the
    integration suite needs a fixture that exercises the
    ``RuntimeError("Refusing to commit empty …")`` path through real
    htslib tools. Pinned content (no RNG draws) so the md5 is stable
    across runs and the content-addressed cache key never drifts.
    """
    path = root / "sample.header-only.vcf"
    lines = [
        "##fileformat=VCFv4.2",
        "##source=cfdb-integration-fixtures",
        "##contig=<ID=chr1,length=1000000>",
        '##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">',
        '##FILTER=<ID=PASS,Description="Passed">',
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
    ]
    text = "\n".join(lines) + "\n"
    path.write_text(text)
    return SampleFile(path=path, md5=_md5(path), format="VCF")


def make_bigbed(root: Path) -> SampleFile | None:
    """Emit a bigBed via bedToBigBed, or return None if the tool is missing.

    Builds a small (~200 feature) sorted BED and a chrom.sizes file
    alongside, then converts. The resulting .bb is tiny but valid.
    """
    if shutil.which("bedToBigBed") is None:
        return None

    rng = random.Random(SEED + 8)
    bed_path = root / "bigbed_input.bed"
    sizes_path = root / "chrom.sizes"
    bb_path = root / "sample.bb"

    sizes_path.write_text("\n".join(f"{c}\t{CHROM_SIZE}" for c in CHROMS) + "\n")

    rows = []
    for i in range(200):
        chrom = rng.choice(CHROMS)
        start = rng.randint(0, CHROM_SIZE - 1000)
        end = start + rng.randint(50, 1000)
        rows.append((chrom, start, end, f"feat{i:04d}"))
    rows.sort(key=lambda r: (r[0], r[1]))
    with bed_path.open("w") as fh:
        for chrom, start, end, name in rows:
            fh.write(f"{chrom}\t{start}\t{end}\t{name}\n")

    subprocess.run(
        ["bedToBigBed", str(bed_path), str(sizes_path), str(bb_path)],
        check=True,
        capture_output=True,
    )
    return SampleFile(path=bb_path, md5=_md5(bb_path), format="bigBed")


def generate_all(root: Path) -> dict[str, SampleFile | None]:
    """Build every sample fixture under ``root``.

    Returns a mapping keyed by format name. The ``bigBed`` entry is
    ``None`` when ``bedToBigBed`` is not on PATH; callers use that to
    skip bigBed-specific tests.
    """
    root.mkdir(parents=True, exist_ok=True)

    sam = make_sam(root)
    samples: dict[str, SampleFile | None] = {
        "SAM": sam,
        "BAM": make_bam(root, sam),
        "VCF": make_vcf_gz(root),
        "VCF_EMPTY": make_empty_vcf(root),
        "BED": make_bed_gz(root),
        "GFF3": make_gff3_gz(root),
        "GTF": make_gtf_gz(root),
        "NarrowPeak": make_narrowpeak_gz(root),
        "BroadPeak": make_broadpeak_gz(root),
        "bigBed": make_bigbed(root),
    }
    return samples
