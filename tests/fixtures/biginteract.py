"""Synthetic bigInteract (bed5+13) builder for the bigInteract tile tests.

Plain function, not a pytest fixture, matching ``coolers.py``'s convention.

``pybigtools``'s own Python wrapper (``pybigtools.open``) unconditionally
returns a read-only ``BBIReader``, regardless of the ``mode`` argument — it
drops write-mode support rather than dispatching to ``BigBedWriter``. The
writer is only reachable through the compiled extension module directly,
``pybigtools.pybigtools.open(path, "w")``; that is what this module does.
``BigBedWriter`` itself cannot be constructed via its class (pybigtools
raises "No constructor defined" — it has no public ``__new__``), so there
is no more direct route.
"""

from __future__ import annotations

from pathlib import Path
from typing import NamedTuple

import pytest

#: Standard UCSC ``bigInteract`` AutoSql schema (bed5+13). Declared
#: explicitly, rather than left to ``autosql=None``, so the extra fields
#: are typed the way a real UCSC-produced file's are, not generic strings.
INTERACT_AUTOSQL = """table interact
"Interaction between two regions"
    (
    string chrom;      "Chromosome (or contig, scaffold, etc.)"
    uint chromStart;   "Start position of lower region"
    uint chromEnd;     "End position of upper region"
    string name;       "Name of item, for display"
    uint score;        "Score (0-1000)"
    double value;      "Strength of interaction"
    string exp;        "Experiment name"
    string color;      "Item color"
    string sourceChrom;  "Chromosome of source region"
    uint sourceStart;  "Start position of source region"
    uint sourceEnd;    "End position of source region"
    string sourceName;  "Identifier of source region"
    string sourceStrand; "Orientation of source region"
    string targetChrom; "Chromosome of target region"
    uint targetStart;  "Start position of target region"
    uint targetEnd;    "End position of target region"
    string targetName; "Identifier of target region"
    string targetStrand; "Orientation of target region"
    )
"""

#: Tiny single-chromosome genome, matching the scale of ``coolers.py``'s
#: fixtures — just large enough to exercise real tile geometry.
INTERACT_CHROMSIZES: dict[str, int] = {"chr1": 10_000}


class InteractionRecord(NamedTuple):
    """One bigInteract record — the fields a test wants to assert against."""

    source_start: int
    source_end: int
    target_start: int
    target_end: int
    value: float


#: Deterministic records spread across the genome, far enough apart that
#: they land in different tiles at low zoom and the same tile at high zoom.
CANONICAL_RECORDS: list[InteractionRecord] = [
    InteractionRecord(
        100 + i * 500, 200 + i * 500, 3000 + i * 500, 3100 + i * 500, 1.5 + i
    )
    for i in range(5)
]


def build_biginteract(
    path: Path,
    records: list[InteractionRecord] = CANONICAL_RECORDS,
    chromsizes: dict[str, int] = INTERACT_CHROMSIZES,
    chrom: str = "chr1",
) -> Path:
    """Write a real bed5+13 bigInteract file at ``path``."""
    from pybigtools import pybigtools as bt

    pytest.importorskip("pybigtools")

    vals = []
    for i, rec in enumerate(records):
        lo = min(rec.source_start, rec.target_start)
        hi = max(rec.source_end, rec.target_end)
        rest = "\t".join(
            [
                f"rec{i}",
                "500",
                str(rec.value),
                "exp1",
                "0,0,0",
                chrom,
                str(rec.source_start),
                str(rec.source_end),
                f"src{i}",
                ".",
                chrom,
                str(rec.target_start),
                str(rec.target_end),
                f"tgt{i}",
                ".",
            ]
        )
        vals.append((chrom, lo, hi, rest))

    writer = bt.open(str(path), "w")
    writer.write(dict(chromsizes), vals, autosql=INTERACT_AUTOSQL)
    return path
