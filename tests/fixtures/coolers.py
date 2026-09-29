"""Synthetic cooler builders for the matrix tile-serving tests.

Ported from the clodius fork's ``test/harness/builders.py`` so cfdb's tile
tests run against a real file rather than a mock — the payload assertions
(a 256x256 dense block, base64-encoded, with no ``shape`` field) are only
meaningful against bytes clodius actually produced.

Plain functions rather than pytest fixtures, so a test that needs a file
per case can call one directly; ``conftest.py`` wraps them for the ordinary
shared-file case.

Two writer behaviours are not what the documentation suggests, and both
cost the clodius authors a debugging round:

* ``cooler.create_cooler`` defaults to ``mode="w"``, which **truncates the
  whole file**. Writing several resolutions in a loop silently leaves only
  the last one, with no error at all.
* Bins must be declared as an ordered ``Categorical`` or the chromosome
  order in the resulting file is not the order you passed.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

#: Tiling genome, 3000 bp total. Deliberately tiny relative to the bin
#: sizes below: a cooler tile is 256 bins wide, so a realistic 1 kb binsize
#: on this genome would put the whole thing inside a single bin of a single
#: tile — one real value and 65,535 padding cells, exercising no geometry
#: at all. Three chromosomes of unequal length so tiles straddle boundaries.
CANONICAL_CHROMSIZES: list[tuple[str, int]] = [
    ("c1", 1000),
    ("c2", 1500),
    ("c3", 500),
]

CANONICAL_TOTAL = 3000


def build_cool(
    path: Path,
    chromsizes: list[tuple[str, int]] = CANONICAL_CHROMSIZES,
    binsize: int = 4,
    seed: int = 0,
    weights: dict[str, float] | None = None,
) -> Path:
    """A flat, single-resolution cooler — the shape that needs coarsening."""
    return _write_cooler(str(path), chromsizes, binsize, seed, weights, mode="w")


def build_mcool(
    path: Path,
    chromsizes: list[tuple[str, int]] = CANONICAL_CHROMSIZES,
    resolutions: tuple[int, ...] = (1, 2, 4),
    seed: int = 0,
    weights: dict[str, float] | None = None,
) -> Path:
    """A multi-resolution cooler, one group per entry in ``resolutions``.

    ``weights`` maps a bin-column name to a constant value, adding that
    column at every resolution, which is what makes clodius's balancing
    modifiers reachable. It takes a mapping rather than a single value
    because a column named ``weight`` is also the one
    ``resolve_balance`` falls back to by default — so a cooler carrying
    only that column cannot distinguish honouring a named request from
    ignoring it. Pass a second name to make the request observable.
    """
    for i, binsize in enumerate(sorted(resolutions)):
        _write_cooler(
            f"{path}::/resolutions/{binsize}",
            chromsizes,
            binsize,
            seed,
            weights,
            # The first call creates the file; every later one MUST append,
            # or it truncates what the previous call just wrote.
            mode="w" if i == 0 else "a",
        )
    return path


def build_variable_bin_cool(
    path: Path,
    chromsizes: list[tuple[str, int]] = CANONICAL_CHROMSIZES,
    seed: int = 0,
) -> Path:
    """A flat cooler with an irregular bin table (``Cooler.binsize`` is None).

    The restriction-fragment shape the matrix processor refuses to coarsen:
    ``cooler zoomify``'s binary ladder is meaningless over variable-width
    bins, so ``run()`` must raise rather than emit a ladder that
    misrepresents them.
    """
    cooler = pytest.importorskip("cooler")
    pd = pytest.importorskip("pandas")
    rng = np.random.default_rng(seed)

    rows = []
    for name, length in chromsizes:
        cuts = np.sort(rng.choice(np.arange(1, int(length)), size=7, replace=False))
        starts = np.concatenate(([0], cuts))
        ends = np.concatenate((cuts, [int(length)]))
        rows.append(pd.DataFrame({"chrom": name, "start": starts, "end": ends}))
    bins = pd.concat(rows, ignore_index=True)
    bins["chrom"] = pd.Categorical(
        bins["chrom"],
        categories=[name for name, _ in chromsizes],
        ordered=True,
    )

    n = len(bins)
    b1 = rng.integers(0, n, size=100)
    b2 = rng.integers(0, n, size=100)
    pixels = (
        pd.DataFrame(
            {
                "bin1_id": np.minimum(b1, b2),
                "bin2_id": np.maximum(b1, b2),
                "count": 1,
            }
        )
        .groupby(["bin1_id", "bin2_id"], as_index=False)["count"]
        .sum()
        .sort_values(["bin1_id", "bin2_id"])
        .reset_index(drop=True)
    )

    cooler.create_cooler(
        str(path), bins, pixels, mode="w", ordered=True, symmetric_upper=True
    )
    return path


def _write_cooler(
    uri: str,
    chromsizes: list[tuple[str, int]],
    binsize: int,
    seed: int,
    weights: dict[str, float] | None,
    mode: str,
) -> Path:
    """Write one cooler at ``uri`` with a deterministic random pixel table."""
    cooler = pytest.importorskip("cooler")
    pd = pytest.importorskip("pandas")
    rng = np.random.default_rng(seed)

    rows = []
    for name, length in chromsizes:
        starts = np.arange(0, int(length), binsize)
        ends = np.minimum(starts + binsize, int(length))
        rows.append(pd.DataFrame({"chrom": name, "start": starts, "end": ends}))
    bins = pd.concat(rows, ignore_index=True)
    bins["chrom"] = pd.Categorical(
        bins["chrom"],
        categories=[name for name, _ in chromsizes],
        ordered=True,
    )
    for column, value in (weights or {}).items():
        bins[column] = float(value)

    n = len(bins)
    b1 = rng.integers(0, n, size=min(4 * n, 400))
    b2 = rng.integers(0, n, size=min(4 * n, 400))
    pixels = (
        pd.DataFrame(
            {
                "bin1_id": np.minimum(b1, b2),
                "bin2_id": np.maximum(b1, b2),
                "count": 1,
            }
        )
        .groupby(["bin1_id", "bin2_id"], as_index=False)["count"]
        .sum()
        .sort_values(["bin1_id", "bin2_id"])
        .reset_index(drop=True)
    )

    cooler.create_cooler(
        uri, bins, pixels, mode=mode, ordered=True, symmetric_upper=True
    )
    return Path(uri.split("::")[0])
