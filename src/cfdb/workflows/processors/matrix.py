"""Materialize a Hi-C contact map into a tile-servable cooler.

Produces a single artifact kind, :attr:`~cfdb.workflows.models.ArtifactKind.TILESET`
— a multi-resolution cooler that the tile server opens locally and reads
256x256 blocks out of. Unlike DATA and INDEX, it is never streamed to a
client.

Two source shapes reach this processor:

- **Already multi-resolution** (the 773 4DN ``.mcool`` files). The upstream
  file *is* the artifact; it only needs to be in the cache, on a filesystem
  h5py can open.
- **Flat** (the 10 4DN ``.cool`` files). Coarsened into an mcool first,
  because ``clodius.tiles_v2.cooler.CoolerTileset`` refuses a cooler with no
  ``resolutions`` group.

``.hic`` never reaches here: it is read in place over HTTP range requests
and so needs no artifact at all. See :mod:`cfdb.tilesets.formats`.
"""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from cfdb.tilesets.formats import (
    MatrixSource,
    matrix_source_kind,
    needs_materialization,
)
from cfdb.workflows import TILESET_MAX_SOURCE_BYTES
from cfdb.workflows.cache import CacheBackend
from cfdb.workflows.events import Complete, StageComplete, WorkflowEvent
from cfdb.workflows.fetcher import download_source
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.base import Processor

logger = logging.getLogger(__name__)

#: HiGlass tile dimension, in bins. The coarsest resolution worth
#: generating is the one that fits the whole genome into a single tile;
#: past that a zoom level adds nothing. Matches ``cooler.cli.zoomify``'s
#: ``HIGLASS_TILE_DIM`` and ``clodius.tiles_v2.cooler.TILE_SIZE``.
TILE_DIM = 256

#: Pixels processed per chunk during coarsening. Mirrors the
#: ``cooler zoomify`` CLI default.
ZOOMIFY_CHUNKSIZE = int(10e6)


def _has_resolutions_group(path: Path) -> bool:
    """True when ``path`` is already a multi-resolution cooler.

    The filename is not authoritative — flat files named ``.mcool`` and
    multi-resolution files named ``.cool`` both exist upstream — so the
    coarsening decision is made by looking inside the container. This is
    the same group ``CoolerTileset.file`` checks for before serving.
    """
    import h5py

    with h5py.File(path, "r") as handle:
        return "resolutions" in handle


def _zoom_ladder(path: Path) -> list[int]:
    """Resolutions to coarsen a flat cooler onto.

    Reproduces what ``cooler zoomify`` computes for its default binary
    progression: geometric steps of 2 from the base bin size up to the
    resolution at which the whole genome fits in one HiGlass tile.
    Deriving it here rather than passing ``resolutions=None`` is required
    — ``cooler.zoomify_cooler`` takes the list as a mandatory argument.
    """
    import cooler
    from cooler.cli.zoomify import preferred_sequence

    clr = cooler.Cooler(str(path))
    binsize = clr.binsize
    if not binsize:
        # A variable-length bin table (restriction-fragment cooler) has no
        # single base resolution to double from, so the standard ladder is
        # undefined. Refuse rather than emit a ladder that misrepresents
        # the bins.
        raise RuntimeError(
            "cannot build a zoom ladder for a cooler with variable-length "
            "bins; only fixed-binsize coolers can be coarsened"
        )

    genome_length = int(clr.chromsizes.sum())
    coarsest = int(math.ceil(genome_length / TILE_DIM))
    ladder = preferred_sequence(binsize, coarsest, "binary")

    # ``preferred_sequence`` returns [] when the base resolution is
    # already coarser than the whole-genome tile — a small genome, which
    # every test fixture is. The base resolution is still a valid (and the
    # only) zoom level.
    return ladder or [binsize]


def _zoomify(source: Path, dest: Path) -> None:
    """Coarsen a flat cooler at ``source`` into an mcool at ``dest``.

    Runs the Python API rather than shelling out to ``cooler zoomify``.
    The console script is only incidentally on ``PATH`` — a consequence of
    a transitive install rather than something either Dockerfile declares
    — so a missing entry point would surface at job time as an opaque
    ``FileNotFoundError`` instead of an ImportError at startup. The API
    also raises real exceptions instead of stderr that has to be parsed.
    """
    import cooler

    resolutions = _zoom_ladder(source)
    logger.info(f"Coarsening {source.name} onto resolutions {resolutions}")
    cooler.zoomify_cooler(
        str(source),
        str(dest),
        resolutions,
        ZOOMIFY_CHUNKSIZE,
        # One worker: CFDB_WORKER_MAX_CONCURRENT_TASKS defaults to 1 on a
        # 1-vCPU worker, so a process pool would only contend with itself.
        nproc=1,
    )


class MatrixTilesetProcessor(Processor):
    """Prepare a contact map for random-access tile reads."""

    processor_version = 0

    # Every contact-map container collapses onto this one EDAM term
    # upstream, alongside AnnData and bare HDF5. Claiming the whole term
    # here is safe because the real discrimination happens in
    # ``needs_processing`` / ``artifact_kinds_produced`` below, and every
    # consumer of ``ProcessorRegistry.lookup_for`` consults one or the
    # other before acting.
    supported_formats = frozenset({"HDF5"})
    artifact_kinds = (ArtifactKind.TILESET,)

    def needs_processing(self, file_meta: dict[str, Any]) -> bool:
        """True only for coolers — the formats that need an artifact built.

        Returns False for ``.h5ad`` and bare ``.h5`` (not contact maps at
        all) and for ``.hic`` (a contact map, but one served in place over
        HTTP range requests). In every one of those cases the callers of
        ``lookup_for`` fall through exactly as they did before this
        processor existed.
        """
        return needs_materialization(file_meta)

    def artifact_kinds_produced(
        self, file_meta: dict[str, Any] | None = None
    ) -> tuple[ArtifactKind, ...]:
        """Artifact kinds this processor writes for ``file_meta``.

        Overridden because ``/index`` asks this question *without* first
        consulting :meth:`needs_processing` — it uses an empty result to
        mean "this format has no index in any state of the world". Without
        the override, an ``.h5ad`` would advertise a TILESET it will never
        produce.
        """
        if file_meta is None:
            return self.artifact_kinds
        return self.artifact_kinds if needs_materialization(file_meta) else ()

    async def run(
        self,
        file_meta: dict[str, Any],
        workdir: Path,
        cache: CacheBackend,
    ) -> AsyncIterator[WorkflowEvent]:
        """Fetch the contact map, coarsen it if flat, and cache it."""
        workdir.mkdir(parents=True, exist_ok=True)
        tileset_key = self.cache_key_for(file_meta, ArtifactKind.TILESET)

        if await cache.head(tileset_key) is None:
            artifact = await self._materialize(file_meta, workdir)
            await cache.put(tileset_key, artifact)
        yield StageComplete(kind=ArtifactKind.TILESET, key=tileset_key)

        yield Complete(artifacts={ArtifactKind.TILESET.value: tileset_key})

    async def _materialize(
        self, file_meta: dict[str, Any], workdir: Path
    ) -> Path:
        """Produce the multi-resolution cooler to be committed."""
        self._assert_source_within_size_cap(file_meta)

        source = workdir / "source.h5"
        await download_source(file_meta, source)

        if await asyncio.to_thread(_has_resolutions_group, source):
            # Already a pyramid. The upstream bytes are the artifact.
            return source

        if matrix_source_kind(file_meta) is MatrixSource.MCOOL:
            logger.info(
                "%s is named .mcool but carries no resolutions group; "
                "coarsening it as a flat cooler",
                file_meta.get("filename"),
            )

        out = workdir / "tileset.mcool"
        await asyncio.to_thread(_zoomify, source, out)
        return out

    def _assert_source_within_size_cap(self, file_meta: dict[str, Any]) -> None:
        """Refuse an oversized source before a byte is downloaded.

        The TILESET artifact for an ``.mcool`` is a copy of the upstream
        file, so an oversized source — or a mislabelled one, such as a
        multi-tens-of-GB ``.hic`` that reached this processor by a filename
        that lies — would fill the worker's disk and fail somewhere far
        less legible than here.
        """
        if not TILESET_MAX_SOURCE_BYTES:
            return
        size = file_meta.get("size_in_bytes")
        if isinstance(size, str):
            # 4DN and HuBMAP load size_in_bytes as a string through the
            # C2M2 TSV path (the README's BigInt note), and 4DN is the
            # only DCC whose contact maps reach this processor — an
            # int-only check leaves the cap inert for exactly the corpus
            # it exists to bound.
            try:
                size = int(size)
            except ValueError:
                size = None
        if not isinstance(size, int) or size <= TILESET_MAX_SOURCE_BYTES:
            return
        raise RuntimeError(
            f"{file_meta.get('filename') or file_meta.get('local_id')} is "
            f"{size} bytes, above the "
            f"{TILESET_MAX_SOURCE_BYTES}-byte tileset source cap "
            "(CFDB_TILESET_MAX_SOURCE_BYTES); refusing to materialize it"
        )
