"""Which files are contact maps, and of what kind.

The C2M2 ontology cannot answer this. ``services/ontology_mappings.py`` maps
``hdf5``, ``hic``, ``cool``, ``mcool`` **and** ``h5ad`` all onto the single
EDAM term ``format:3590`` / ``"HDF5"`` — correctly, since every one of them
really is an HDF5 container — and ``ProcessorRegistry.lookup_for`` keys on
that name alone. So format name distinguishes a contact map from a BAM but
not from an AnnData matrix, and the filename extension is the only
discriminator the file document carries.

This module is deliberately **stdlib-only**. It is imported by both the API
and by :class:`~cfdb.workflows.processors.matrix.MatrixTilesetProcessor`,
which is cloudpickled across the wool boundary into a worker; pulling in
``processors.tools.format_name`` for the format check would drag
``workflows.cache`` and boto3 along with it. The check is three lines,
so it is inlined instead.
"""

from __future__ import annotations

from enum import Enum
from pathlib import PurePosixPath
from typing import Any

#: The EDAM term every contact-map format collapses onto upstream. Matching
#: on it first means a filename that merely *ends* in ``.cool`` cannot make
#: a non-HDF5 record look like a contact map.
HDF5_FORMAT_NAME = "HDF5"


class MatrixSource(str, Enum):
    """The contact-map container formats cfdb can serve tiles from."""

    #: Multi-resolution cooler. Already a tile pyramid; needs only to be
    #: fetched into the cache.
    MCOOL = "mcool"
    #: Single-resolution cooler. Must be coarsened into an mcool before it
    #: can be served — clodius refuses a cooler with no ``resolutions``
    #: group.
    COOL = "cool"
    #: Juicer ``.hic``. Recognized so it can be refused specifically —
    #: the tile backend reads local files only, and cfdb does not
    #: materialize ``.hic`` artifacts, so these answer 501 rather than the
    #: 404 a non-contact-map gets.
    HIC = "hic"


#: Filename suffix to container format. Lower-cased before lookup.
_SUFFIX_TO_SOURCE = {
    ".mcool": MatrixSource.MCOOL,
    ".cool": MatrixSource.COOL,
    ".hic": MatrixSource.HIC,
}


def matrix_source_kind(file_meta: dict[str, Any]) -> MatrixSource | None:
    """Classify ``file_meta`` as a contact-map container, or ``None``.

    Args:
        file_meta: A projected file document. Reads only ``file_format.name``
            and ``filename``, both of which ``FILE_DOC_PROJECTION`` selects.

    Returns:
        The :class:`MatrixSource` this file is stored in, or ``None`` when it
        is not a contact map — including for the other HDF5 formats
        (``.h5ad``, bare ``.h5``) that share the same EDAM term.

    Note:
        The extension decides **routing** only. Whether a cooler actually
        needs coarsening is settled by looking for a ``resolutions`` group
        in the file itself, because flat files named ``.mcool`` and
        multi-resolution files named ``.cool`` both exist in the wild.
    """
    file_format = file_meta.get("file_format")
    if not isinstance(file_format, dict):
        return None
    if file_format.get("name") != HDF5_FORMAT_NAME:
        return None

    filename = file_meta.get("filename")
    if not isinstance(filename, str) or not filename:
        return None

    suffix = PurePosixPath(filename.lower()).suffix
    return _SUFFIX_TO_SOURCE.get(suffix)


#: Formats that must be materialized into the cache before tiles can be
#: read. ``HIC`` is absent because cfdb does not serve it at all — see
#: :class:`MatrixSource`.
MATERIALIZED_SOURCES = frozenset({MatrixSource.MCOOL, MatrixSource.COOL})


def needs_materialization(file_meta: dict[str, Any]) -> bool:
    """True when this file must be preprocessed before it can be tiled."""
    return matrix_source_kind(file_meta) in MATERIALIZED_SOURCES


#: ``file_format.name`` for a bigInteract file — minted rather than an EDAM
#: term (``services/ontology_mappings.py``), specifically so it is distinct
#: from plain ``bigBed`` and from this module's own ``HDF5_FORMAT_NAME``.
#: Unlike :class:`MatrixSource`, a bigInteract file needs no filename-suffix
#: dance and no materialization: the name alone identifies it, and it is
#: read in place from its upstream URL — see
#: :meth:`~cfdb.tilesets.service.TilesetService._open_bbi_interaction`.
BBI_INTERACTION_FORMAT_NAME = "bigInteract"


def is_bbi_interaction_source(file_meta: dict[str, Any]) -> bool:
    """True when ``file_meta`` is a bigInteract file."""
    file_format = file_meta.get("file_format")
    return (
        isinstance(file_format, dict)
        and file_format.get("name") == BBI_INTERACTION_FORMAT_NAME
    )


#: Suffix on a tileset uid that selects the 1D arc/link presentation of a
#: bigInteract file rather than its default 2D rectangle-domain one. ``:``
#: rather than ``.`` deliberately: a uid half must stay free of ``.``
#: (clodius parses a tile id by splitting on it), and this never reaches
#: clodius at all — ``split_bbi_presentation`` strips it before anything
#: downstream sees the uid.
LINKS_PRESENTATION_SUFFIX = ":links"


def split_bbi_presentation(uid: str) -> tuple[str, str]:
    """Split a bigInteract uid into its base uid and presentation mode.

    Returns:
        ``(base_uid, presentation)``, where ``presentation`` is
        ``"links"`` when ``uid`` ends with :data:`LINKS_PRESENTATION_SUFFIX`
        and ``"rectangles"`` (the default) otherwise.
    """
    if uid.endswith(LINKS_PRESENTATION_SUFFIX):
        return uid[: -len(LINKS_PRESENTATION_SUFFIX)], "links"
    return uid, "rectangles"
