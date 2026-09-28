"""HiGlass response payload assembly.

clodius produces tile payloads that are already JSON-ready and a
``TilesetInfo`` that is deliberately incomplete as a wire document: it
models what can be read out of the *file*, and leaves what only a server
knows — the dataset's name, its identifier, the coordinate system it is
annotated against — to the server. This module supplies exactly that
difference.
"""

from __future__ import annotations

from typing import Any

from cfdb.tilesets.backend import datatype_of


def tileset_info_payload(
    uid: str, tileset: Any, file_doc: dict[str, Any]
) -> dict[str, Any]:
    """Build the ``tileset_info`` document HiGlass expects for ``uid``.

    Args:
        uid: The tileset identifier, ``{dcc}/{local_id}``.
        tileset: An open clodius tileset.
        file_doc: The projected file document, for the display name and
            the coordinate system.

    Returns:
        clodius's own info fields, plus the four the library does not
        model.
    """
    payload = tileset.info().model_dump(exclude_none=True)

    # ``exclude_none`` above drops ``max_width`` and ``max_zoom``, both of
    # which clodius leaves None for an explicit-ladder tileset on purpose:
    # for a cooler the extent belongs to the zoom level, not the tileset,
    # and a single value taken from the coarsest resolution over-reports
    # the tile count at every finer zoom. Emitting them as JSON ``null``
    # would invite a client to do arithmetic on them; absence is what the
    # explicit-ladder path expects.
    payload["datatype"] = datatype_of(tileset)
    payload["name"] = file_doc.get("filename") or file_doc.get("local_id") or uid
    payload["uuid"] = uid

    # Omitted entirely when unknown — 21 of the 4DN contact maps carry no
    # assembly. HiGlass matches ``coordSystem`` against chromosome-info
    # tilesets, so a plausible-looking placeholder ("", "unknown") risks a
    # silently misaligned track, whereas an absent key makes the client
    # fall back to the ``chromsizes`` array that every tileset_info
    # carries. The heatmap still renders either way.
    coord_system = file_doc.get("genome_assembly")
    if coord_system:
        payload["coordSystem"] = coord_system

    return payload


def error_payload(message: str) -> dict[str, str]:
    """A per-entry error object.

    HiGlass renders these in place of tile data rather than treating the
    response as failed, which is what keeps one malformed tile id in a
    batch of sixty from blanking the whole track.
    """
    return {"error": message}
