"""Tests for HiGlass payload assembly in ``cfdb.tilesets.wire``."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cfdb.tilesets.wire import tileset_info_payload

UID = "4dn/4DNFIABC123"

#: What the stub tileset's ``info()`` reports, mirroring the shape of
#: clodius's ``TilesetInfo`` for an explicit-ladder cooler: the extent
#: fields are None on purpose, and ``exclude_none`` must drop them.
_BASE_FIELDS = {
    "resolutions": [1000, 2000, 4000],
    "chromsizes": [["chr1", 3000], ["chr2", 3000]],
    "max_pos": [6000, 6000],
    "tile_size": 256,
    "max_width": None,
    "max_zoom": None,
}


class _StubInfo:
    """Duck-type of the pydantic info model at the clodius boundary."""

    def __init__(self, fields: dict) -> None:
        self._fields = fields

    def model_dump(self, exclude_none: bool = False) -> dict:
        if exclude_none:
            return {k: v for k, v in self._fields.items() if v is not None}
        return dict(self._fields)


class _StubTileset:
    """Duck-type of an open clodius tileset."""

    datatype = "matrix"

    def info(self) -> _StubInfo:
        return _StubInfo(_BASE_FIELDS)


def _doc(**fields) -> dict:
    """Return a projected file document carrying exactly ``fields``."""
    return dict(fields)


class TestTilesetInfoPayload:
    """The four fields the server adds on top of what the file knows."""

    @pytest.mark.parametrize(
        ("doc", "expected_name"),
        [
            (_doc(local_id="4DNFIABC123"), "4DNFIABC123"),
            (_doc(), UID),
        ],
        ids=["local-id-fallback", "uid-fallback"],
    )
    def test_should_fall_back_through_the_name_chain(self, doc, expected_name):
        """Test the display-name fallback chain.

        Given:
            A document missing ``filename`` but carrying ``local_id``, and
            a document missing both.
        When:
            tileset_info_payload is called.
        Then:
            It should name the tileset after the local_id, and after the
            uid when even that is absent — a tileset always has a name.
        """
        # Act
        payload = tileset_info_payload(UID, _StubTileset(), doc)

        # Assert
        assert payload["name"] == expected_name

    def test_should_omit_coord_system_when_the_assembly_is_empty(self):
        """Test that an empty assembly string is not emitted.

        Given:
            A document whose ``genome_assembly`` is the empty string.
        When:
            tileset_info_payload is called.
        Then:
            It should leave ``coordSystem`` out entirely — an empty string
            is exactly the plausible-looking placeholder that risks a
            silently misaligned HiGlass track, whereas an absent key makes
            the client fall back to ``chromsizes``.
        """
        # Arrange
        doc = _doc(filename="sample.mcool", genome_assembly="")

        # Act
        payload = tileset_info_payload(UID, _StubTileset(), doc)

        # Assert
        assert "coordSystem" not in payload

    _ABSENT = object()
    _field = st.one_of(
        st.just(_ABSENT),
        st.none(),
        st.just(""),
        st.text(min_size=1, max_size=20),
    )

    @settings(max_examples=50)
    @given(filename=_field, local_id=_field, assembly=_field)
    def test_should_derive_every_server_field_from_the_document(
        self, filename, local_id, assembly
    ):
        """Test the field-derivation contract over the document domain.

        Given:
            Documents whose ``filename``, ``local_id``, and
            ``genome_assembly`` are independently absent, None, empty, or
            arbitrary text.
        When:
            tileset_info_payload is called.
        Then:
            It should set ``uuid`` to the uid, name the tileset with the
            first truthy of filename, local_id, and uid, emit
            ``coordSystem`` exactly when the assembly is truthy, and pass
            the tileset's own non-None info fields through unchanged.
        """
        # Arrange
        doc: dict = {}
        for key, value in (
            ("filename", filename),
            ("local_id", local_id),
            ("genome_assembly", assembly),
        ):
            if value is not self._ABSENT:
                doc[key] = value

        # Act
        payload = tileset_info_payload(UID, _StubTileset(), doc)

        # Assert
        assert payload["uuid"] == UID
        assert payload["name"] == (
            doc.get("filename") or doc.get("local_id") or UID
        )
        if doc.get("genome_assembly"):
            assert payload["coordSystem"] == doc["genome_assembly"]
        else:
            assert "coordSystem" not in payload
        assert payload["datatype"] == "matrix"
        for key, value in _BASE_FIELDS.items():
            if value is None:
                assert key not in payload
            else:
                assert payload[key] == value
