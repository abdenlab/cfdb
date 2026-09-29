"""Tests for the clodius import gate in ``cfdb.tilesets.backend``."""

from __future__ import annotations

import sys

import pytest

from cfdb.tilesets.backend import (
    is_available,
    is_tile_error,
    load_backend,
    load_hic_tileset,
)
from cfdb.tilesets.errors import TileBackendUnavailable

#: The exact modules ``load_backend`` imports from. A ``None`` entry in
#: ``sys.modules`` makes ``from X import Y`` raise ImportError, which is how
#: a build without the ``tiles`` extra is simulated in an environment that
#: does carry clodius.
_ABSENT_BACKEND_MODULES = {
    "clodius.core.errors": None,
    "clodius.tiles_v2.cooler": None,
}


class TestLoadBackend:
    """The one seam that turns the tile subsystem on or off."""

    def test_should_raise_when_clodius_is_absent(self, mocker):
        """Test the degraded import gate of a broken or incomplete install.

        Given:
            ``sys.modules`` patched so both clodius modules the gate
            imports from raise ImportError, simulating a build whose
            install of the ordinary clodius dependency is broken or
            incomplete.
        When:
            load_backend is called.
        Then:
            It should raise TileBackendUnavailable, chained from the
            underlying ImportError.
        """
        # Arrange
        mocker.patch.dict(sys.modules, _ABSENT_BACKEND_MODULES)

        # Act & assert
        with pytest.raises(
            TileBackendUnavailable, match="clodius tile backend"
        ) as excinfo:
            load_backend()
        assert isinstance(excinfo.value.__cause__, ImportError)

    def test_should_resolve_the_clodius_names(self):
        """Test the resolved-names contract of a build with clodius.

        Given:
            An environment where clodius is installed.
        When:
            load_backend is called.
        Then:
            It should return a backend whose ``cooler_tileset`` is
            clodius's ``CoolerTileset`` and whose error attributes are the
            clodius exception types the per-tile/500 split keys on.
        """
        # Arrange
        from clodius.core.errors import TileOutOfBounds, TilesetError
        from clodius.tiles_v2.cooler import CoolerTileset

        # Act
        backend = load_backend()

        # Assert
        assert backend.cooler_tileset is CoolerTileset
        assert backend.tile_error is TilesetError
        assert backend.tile_out_of_bounds is TileOutOfBounds
        assert issubclass(backend.tile_error, Exception)
        assert issubclass(backend.tile_out_of_bounds, Exception)


class TestIsAvailable:
    """The bool the lifespan and the 501/503 split gate on."""

    def test_should_return_false_when_clodius_is_absent(self, mocker):
        """Test the availability probe against a backendless build.

        Given:
            ``sys.modules`` patched so the clodius imports raise
            ImportError.
        When:
            is_available is called.
        Then:
            It should return False — the value that makes the lifespan
            skip the tile service and the routers answer 501.
        """
        # Arrange
        mocker.patch.dict(sys.modules, _ABSENT_BACKEND_MODULES)

        # Act
        result = is_available()

        # Assert
        assert result is False

    def test_should_return_true_when_clodius_is_present(self):
        """Test the availability probe against an installed backend.

        Given:
            An environment where clodius is installed and unpatched.
        When:
            is_available is called.
        Then:
            It should return True.
        """
        # Act
        result = is_available()

        # Assert
        assert result is True


class TestLoadHicTileset:
    """The .hic module gate, resolved separately from the cooler tileset."""

    def test_should_raise_when_the_hic_module_is_missing(self, mocker):
        """Test the error reported by a clodius that lost its hic module.

        Given:
            ``sys.modules`` patched so ``clodius.tiles_v2.hic`` raises
            ImportError while the cooler tileset stays importable.
        When:
            load_hic_tileset is called.
        Then:
            It should raise TileBackendUnavailable stating that mcool
            serving is unaffected, so the failure reads as the narrower
            capability gap it is.
        """
        # Arrange
        mocker.patch.dict(sys.modules, {"clodius.tiles_v2.hic": None})

        # Act & assert
        with pytest.raises(
            TileBackendUnavailable, match=r"\.mcool files are unaffected"
        ) as excinfo:
            load_hic_tileset()
        assert isinstance(excinfo.value.__cause__, ImportError)


class TestIsTileError:
    """The classification that splits per-tile payloads from 500s.

    Every test here loads a real backend and classifies against clodius's
    real error hierarchy — unlike TestLoadBackend's absent-module tests,
    there is no sys.modules trick standing in for it.
    """

    def test_should_recognize_a_clodius_tile_error(self):
        """Test that a clodius per-tile error is classified as such.

        Given:
            A loaded backend and an instance of its ``tile_error`` type.
        When:
            is_tile_error is called with that instance.
        Then:
            It should return True — the branch that renders the failure
            as a per-tile error payload instead of failing the request.
        """
        # Arrange
        backend = load_backend()
        exc = backend.tile_error("bad tile position")

        # Act
        result = is_tile_error(backend, exc)

        # Assert
        assert result is True

    def test_should_recognize_a_malformed_tile_id_as_a_tile_error(self):
        """Test that a parse-time clodius failure is classified as such too.

        Given:
            A loaded backend and an instance of clodius's
            ``MalformedTileId`` — raised by ``parse_tile_id`` before a tile
            is ever read, and no longer a subclass of the narrower
            ``TileError`` clodius restored, but still a ``TilesetError``.
        When:
            is_tile_error is called with that instance.
        Then:
            It should return True — a bad id must still degrade to a
            per-tile error payload rather than failing the whole batch.
        """
        # Arrange
        from clodius.core.errors import MalformedTileId

        backend = load_backend()
        exc = MalformedTileId("not an int")

        # Act
        result = is_tile_error(backend, exc)

        # Assert
        assert result is True

    def test_should_reject_a_non_tile_error(self):
        """Test that an ordinary exception is not classified as a tile error.

        Given:
            A loaded backend and a plain ValueError.
        When:
            is_tile_error is called with the ValueError.
        Then:
            It should return False — bugs must propagate and become 500s
            rather than being swallowed into per-tile payloads.
        """
        # Arrange
        backend = load_backend()
        exc = ValueError("a bug, not a tile problem")

        # Act
        result = is_tile_error(backend, exc)

        # Assert
        assert result is False
