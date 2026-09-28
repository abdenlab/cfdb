"""Tests for the failure vocabulary in ``cfdb.tilesets.errors``."""

from __future__ import annotations

import inspect

import cfdb.tilesets.errors as errors_module
from cfdb.tilesets.errors import TilesetError


class TestTilesetErrorHierarchy:
    """The routers catch the subsystem's failures with one except clause."""

    def test_should_derive_every_public_exception_from_tileset_error(self):
        """Test that the hierarchy is closed under its declared base.

        Given:
            Every public exception class defined in
            ``cfdb.tilesets.errors``.
        When:
            Each is checked against TilesetError.
        Then:
            It should be a subclass — the routers' single
            ``except TilesetError`` is the whole safety net, so an
            exception defined here but outside the hierarchy would escape
            it and surface as a 500.
        """
        # Arrange
        public_exceptions = [
            obj
            for name, obj in vars(errors_module).items()
            if inspect.isclass(obj)
            and issubclass(obj, BaseException)
            and obj.__module__ == errors_module.__name__
            and not name.startswith("_")
        ]

        # Act
        outsiders = [
            exc for exc in public_exceptions if not issubclass(exc, TilesetError)
        ]

        # Assert
        assert outsiders == []
        # Guard against vacuity: the module must actually define the
        # vocabulary the routers translate into status codes.
        names = {exc.__name__ for exc in public_exceptions}
        assert {
            "TilesetError",
            "TilesetUnsupported",
            "TilesetNotReady",
            "TilesetIdentityIncomplete",
            "TileBackendUnavailable",
            "TilesetTooLarge",
            "TilesetHydrationTimeout",
        } <= names
