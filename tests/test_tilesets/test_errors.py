"""Tests for the failure vocabulary in ``cfdb.tilesets.errors``."""

from __future__ import annotations

import inspect

import cfdb.tilesets.errors as errors_module
from cfdb.tilesets.errors import TilesetError, TilesetSourceUnavailable


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


class TestTilesetSourceUnavailable:
    """The one TilesetError carrying its own HTTP status code."""

    def test_should_default_the_status_code_to_502(self):
        """Test the constructor's default when no status_code is given.

        Given:
            TilesetSourceUnavailable constructed with only a message.
        When:
            Its status_code attribute is read.
        Then:
            It should be 502 — every real call site passes an explicit
            status_code, so this default is otherwise never exercised.
        """
        # Act
        exc = TilesetSourceUnavailable("upstream unreachable")

        # Assert
        assert exc.status_code == 502

    def test_should_preserve_an_explicit_status_code(self):
        """Test that a passed-in status_code is stored as given.

        Given:
            TilesetSourceUnavailable constructed with an explicit
            status_code.
        When:
            Its status_code attribute is read.
        Then:
            It should equal the value passed in, and the instance
            should still be a TilesetError.
        """
        # Act
        exc = TilesetSourceUnavailable("not found upstream", status_code=404)

        # Assert
        assert exc.status_code == 404
        assert isinstance(exc, TilesetError)
        assert str(exc) == "not found upstream"
