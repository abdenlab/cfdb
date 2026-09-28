"""Tests for env-knob parsing at ``cfdb.workflows`` import time.

Only ``CFDB_TILESET_MAX_SOURCE_BYTES`` is exercised through a module
reload: its ``minimum=0`` no-cap sentinel is unique among the workflow
knobs, so a copy-paste refactor onto the ``minimum=1`` shape the runtime
caps use would silently break the documented "0 disables the guard"
contract. The reload pattern mirrors ``test_urlsafe.py``: mutate the env
via monkeypatch, reload the module to re-run its import-time parsing,
and reload again under a clean env in a ``finally`` so no other test
sees the mutated constants.
"""

from __future__ import annotations

import importlib

import pytest

import cfdb.workflows as workflows_module

_TWENTY_GIB = 20 * 1024**3


def test_tileset_max_source_bytes_should_parse_zero_as_no_cap_sentinel(monkeypatch):
    """Test that "0" survives parsing as the documented no-cap sentinel.

    Given:
        ``CFDB_TILESET_MAX_SOURCE_BYTES`` set to ``"0"`` and the module
        reloaded so the value is re-parsed.
    When:
        ``TILESET_MAX_SOURCE_BYTES`` is read from the reloaded module.
    Then:
        It should equal 0 — the unique ``minimum=0`` sentinel that
        disables the source-size guard rather than rejecting it.
    """
    # Arrange
    monkeypatch.setenv("CFDB_TILESET_MAX_SOURCE_BYTES", "0")
    try:
        # Act
        reloaded = importlib.reload(workflows_module)

        # Assert
        assert reloaded.TILESET_MAX_SOURCE_BYTES == 0
    finally:
        # Restore the default module state.
        monkeypatch.delenv("CFDB_TILESET_MAX_SOURCE_BYTES", raising=False)
        importlib.reload(workflows_module)


def test_tileset_max_source_bytes_should_default_to_twenty_gib_when_unset(monkeypatch):
    """Test that an unset env var yields the documented 20 GiB default.

    Given:
        ``CFDB_TILESET_MAX_SOURCE_BYTES`` absent from the environment and
        the module reloaded.
    When:
        ``TILESET_MAX_SOURCE_BYTES`` is read from the reloaded module.
    Then:
        It should equal 21474836480 (20 GiB), the README-documented
        default cap.
    """
    # Arrange
    monkeypatch.delenv("CFDB_TILESET_MAX_SOURCE_BYTES", raising=False)
    try:
        # Act
        reloaded = importlib.reload(workflows_module)

        # Assert
        assert reloaded.TILESET_MAX_SOURCE_BYTES == _TWENTY_GIB
    finally:
        # Restore the default module state.
        importlib.reload(workflows_module)


def test_tileset_max_source_bytes_should_raise_when_negative(monkeypatch):
    """Test that a negative cap fails fast at import with the var named.

    Given:
        ``CFDB_TILESET_MAX_SOURCE_BYTES`` set to ``"-1"``.
    When:
        The module is reloaded so import-time parsing re-runs.
    Then:
        It should raise ValueError naming
        ``CFDB_TILESET_MAX_SOURCE_BYTES`` so a misconfigured deployment
        fails at boot rather than admitting every source.
    """
    # Arrange
    monkeypatch.setenv("CFDB_TILESET_MAX_SOURCE_BYTES", "-1")
    try:
        # Act & assert
        with pytest.raises(ValueError, match="CFDB_TILESET_MAX_SOURCE_BYTES"):
            importlib.reload(workflows_module)
    finally:
        # Restore the default module state.
        monkeypatch.delenv("CFDB_TILESET_MAX_SOURCE_BYTES", raising=False)
        importlib.reload(workflows_module)
