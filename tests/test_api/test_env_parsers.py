"""Tests for the env-var parsers in :mod:`cfdb.api`."""

from __future__ import annotations

import pytest

from cfdb.api import _parse_assign_public_ip


class TestParseAssignPublicIp:
    def test__parse_assign_public_ip_with_unset_var(self, monkeypatch):
        """Test that an unset env var returns the supplied default.

        Given:
            No environment variable set for the given name.
        When:
            ``_parse_assign_public_ip`` is called with a default value.
        Then:
            It should return the default rather than raising so the PoC
            profile (no ECS env) still imports cleanly.
        """
        # Arrange
        monkeypatch.delenv("CFDB_TEST_ASSIGN_PUBLIC_IP", raising=False)

        # Act
        result = _parse_assign_public_ip(
            "CFDB_TEST_ASSIGN_PUBLIC_IP", default="DISABLED"
        )

        # Assert
        assert result == "DISABLED"

    def test__parse_assign_public_ip_with_valid_value(self, monkeypatch):
        """Test that an in-set value is returned as-is.

        Given:
            ``CFDB_TEST_ASSIGN_PUBLIC_IP=ENABLED``.
        When:
            ``_parse_assign_public_ip`` is called.
        Then:
            It should return ``"ENABLED"`` unchanged so the lifespan can
            hand it to ``EcsProvisioner`` verbatim.
        """
        # Arrange
        monkeypatch.setenv("CFDB_TEST_ASSIGN_PUBLIC_IP", "ENABLED")

        # Act
        result = _parse_assign_public_ip(
            "CFDB_TEST_ASSIGN_PUBLIC_IP", default="DISABLED"
        )

        # Assert
        assert result == "ENABLED"

    def test__parse_assign_public_ip_with_invalid_value(self, monkeypatch):
        """Test that an out-of-set value surfaces as an ImportError.

        Given:
            ``CFDB_TEST_ASSIGN_PUBLIC_IP`` set to an invalid value
            (lowercase, typo, anything outside {ENABLED, DISABLED}).
        When:
            ``_parse_assign_public_ip`` is called.
        Then:
            It should raise ``ImportError`` so the misconfiguration
            surfaces at module load rather than crashing the lifespan
            mid-bootstrap.
        """
        # Arrange
        monkeypatch.setenv("CFDB_TEST_ASSIGN_PUBLIC_IP", "enabled")

        # Act & assert
        with pytest.raises(ImportError, match="must be one of"):
            _parse_assign_public_ip(
                "CFDB_TEST_ASSIGN_PUBLIC_IP", default="DISABLED"
            )
