"""Tests for the outbound-URL allowlist guard."""

from __future__ import annotations

import importlib

import pytest

from cfdb.workflows import urlsafe as urlsafe_module


def _validate(url: str) -> str:
    """Look up ``validate_outbound_url`` from the live module each call.

    The loopback-env test reloads ``urlsafe_module`` to pick up an env
    flag, which would otherwise leave the test file's top-level
    ``from … import …`` bindings stale (pointing at the pre-reload
    function whose globals now reference a NEW ``UnsafeOutboundURL``).
    Looking up the function via the module attribute on every call keeps
    each test bound to whatever symbol is currently exported.
    """
    return urlsafe_module.validate_outbound_url(url)


def _unsafe_exc() -> type[Exception]:
    """Return the live ``UnsafeOutboundURL`` class from the module."""
    return urlsafe_module.UnsafeOutboundURL


class TestValidateOutboundURL:
    def test_validate_outbound_url_should_accept_s3_subdomain_allowlist_match(self):
        """Test that an S3 subdomain in the allowlist passes through.

        Given:
            An ``https://encode-public.s3.amazonaws.com`` URL.
        When:
            ``validate_outbound_url`` is called.
        Then:
            It should return the URL unchanged because the host matches an
            allowlisted suffix.
        """
        # Arrange
        url = "https://encode-public.s3.amazonaws.com/x.bam"

        # Act
        result = _validate(url)

        # Assert
        assert result == url

    def test_validate_outbound_url_should_accept_exact_netloc_allowlist_match(self):
        """Test that the 4DN portal host passes through.

        Given:
            An ``https://data.4dnucleome.org`` URL (an exact-netloc entry
            in ``ALLOWED_NETLOC_SUFFIXES``).
        When:
            ``validate_outbound_url`` is called.
        Then:
            It should return the URL unchanged.
        """
        # Arrange
        url = "https://data.4dnucleome.org/files/x.tbi"

        # Act
        result = _validate(url)

        # Assert
        assert result == url

    def test_validate_outbound_url_should_reject_http_scheme(self):
        """Test that bare ``http://`` schemes are blocked (IMDS protection).

        Given:
            An ``http://169.254.169.254`` URL targeting AWS instance
            metadata.
        When:
            ``validate_outbound_url`` is called.
        Then:
            It should raise ``UnsafeOutboundURL`` naming the scheme.
        """
        # Arrange
        url = "http://169.254.169.254/latest/meta-data/"

        # Act & assert
        with pytest.raises(_unsafe_exc(), match="scheme"):
            _validate(url)

    def test_validate_outbound_url_should_reject_non_allowlisted_host(self):
        """Test that an https URL pointing to an attacker host is rejected.

        Given:
            An ``https://attacker.example.com`` URL.
        When:
            ``validate_outbound_url`` is called.
        Then:
            It should raise ``UnsafeOutboundURL`` whose message names the
            allowlist violation.
        """
        # Arrange
        url = "https://attacker.example.com/x"

        # Act & assert
        with pytest.raises(_unsafe_exc(), match="host not in allowlist"):
            _validate(url)

    def test_validate_outbound_url_should_accept_drs_scheme_with_host(self):
        """Test that DRS URIs short-circuit on scheme.

        Given:
            A ``drs://drs.hubmapconsortium.org`` URI.
        When:
            ``validate_outbound_url`` is called.
        Then:
            It should return the URI unchanged — the HTTPS host allowlist
            is enforced AFTER DRS resolution, not on the broker URI.
        """
        # Arrange
        url = "drs://drs.hubmapconsortium.org/abc"

        # Act
        result = _validate(url)

        # Assert
        assert result == url

    def test_validate_outbound_url_should_reject_drs_uri_with_empty_host(self):
        """Test that an empty DRS host is rejected.

        Given:
            A ``drs://`` URI with no netloc.
        When:
            ``validate_outbound_url`` is called.
        Then:
            It should raise ``UnsafeOutboundURL`` matching "missing DRS host".
        """
        # Arrange
        url = "drs://"

        # Act & assert
        with pytest.raises(_unsafe_exc(), match="missing DRS host"):
            _validate(url)

    def test_validate_outbound_url_should_accept_url_with_userinfo(self):
        """Test that user:pass prefixes do not bypass the allowlist.

        Given:
            An HTTPS URL with ``user:pass@`` userinfo and an allowlisted
            host.
        When:
            ``validate_outbound_url`` is called.
        Then:
            It should return the URL unchanged — the userinfo is stripped
            before the allowlist comparison.
        """
        # Arrange
        url = "https://user:pass@encode-public.s3.amazonaws.com/x"

        # Act
        result = _validate(url)

        # Assert
        assert result == url

    def test_validate_outbound_url_should_accept_loopback_http_when_env_flag_set(
        self, monkeypatch
    ):
        """Test that ``CFDB_URLSAFE_ALLOW_HTTP_LOOPBACK=1`` enables loopback.

        Given:
            ``CFDB_URLSAFE_ALLOW_HTTP_LOOPBACK=1`` is set and the module
            is reloaded so the flag is picked up.
        When:
            ``validate_outbound_url`` is called on an ``http://127.0.0.1``
            fixture URL.
        Then:
            It should return the URL unchanged.
        """
        # Arrange
        monkeypatch.setenv("CFDB_URLSAFE_ALLOW_HTTP_LOOPBACK", "1")
        try:
            reloaded = importlib.reload(urlsafe_module)

            # Act
            result = reloaded.validate_outbound_url("http://127.0.0.1:8080/fixture")

            # Assert
            assert result == "http://127.0.0.1:8080/fixture"
        finally:
            # Restore the default module state.
            monkeypatch.delenv("CFDB_URLSAFE_ALLOW_HTTP_LOOPBACK", raising=False)
            importlib.reload(urlsafe_module)

    def test_validate_outbound_url_should_reject_https_with_empty_netloc(self):
        """Test that ``https://`` with no host is rejected.

        Given:
            An ``https://`` URL with no netloc.
        When:
            ``validate_outbound_url`` is called.
        Then:
            It should raise ``UnsafeOutboundURL`` matching "missing netloc".
        """
        # Arrange
        url = "https://"

        # Act & assert
        with pytest.raises(_unsafe_exc(), match="missing netloc"):
            _validate(url)
