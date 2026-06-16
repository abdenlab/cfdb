"""Tests for the wool worker mutual-TLS credential builder."""

from __future__ import annotations

import pytest

import wool
from cfdb.workflows.credentials import (
    TLS_CA_ENV,
    TLS_CERT_ENV,
    TLS_KEY_ENV,
    build_worker_credentials,
    worker_credentials_from_env,
)


def _write_pem(tmp_path, name: str) -> str:
    """Create a stand-in PEM file under ``tmp_path`` and return its path.

    The content is never parsed because ``wool.WorkerCredentials.from_files``
    is patched out in these tests; only the file's existence matters.
    """
    path = tmp_path / name
    path.write_text(f"-----BEGIN {name}-----\n")
    return str(path)


class TestBuildWorkerCredentials:
    def test_build_worker_credentials_should_return_none_when_all_unset(self):
        """Test that no cert config yields the plaintext path.

        Given:
            All three cert paths are None.
        When:
            ``build_worker_credentials`` is called.
        Then:
            It should return None so the caller stays on plaintext gRPC.
        """
        # Act
        result = build_worker_credentials(None, None, None)

        # Assert
        assert result is None

    def test_build_worker_credentials_should_return_none_when_all_empty(self):
        """Test that empty-string paths are treated as unset.

        Given:
            All three cert paths are empty strings (exported-but-empty
            env vars).
        When:
            ``build_worker_credentials`` is called.
        Then:
            It should return None rather than treating "" as a path.
        """
        # Act
        result = build_worker_credentials("", "", "")

        # Assert
        assert result is None

    @pytest.mark.parametrize(
        "ca, cert, key, missing",
        [
            ("ca.pem", None, None, (TLS_CERT_ENV, TLS_KEY_ENV)),
            (None, "cert.pem", None, (TLS_CA_ENV, TLS_KEY_ENV)),
            (None, None, "key.pem", (TLS_CA_ENV, TLS_CERT_ENV)),
            ("ca.pem", "cert.pem", None, (TLS_KEY_ENV,)),
        ],
    )
    def test_build_worker_credentials_should_raise_when_config_partial(
        self, ca, cert, key, missing
    ):
        """Test that a half-configured channel fails fast.

        Given:
            Some but not all of the three cert paths are set.
        When:
            ``build_worker_credentials`` is called.
        Then:
            It should raise ValueError naming the missing env var(s) so
            the channel cannot silently fall back to plaintext.
        """
        # Act & assert
        with pytest.raises(ValueError) as excinfo:
            build_worker_credentials(ca, cert, key)
        for name in missing:
            assert name in str(excinfo.value)

    def test_build_worker_credentials_should_raise_when_path_missing(
        self, tmp_path
    ):
        """Test that a non-existent cert path is rejected with its name.

        Given:
            All three paths are set but one does not exist on disk.
        When:
            ``build_worker_credentials`` is called.
        Then:
            It should raise ValueError naming the offending path.
        """
        # Arrange
        ca = _write_pem(tmp_path, "ca.pem")
        cert = _write_pem(tmp_path, "cert.pem")
        key = str(tmp_path / "absent-key.pem")

        # Act & assert
        with pytest.raises(ValueError, match="absent-key.pem"):
            build_worker_credentials(ca, cert, key)

    def test_build_worker_credentials_should_load_files_when_all_present(
        self, tmp_path, mocker
    ):
        """Test that complete config delegates to wool with mutual TLS.

        Given:
            All three cert paths exist on disk.
        When:
            ``build_worker_credentials`` is called with the default mutual.
        Then:
            It should return the credentials from
            ``wool.WorkerCredentials.from_files`` called with the paths
            mapped to the right kwargs and ``mutual=True``.
        """
        # Arrange
        ca = _write_pem(tmp_path, "ca.pem")
        cert = _write_pem(tmp_path, "cert.pem")
        key = _write_pem(tmp_path, "key.pem")
        sentinel = object()
        from_files = mocker.patch.object(
            wool.WorkerCredentials, "from_files", return_value=sentinel
        )

        # Act
        result = build_worker_credentials(ca, cert, key)

        # Assert
        assert result is sentinel
        from_files.assert_called_once_with(
            ca_path=ca, key_path=key, cert_path=cert, mutual=True
        )

    def test_build_worker_credentials_should_pass_mutual_false_through(
        self, tmp_path, mocker
    ):
        """Test that the mutual flag is forwarded to wool.

        Given:
            Complete cert config and ``mutual=False``.
        When:
            ``build_worker_credentials`` is called.
        Then:
            It should forward ``mutual=False`` to
            ``wool.WorkerCredentials.from_files``.
        """
        # Arrange
        ca = _write_pem(tmp_path, "ca.pem")
        cert = _write_pem(tmp_path, "cert.pem")
        key = _write_pem(tmp_path, "key.pem")
        from_files = mocker.patch.object(
            wool.WorkerCredentials, "from_files", return_value=object()
        )

        # Act
        build_worker_credentials(ca, cert, key, mutual=False)

        # Assert
        assert from_files.call_args.kwargs["mutual"] is False


class TestWorkerCredentialsFromEnv:
    def test_worker_credentials_from_env_should_return_none_when_unset(
        self, monkeypatch
    ):
        """Test that an unconfigured environment yields plaintext.

        Given:
            None of the ``CFDB_WORKER_TLS_*`` env vars are set.
        When:
            ``worker_credentials_from_env`` is called.
        Then:
            It should return None.
        """
        # Arrange
        for var in (TLS_CA_ENV, TLS_CERT_ENV, TLS_KEY_ENV):
            monkeypatch.delenv(var, raising=False)

        # Act
        result = worker_credentials_from_env()

        # Assert
        assert result is None

    def test_worker_credentials_from_env_should_read_the_env_vars(
        self, tmp_path, monkeypatch, mocker
    ):
        """Test that the builder reads cert paths from the environment.

        Given:
            The three ``CFDB_WORKER_TLS_*`` env vars point at existing
            files.
        When:
            ``worker_credentials_from_env`` is called.
        Then:
            It should delegate to ``wool.WorkerCredentials.from_files``
            with the env-supplied paths.
        """
        # Arrange
        ca = _write_pem(tmp_path, "ca.pem")
        cert = _write_pem(tmp_path, "cert.pem")
        key = _write_pem(tmp_path, "key.pem")
        monkeypatch.setenv(TLS_CA_ENV, ca)
        monkeypatch.setenv(TLS_CERT_ENV, cert)
        monkeypatch.setenv(TLS_KEY_ENV, key)
        from_files = mocker.patch.object(
            wool.WorkerCredentials, "from_files", return_value=object()
        )

        # Act
        worker_credentials_from_env()

        # Assert
        from_files.assert_called_once_with(
            ca_path=ca, key_path=key, cert_path=cert, mutual=True
        )
