"""Tests for the ECS cert-materialization entrypoint shim."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "cfdb-tls-entrypoint.sh"


def _run(env: dict[str, str], command: list[str], tls_dir: Path) -> subprocess.CompletedProcess:
    """Run the entrypoint shim with ``env`` and an exec target, return the result.

    ``CFDB_WORKER_TLS_DIR`` is pinned to ``tls_dir`` so materialized PEMs land
    in the test's tmp path rather than the default ``/tmp/cfdb-tls``.
    """
    full_env = {"PATH": os.environ["PATH"], "CFDB_WORKER_TLS_DIR": str(tls_dir)}
    full_env.update(env)
    return subprocess.run(
        [str(_SCRIPT), *command],
        env=full_env,
        capture_output=True,
        text=True,
        check=False,
    )


class TestEntrypoint:
    def test_entrypoint_should_materialize_pems_and_export_paths_when_pem_env_set(
        self, tmp_path
    ):
        """Test that PEM env vars become files the path vars point at.

        Given:
            CFDB_WORKER_TLS_CA/CERT/KEY_PEM hold PEM content.
        When:
            The shim runs and execs a command that echoes the resolved
            CFDB_WORKER_TLS_* path vars.
        Then:
            It should write each PEM to a file and export
            CFDB_WORKER_TLS_CA/CERT/KEY pointing at files with that content.
        """
        # Arrange
        tls_dir = tmp_path / "tls"
        env = {
            "CFDB_WORKER_TLS_CA_PEM": "ca-content",
            "CFDB_WORKER_TLS_CERT_PEM": "cert-content",
            "CFDB_WORKER_TLS_KEY_PEM": "key-content",
        }
        command = [
            "sh",
            "-c",
            'printf "%s\\n%s\\n%s\\n" '
            '"$CFDB_WORKER_TLS_CA" "$CFDB_WORKER_TLS_CERT" "$CFDB_WORKER_TLS_KEY"',
        ]

        # Act
        result = _run(env, command, tls_dir)

        # Assert
        assert result.returncode == 0
        ca_path, cert_path, key_path = result.stdout.split()
        assert Path(ca_path).read_text().strip() == "ca-content"
        assert Path(cert_path).read_text().strip() == "cert-content"
        assert Path(key_path).read_text().strip() == "key-content"

    def test_entrypoint_should_write_key_files_with_owner_only_perms(self, tmp_path):
        """Test that materialized PEM files are not world/group readable.

        Given:
            A PEM env var holding private-key content.
        When:
            The shim materializes it to a file.
        Then:
            It should create the file with 0600 permissions so the key is
            readable only by the container user.
        """
        # Arrange
        tls_dir = tmp_path / "tls"
        env = {"CFDB_WORKER_TLS_KEY_PEM": "secret-key"}
        command = ["sh", "-c", 'printf "%s" "$CFDB_WORKER_TLS_KEY"']

        # Act
        result = _run(env, command, tls_dir)

        # Assert
        assert result.returncode == 0
        mode = Path(result.stdout.strip()).stat().st_mode & 0o777
        assert mode == 0o600

    def test_entrypoint_should_pass_through_when_no_pem_env(self, tmp_path):
        """Test that the shim is inert without PEM env vars.

        Given:
            No CFDB_WORKER_TLS_*_PEM vars and an unset path var.
        When:
            The shim runs and execs a command echoing the CA path var.
        Then:
            It should leave CFDB_WORKER_TLS_CA unset and still exec the
            command (plaintext / local mounted-cert paths pass through).
        """
        # Arrange
        tls_dir = tmp_path / "tls"
        command = ["sh", "-c", 'echo "ca=[${CFDB_WORKER_TLS_CA:-unset}]"']

        # Act
        result = _run({}, command, tls_dir)

        # Assert
        assert result.returncode == 0
        assert result.stdout.strip() == "ca=[unset]"
        assert not tls_dir.exists()

    def test_entrypoint_should_preserve_preset_path_var_when_no_pem_env(self, tmp_path):
        """Test that an already-set path var survives untouched.

        Given:
            CFDB_WORKER_TLS_CA points at a mounted cert path and no
            *_PEM vars are set (the local mounted-cert case).
        When:
            The shim runs.
        Then:
            It should leave CFDB_WORKER_TLS_CA at the preset path.
        """
        # Arrange
        tls_dir = tmp_path / "tls"
        env = {"CFDB_WORKER_TLS_CA": "/mounted/ca.pem"}
        command = ["sh", "-c", 'printf "%s" "$CFDB_WORKER_TLS_CA"']

        # Act
        result = _run(env, command, tls_dir)

        # Assert
        assert result.returncode == 0
        assert result.stdout.strip() == "/mounted/ca.pem"

    def test_entrypoint_should_propagate_exec_exit_code(self, tmp_path):
        """Test that the shim execs the command rather than swallowing it.

        Given:
            A command that exits non-zero.
        When:
            The shim runs it.
        Then:
            It should exec the command so its exit code propagates.
        """
        # Arrange
        tls_dir = tmp_path / "tls"
        command = ["sh", "-c", "exit 7"]

        # Act
        result = _run({}, command, tls_dir)

        # Assert
        assert result.returncode == 7
