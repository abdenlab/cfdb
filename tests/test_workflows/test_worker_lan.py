"""Tests for the local-dev LAN worker entrypoint argument parsing."""

from __future__ import annotations

from unittest.mock import patch

from click.testing import CliRunner

from cfdb.workflows import worker_lan


def _invoke(args: list[str]) -> tuple[int, dict[str, object]]:
    """Run ``worker_lan.main`` with ``args``, capturing the ``serve`` kwargs.

    Returns ``(exit_code, captured_kwargs)``. The real ``serve`` is patched
    out so the test never actually spawns a worker pool.
    """
    captured: dict[str, object] = {}

    async def _fake_serve(**kwargs: object) -> int:
        captured.update(kwargs)
        return 0

    runner = CliRunner()
    with patch.object(worker_lan, "serve", _fake_serve):
        result = runner.invoke(worker_lan.main, args, standalone_mode=True)
    return result.exit_code, captured


class TestMainCli:
    def test_main_uses_documented_defaults_when_no_args_or_env(self, monkeypatch):
        """Test that bare invocation surfaces the documented defaults.

        Given:
            No CLI arguments and no overriding environment variables.
        When:
            ``worker_lan.main`` is invoked.
        Then:
            ``serve`` should be called with the documented namespace and
            worker-count defaults so a bare ``python -m`` launch pairs
            with a bare API.
        """
        # Arrange — clear any env overrides so defaults apply
        for var in (
            "WORKFLOW_POOL_NAMESPACE",
            "WORKFLOW_WORKER_COUNT",
            "CFDB_WORKER_TLS_CA",
            "CFDB_WORKER_TLS_CERT",
            "CFDB_WORKER_TLS_KEY",
        ):
            monkeypatch.delenv(var, raising=False)

        # Act
        exit_code, captured = _invoke([])

        # Assert
        assert exit_code == 0
        assert captured["namespace"] == worker_lan.DEFAULT_NAMESPACE
        assert captured["workers"] == worker_lan.DEFAULT_WORKER_COUNT
        assert captured["tls_ca"] is None
        assert captured["tls_cert"] is None
        assert captured["tls_key"] is None

    def test_main_with_env_overrides(self, monkeypatch):
        """Test that environment variables override the defaults.

        Given:
            ``WORKFLOW_POOL_NAMESPACE`` and ``WORKFLOW_WORKER_COUNT`` set
            to non-default values.
        When:
            ``worker_lan.main`` is invoked with no CLI flags.
        Then:
            ``serve`` should receive the env-driven values so the same
            env vars size both the API and the publisher.
        """
        # Arrange
        monkeypatch.setenv("WORKFLOW_POOL_NAMESPACE", "cfdb-dev")
        monkeypatch.setenv("WORKFLOW_WORKER_COUNT", "4")

        # Act
        exit_code, captured = _invoke([])

        # Assert
        assert exit_code == 0
        assert captured["namespace"] == "cfdb-dev"
        assert captured["workers"] == 4

    def test_main_cli_flags_override_env_vars(self, monkeypatch):
        """Test that CLI flags win over environment variables.

        Given:
            ``WORKFLOW_POOL_NAMESPACE`` set in the environment.
        When:
            ``worker_lan.main`` is invoked with an explicit
            ``--namespace`` CLI flag.
        Then:
            ``serve`` should receive the CLI-supplied value.
        """
        # Arrange
        monkeypatch.setenv("WORKFLOW_POOL_NAMESPACE", "cfdb-dev")

        # Act
        exit_code, captured = _invoke(["--namespace", "cfdb-cli"])

        # Assert
        assert exit_code == 0
        assert captured["namespace"] == "cfdb-cli"

    def test_main_rejects_non_positive_worker_count(self, monkeypatch):
        """Test that a worker count below 1 is rejected at parse time.

        Given:
            A ``--workers`` value of 0.
        When:
            ``worker_lan.main`` is invoked.
        Then:
            Click should exit non-zero before reaching ``serve`` so the
            misconfiguration surfaces clearly rather than spawning an
            empty pool the API blocks against.
        """
        # Arrange
        monkeypatch.delenv("WORKFLOW_WORKER_COUNT", raising=False)

        # Act
        exit_code, captured = _invoke(["--workers", "0"])

        # Assert
        assert exit_code != 0
        assert "workers" not in captured

    def test_main_passes_tls_paths_from_env(self, monkeypatch):
        """Test that the TLS cert paths flow from env into serve.

        Given:
            ``CFDB_WORKER_TLS_CA`` / ``CFDB_WORKER_TLS_CERT`` /
            ``CFDB_WORKER_TLS_KEY`` set in the environment.
        When:
            ``worker_lan.main`` is invoked with no TLS CLI flags.
        Then:
            ``serve`` should receive the env-driven cert paths so the
            LAN pool can enable mTLS purely via env vars.
        """
        # Arrange
        monkeypatch.setenv("CFDB_WORKER_TLS_CA", "/c/ca.pem")
        monkeypatch.setenv("CFDB_WORKER_TLS_CERT", "/c/worker-cert.pem")
        monkeypatch.setenv("CFDB_WORKER_TLS_KEY", "/c/worker-key.pem")

        # Act
        exit_code, captured = _invoke([])

        # Assert
        assert exit_code == 0
        assert captured["tls_ca"] == "/c/ca.pem"
        assert captured["tls_cert"] == "/c/worker-cert.pem"
        assert captured["tls_key"] == "/c/worker-key.pem"

    def test_main_tls_cli_flags_override_env_vars(self, monkeypatch):
        """Test that TLS CLI flags win over environment variables.

        Given:
            ``CFDB_WORKER_TLS_CA`` set in the environment.
        When:
            ``worker_lan.main`` is invoked with an explicit ``--tls-ca``
            CLI flag.
        Then:
            ``serve`` should receive the CLI-supplied cert path.
        """
        # Arrange
        monkeypatch.setenv("CFDB_WORKER_TLS_CA", "/env/ca.pem")

        # Act
        exit_code, captured = _invoke(["--tls-ca", "/cli/ca.pem"])

        # Assert
        assert exit_code == 0
        assert captured["tls_ca"] == "/cli/ca.pem"
