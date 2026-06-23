"""Tests for the ECS worker entrypoint argument parsing."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from cfdb.workflows import worker_main


def _invoke(args: list[str]) -> tuple[int, dict[str, object]]:
    """Run ``worker_main.main`` with ``args``, capturing the ``serve`` kwargs.

    Returns ``(exit_code, captured_kwargs)``. The real ``serve`` is patched
    out — the entrypoint always calls ``raise SystemExit(asyncio.run(serve(...)))``,
    so swapping ``asyncio.run`` for ``lambda coro: coro.close() or 0`` plus
    stubbing ``serve`` keeps the test from actually booting a worker.
    """
    captured: dict[str, object] = {}

    async def _fake_serve(**kwargs: object) -> int:
        captured.update(kwargs)
        return 0

    runner = CliRunner()
    with patch.object(worker_main, "serve", _fake_serve):
        result = runner.invoke(worker_main.main, args, standalone_mode=True)
    return result.exit_code, captured


class TestMainCli:
    def test_main_uses_documented_defaults_when_no_args_or_env(self, monkeypatch):
        """Test that bare invocation surfaces the documented defaults.

        Given:
            No CLI arguments and no overriding environment variables.
        When:
            ``worker_main.main`` is invoked.
        Then:
            ``serve`` should be called with the documented worker port,
            health port, max lifetime, and drain-grace defaults so a
            bare container ``CMD`` works.
        """
        # Arrange — clear any env overrides so defaults apply
        for var in (
            "CFDB_WORKER_GRPC_PORT",
            "CFDB_WORKER_HEALTH_PORT",
            "CFDB_WORKER_MAX_LIFETIME_SECONDS",
            "CFDB_WORKER_DRAIN_GRACE_SECONDS",
            "CFDB_WORKER_TLS_CA",
            "CFDB_WORKER_TLS_CERT",
            "CFDB_WORKER_TLS_KEY",
        ):
            monkeypatch.delenv(var, raising=False)

        # Act
        exit_code, captured = _invoke([])

        # Assert
        assert exit_code == 0
        assert captured["worker_port"] == worker_main.DEFAULT_WORKER_PORT
        assert captured["health_port"] == worker_main.DEFAULT_HEALTH_PORT
        assert captured["max_lifetime_seconds"] == worker_main.DEFAULT_MAX_LIFETIME_SECONDS
        assert captured["drain_grace_seconds"] == worker_main.DEFAULT_DRAIN_GRACE_SECONDS
        assert captured["tls_ca"] is None
        assert captured["tls_cert"] is None
        assert captured["tls_key"] is None

    def test_main_with_env_overrides(self, monkeypatch):
        """Test that environment variables override the defaults.

        Given:
            ``CFDB_WORKER_GRPC_PORT`` and friends set to non-default values.
        When:
            ``worker_main.main`` is invoked with no CLI flags.
        Then:
            ``serve`` should receive the env-driven values.
        """
        # Arrange
        monkeypatch.setenv("CFDB_WORKER_GRPC_PORT", "60001")
        monkeypatch.setenv("CFDB_WORKER_HEALTH_PORT", "9001")
        monkeypatch.setenv("CFDB_WORKER_MAX_LIFETIME_SECONDS", "1800")
        monkeypatch.setenv("CFDB_WORKER_DRAIN_GRACE_SECONDS", "10")

        # Act
        exit_code, captured = _invoke([])

        # Assert
        assert exit_code == 0
        assert captured["worker_port"] == 60001
        assert captured["health_port"] == 9001
        assert captured["max_lifetime_seconds"] == 1800.0
        assert captured["drain_grace_seconds"] == 10.0

    def test_main_cli_flags_override_env_vars(self, monkeypatch):
        """Test that CLI flags win over environment variables.

        Given:
            ``CFDB_WORKER_GRPC_PORT`` set in the environment.
        When:
            ``worker_main.main`` is invoked with an explicit
            ``--worker-port`` CLI flag.
        Then:
            ``serve`` should receive the CLI-supplied value.
        """
        # Arrange
        monkeypatch.setenv("CFDB_WORKER_GRPC_PORT", "60001")

        # Act
        exit_code, captured = _invoke(["--worker-port", "55555"])

        # Assert
        assert exit_code == 0
        assert captured["worker_port"] == 55555

    def test_main_rejects_out_of_range_worker_port(self, monkeypatch):
        """Test that a port outside [1, 65535] is rejected at parse time.

        Given:
            A ``--worker-port`` value above 65535.
        When:
            ``worker_main.main`` is invoked.
        Then:
            Click should exit non-zero before reaching ``serve``, so the
            failure surfaces clearly rather than as an opaque bind error
            later.
        """
        # Arrange
        for var in ("CFDB_WORKER_GRPC_PORT",):
            monkeypatch.delenv(var, raising=False)

        # Act
        exit_code, captured = _invoke(["--worker-port", "99999"])

        # Assert
        assert exit_code != 0
        assert "worker_port" not in captured

    def test_main_passes_tls_paths_from_env(self, monkeypatch):
        """Test that the TLS cert paths flow from env into serve.

        Given:
            ``CFDB_WORKER_TLS_CA`` / ``CFDB_WORKER_TLS_CERT`` /
            ``CFDB_WORKER_TLS_KEY`` set in the environment.
        When:
            ``worker_main.main`` is invoked with no TLS CLI flags.
        Then:
            ``serve`` should receive the env-driven cert paths so the
            container can enable mTLS purely via env vars.
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
            ``worker_main.main`` is invoked with an explicit ``--tls-ca``
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


class _StopServe(Exception):
    """Sentinel raised from the mocked ``worker.start`` to halt ``serve``.

    ``serve`` constructs the worker (with its backpressure kwarg) immediately
    before ``await worker.start()``, so raising here exits the coroutine right
    after the construction under test without entering the run loop, the
    signal-wait, or the health/drain teardown.
    """


def _arrange_serve(mocker) -> object:
    """Patch ``serve``'s collaborators and return the ``LocalWorker`` spy."""
    mocker.patch.object(worker_main, "build_worker_credentials", return_value=None)
    mocker.patch.object(
        worker_main, "_start_health_server", mocker.AsyncMock(return_value=mocker.Mock())
    )
    worker_instance = mocker.Mock()
    worker_instance.start = mocker.AsyncMock(side_effect=_StopServe)
    worker_instance.stop = mocker.AsyncMock()
    return mocker.patch.object(
        worker_main.wool, "LocalWorker", return_value=worker_instance
    )


class TestServeBackpressureWiring:
    @pytest.mark.asyncio
    async def test_serve_should_wire_taskcount_backpressure_when_threshold_positive(
        self, mocker, monkeypatch
    ):
        """Test that a positive task ceiling becomes a TaskCountBackpressure.

        Given:
            ``WORKER_MAX_CONCURRENT_TASKS`` set to 1 and ``wool.LocalWorker``
            spied so ``serve`` halts right after constructing the worker.
        When:
            ``serve`` is run.
        Then:
            The worker is constructed with a ``backpressure`` hook whose
            threshold is 1, so the ECS worker entrypoint actually serializes
            its subprocess pipelines.
        """
        # Arrange
        monkeypatch.setattr(worker_main, "WORKER_MAX_CONCURRENT_TASKS", 1)
        local_worker = _arrange_serve(mocker)

        # Act
        with pytest.raises(_StopServe):
            await worker_main.serve(worker_port=0, health_port=0)

        # Assert
        backpressure = local_worker.call_args.kwargs["backpressure"]
        assert backpressure is not None
        assert backpressure.threshold == 1

    @pytest.mark.asyncio
    async def test_serve_should_disable_backpressure_when_threshold_zero(
        self, mocker, monkeypatch
    ):
        """Test that a zero task ceiling wires ``backpressure=None``.

        Given:
            ``WORKER_MAX_CONCURRENT_TASKS`` set to 0 (the disable sentinel)
            and ``wool.LocalWorker`` spied.
        When:
            ``serve`` is run.
        Then:
            The worker is constructed with ``backpressure=None``, restoring
            the unbounded admission behavior.
        """
        # Arrange
        monkeypatch.setattr(worker_main, "WORKER_MAX_CONCURRENT_TASKS", 0)
        local_worker = _arrange_serve(mocker)

        # Act
        with pytest.raises(_StopServe):
            await worker_main.serve(worker_port=0, health_port=0)

        # Assert
        assert local_worker.call_args.kwargs["backpressure"] is None
