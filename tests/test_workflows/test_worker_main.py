"""Tests for the ECS worker entrypoint argument parsing."""

from __future__ import annotations

from unittest.mock import patch

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
