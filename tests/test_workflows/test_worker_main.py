"""Tests for the ECS worker entrypoint argument parsing."""

from __future__ import annotations

import logging
from unittest.mock import patch

import grpc
import pytest
import pytest_asyncio
import wool
from botocore.exceptions import ClientError
from click.testing import CliRunner

from cfdb.workflows import worker_main
from cfdb.workflows.constants import TLS_IDENTITY_ENV


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
            "CFDB_WORKER_IDLE_TIMEOUT_SECONDS",
            "CFDB_WORKER_IDLE_POLL_INTERVAL_SECONDS",
            "CFDB_WORKER_IDLE_POLL_FAILURE_LIMIT",
            "CFDB_WORKER_MAX_LIFETIME_SECONDS",
            "CFDB_WORKER_MAX_LIFETIME_GRACE_SECONDS",
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
        assert captured["idle_timeout_seconds"] == worker_main.DEFAULT_IDLE_TIMEOUT_SECONDS
        assert (
            captured["idle_poll_interval_seconds"]
            == worker_main.DEFAULT_IDLE_POLL_INTERVAL_SECONDS
        )
        assert (
            captured["idle_poll_failure_limit"]
            == worker_main.DEFAULT_IDLE_POLL_FAILURE_LIMIT
        )
        assert captured["max_lifetime_seconds"] == worker_main.DEFAULT_MAX_LIFETIME_SECONDS
        assert (
            captured["max_lifetime_grace_seconds"]
            == worker_main.DEFAULT_MAX_LIFETIME_GRACE_SECONDS
        )
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
        monkeypatch.setenv("CFDB_WORKER_IDLE_TIMEOUT_SECONDS", "60")
        monkeypatch.setenv("CFDB_WORKER_IDLE_POLL_INTERVAL_SECONDS", "5")
        monkeypatch.setenv("CFDB_WORKER_IDLE_POLL_FAILURE_LIMIT", "7")
        monkeypatch.setenv("CFDB_WORKER_MAX_LIFETIME_SECONDS", "1800")
        monkeypatch.setenv("CFDB_WORKER_MAX_LIFETIME_GRACE_SECONDS", "900")
        monkeypatch.setenv("CFDB_WORKER_DRAIN_GRACE_SECONDS", "10")

        # Act
        exit_code, captured = _invoke([])

        # Assert
        assert exit_code == 0
        assert captured["worker_port"] == 60001
        assert captured["health_port"] == 9001
        assert captured["idle_timeout_seconds"] == 60.0
        assert captured["idle_poll_interval_seconds"] == 5.0
        assert captured["idle_poll_failure_limit"] == 7
        assert captured["max_lifetime_seconds"] == 1800.0
        assert captured["max_lifetime_grace_seconds"] == 900.0
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
        monkeypatch.setenv("CFDB_WORKER_IDLE_TIMEOUT_SECONDS", "60")
        monkeypatch.setenv("CFDB_WORKER_MAX_LIFETIME_GRACE_SECONDS", "7200")

        # Act
        exit_code, captured = _invoke(
            [
                "--worker-port",
                "55555",
                "--idle-timeout-seconds",
                "30",
                "--max-lifetime-grace-seconds",
                "3600",
            ]
        )

        # Assert
        assert exit_code == 0
        assert captured["worker_port"] == 55555
        assert captured["idle_timeout_seconds"] == 30.0
        assert captured["max_lifetime_grace_seconds"] == 3600.0

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


def _arrange_idle_serve(mocker, monkeypatch, *, idle_effect):
    """Patch ``serve``'s collaborators for a full run-loop pass.

    Unlike ``_arrange_serve`` the fake worker starts cleanly, so ``serve``
    enters its run loop and exercises the idle-poll path for real; the
    health server is left unpatched — tests pass ``health_port=0`` so it
    binds an ephemeral port for the run's duration. ``idle_effect``
    becomes the fake connection's ``idle`` side effect. Returns
    ``(worker, connection_cls, connection)``.
    """
    monkeypatch.delenv("ECS_CONTAINER_METADATA_URI_V4", raising=False)
    mocker.patch.object(worker_main, "build_worker_credentials", return_value=None)
    worker_instance = mocker.Mock()
    worker_instance.start = mocker.AsyncMock()
    worker_instance.stop = mocker.AsyncMock()
    mocker.patch.object(
        worker_main.wool, "LocalWorker", return_value=worker_instance
    )
    connection = mocker.Mock()
    connection.idle = mocker.AsyncMock(side_effect=idle_effect)
    connection.close = mocker.AsyncMock()
    connection_cls = mocker.patch.object(
        worker_main.wool, "WorkerConnection", return_value=connection
    )
    return worker_instance, connection_cls, connection


class TestServeIdleShutdown:
    @pytest.mark.asyncio
    async def test_serve_should_exit_when_idle_exceeds_timeout(
        self, mocker, monkeypatch, caplog
    ):
        """Test that crossing the idle threshold shuts the worker down.

        Given:
            A worker whose idle RPC reports more continuous idle time
            than the configured idle timeout.
        When:
            ``serve`` is run.
        Then:
            It should exit through the idle path on the first poll —
            with a bounded per-poll RPC deadline, and stopping the
            worker with the drain grace so a dispatch racing the
            teardown completes instead of being cancelled.
        """
        # Arrange
        worker, _, connection = _arrange_idle_serve(
            mocker, monkeypatch, idle_effect=[10.0]
        )

        # Act
        with caplog.at_level(logging.INFO, logger=worker_main.__name__):
            result = await worker_main.serve(
                worker_port=0,
                health_port=0,
                idle_timeout_seconds=5.0,
                idle_poll_interval_seconds=0.01,
                max_lifetime_seconds=60.0,
                max_lifetime_grace_seconds=7.5,
            )

        # Assert
        assert result == 0
        assert any("Idle for" in record.message for record in caplog.records)
        assert connection.idle.await_count == 1
        poll_timeout = connection.idle.await_args.kwargs["timeout"]
        assert 0 < poll_timeout < float("inf")
        worker.stop.assert_awaited_once_with(grace=7.5)

    @pytest.mark.asyncio
    async def test_serve_should_keep_serving_when_worker_busy(
        self, mocker, monkeypatch, caplog
    ):
        """Test that a busy worker is never reaped by the idle path.

        Given:
            A worker whose idle RPC always reports zero (work in
            flight) and a short max lifetime.
        When:
            ``serve`` is run.
        Then:
            It should poll idle at least once without exiting on it and
            terminate via the max-lifetime backstop instead — stopping
            the worker with the drain grace so the in-flight job the
            expiry interrupted completes rather than being cancelled.
        """
        # Arrange
        worker, _, connection = _arrange_idle_serve(
            mocker, monkeypatch, idle_effect=lambda **_: 0.0
        )

        # Act
        with caplog.at_level(logging.INFO, logger=worker_main.__name__):
            result = await worker_main.serve(
                worker_port=0,
                health_port=0,
                idle_timeout_seconds=5.0,
                idle_poll_interval_seconds=0.01,
                max_lifetime_seconds=0.5,
                max_lifetime_grace_seconds=7.5,
            )

        # Assert
        assert result == 0
        assert connection.idle.await_count >= 1
        assert any("Max lifetime" in record.message for record in caplog.records)
        worker.stop.assert_awaited_once_with(grace=7.5)

    @pytest.mark.asyncio
    async def test_serve_should_not_dial_idle_connection_when_timeout_zero(
        self, mocker, monkeypatch
    ):
        """Test that the disable sentinel skips the idle connection.

        Given:
            ``idle_timeout_seconds`` set to 0 and a short max lifetime.
        When:
            ``serve`` is run.
        Then:
            It should never construct a ``WorkerConnection``, restoring
            the pure max-lifetime behavior.
        """
        # Arrange
        _, connection_cls, _ = _arrange_idle_serve(
            mocker, monkeypatch, idle_effect=[0.0]
        )

        # Act
        result = await worker_main.serve(
            worker_port=0,
            health_port=0,
            idle_timeout_seconds=0.0,
            max_lifetime_seconds=1e-6,
        )

        # Assert
        assert result == 0
        connection_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_serve_should_fall_back_to_max_lifetime_when_idle_rpc_unimplemented(
        self, mocker, monkeypatch
    ):
        """Test that a worker without the idle RPC disables idle polling.

        Given:
            A worker whose idle RPC raises ``IdleUnavailable`` and a
            short max lifetime.
        When:
            ``serve`` is run.
        Then:
            It should poll exactly once, keep serving, and exit via the
            max-lifetime backstop — a version-skew scenario must not
            crash-loop the poll.
        """
        # Arrange
        _, _, connection = _arrange_idle_serve(
            mocker,
            monkeypatch,
            idle_effect=wool.IdleUnavailable("no idle rpc"),
        )

        # Act
        result = await worker_main.serve(
            worker_port=0,
            health_port=0,
            idle_timeout_seconds=5.0,
            idle_poll_interval_seconds=0.01,
            max_lifetime_seconds=0.5,
        )

        # Assert
        assert result == 0
        assert connection.idle.await_count == 1
        connection.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_serve_should_keep_polling_when_idle_poll_fails_transiently(
        self, mocker, monkeypatch, caplog
    ):
        """Test that a flaky idle poll is retried rather than fatal.

        Given:
            A worker whose idle RPC fails transiently once and then
            reports idle time beyond the threshold.
        When:
            ``serve`` is run.
        Then:
            It should survive the failed poll and exit via the idle
            path on the next cadence, so a flaky poll never kills a
            worker.
        """
        # Arrange
        _, _, connection = _arrange_idle_serve(
            mocker,
            monkeypatch,
            idle_effect=[
                wool.TransientRpcError(grpc.StatusCode.UNAVAILABLE, "poll failed"),
                10.0,
            ],
        )

        # Act
        with caplog.at_level(logging.INFO, logger=worker_main.__name__):
            result = await worker_main.serve(
                worker_port=0,
                health_port=0,
                idle_timeout_seconds=5.0,
                idle_poll_interval_seconds=0.01,
                max_lifetime_seconds=60.0,
            )

        # Assert
        assert result == 0
        assert connection.idle.await_count == 2
        assert any("Idle for" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_serve_should_dial_loopback_with_the_worker_credentials(
        self, mocker, monkeypatch, caplog
    ):
        """Test that the idle connection targets the worker's own port.

        Given:
            A credentials builder returning a sentinel object and a
            non-default worker port.
        When:
            ``serve`` is run until the idle path exits it.
        Then:
            It should construct the ``WorkerConnection`` against
            loopback at the worker port with those same credentials, so
            the idle poll verifies mTLS the same way wool's own
            drain channel does.
        """
        # Arrange
        credentials = object()
        _, connection_cls, _ = _arrange_idle_serve(
            mocker, monkeypatch, idle_effect=[10.0]
        )
        mocker.patch.object(
            worker_main, "build_worker_credentials", return_value=credentials
        )

        # Act
        with caplog.at_level(logging.INFO, logger=worker_main.__name__):
            await worker_main.serve(
                worker_port=50055,
                health_port=0,
                idle_timeout_seconds=5.0,
                idle_poll_interval_seconds=0.01,
                max_lifetime_seconds=60.0,
            )

        # Assert
        connection_cls.assert_called_once_with(
            "127.0.0.1:50055", credentials=credentials
        )
        assert any("Idle for" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_serve_should_close_idle_connection_on_shutdown(
        self, mocker, monkeypatch, caplog
    ):
        """Test that shutdown releases the idle connection's resources.

        Given:
            A worker that exits via the idle path.
        When:
            ``serve`` returns.
        Then:
            It should close the ``WorkerConnection`` so pooled channels
            are released alongside the worker's own teardown.
        """
        # Arrange
        _, _, connection = _arrange_idle_serve(
            mocker, monkeypatch, idle_effect=[10.0]
        )

        # Act
        with caplog.at_level(logging.INFO, logger=worker_main.__name__):
            await worker_main.serve(
                worker_port=0,
                health_port=0,
                idle_timeout_seconds=5.0,
                idle_poll_interval_seconds=0.01,
                max_lifetime_seconds=60.0,
            )

        # Assert
        connection.close.assert_awaited_once()
        assert any("Idle for" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_serve_should_disable_idle_polling_when_poll_fails_persistently(
        self, mocker, monkeypatch, caplog
    ):
        """Test that sustained poll failure escalates once and stops polling.

        Given:
            A worker whose idle RPC fails on every poll and a failure
            limit of two.
        When:
            ``serve`` is run.
        Then:
            It should emit exactly one ERROR naming the max-lifetime
            bound, poll no further, and exit via the backstop — one
            actionable signal instead of hours of identical warnings.
        """
        # Arrange
        _, _, connection = _arrange_idle_serve(
            mocker, monkeypatch, idle_effect=RuntimeError("subprocess dead")
        )

        # Act
        with caplog.at_level(logging.INFO, logger=worker_main.__name__):
            result = await worker_main.serve(
                worker_port=0,
                health_port=0,
                idle_timeout_seconds=5.0,
                idle_poll_interval_seconds=0.01,
                idle_poll_failure_limit=2,
                max_lifetime_seconds=2.5,
            )

        # Assert
        assert result == 0
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "disabling idle shutdown" in errors[0].getMessage()
        assert "max-lifetime backstop" in errors[0].getMessage()
        assert connection.idle.await_count == 2

    @pytest.mark.asyncio
    async def test_serve_should_keep_polling_when_idle_poll_fails_nontransiently(
        self, mocker, monkeypatch, caplog
    ):
        """Test that isolated failures below the limit never escalate.

        Given:
            A worker whose idle RPC fails non-transiently, succeeds
            (resetting the consecutive-failure count), fails again, and
            then reports idle beyond the threshold — with a failure
            limit of two.
        When:
            ``serve`` is run.
        Then:
            It should retry through both isolated failures without
            escalating to ERROR and exit via the idle path, so only
            *consecutive* failures count toward the limit.
        """
        # Arrange
        _, _, connection = _arrange_idle_serve(
            mocker,
            monkeypatch,
            idle_effect=[
                RuntimeError("blip"),
                0.0,
                RuntimeError("blip"),
                10.0,
            ],
        )

        # Act
        with caplog.at_level(logging.INFO, logger=worker_main.__name__):
            result = await worker_main.serve(
                worker_port=0,
                health_port=0,
                idle_timeout_seconds=5.0,
                idle_poll_interval_seconds=0.01,
                idle_poll_failure_limit=2,
                max_lifetime_seconds=60.0,
            )

        # Assert
        assert result == 0
        assert connection.idle.await_count == 4
        assert not [r for r in caplog.records if r.levelno == logging.ERROR]
        assert any("Idle for" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_serve_should_keep_serving_when_idle_below_threshold(
        self, mocker, monkeypatch, caplog
    ):
        """Test that partial idle accumulation does not trigger the exit.

        Given:
            A worker whose idle RPC reports idle time strictly between
            zero and the threshold, and a short max lifetime.
        When:
            ``serve`` is run.
        Then:
            It should keep serving through those polls and exit via the
            max-lifetime backstop, so a worker inside an ordinary
            dispatch gap is not reaped early.
        """
        # Arrange
        _, _, connection = _arrange_idle_serve(
            mocker, monkeypatch, idle_effect=lambda **_: 3.0
        )

        # Act
        with caplog.at_level(logging.INFO, logger=worker_main.__name__):
            result = await worker_main.serve(
                worker_port=0,
                health_port=0,
                idle_timeout_seconds=5.0,
                idle_poll_interval_seconds=0.01,
                max_lifetime_seconds=0.5,
            )

        # Assert
        assert result == 0
        assert connection.idle.await_count >= 1
        assert any("Max lifetime" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_serve_should_stop_worker_when_idle_connection_close_fails(
        self, mocker, monkeypatch
    ):
        """Test that a close failure does not skip the worker teardown.

        Given:
            A worker exiting via the idle path whose ``WorkerConnection``
            raises on ``close``.
        When:
            ``serve`` runs to completion.
        Then:
            It should still return 0 and stop the worker, so a teardown
            hiccup on the poll channel never leaks the worker itself.
        """
        # Arrange
        worker, _, connection = _arrange_idle_serve(
            mocker, monkeypatch, idle_effect=[10.0]
        )
        connection.close = mocker.AsyncMock(side_effect=RuntimeError("close failed"))

        # Act
        result = await worker_main.serve(
            worker_port=0,
            health_port=0,
            idle_timeout_seconds=5.0,
            idle_poll_interval_seconds=0.01,
            max_lifetime_seconds=60.0,
        )

        # Assert
        assert result == 0
        worker.stop.assert_awaited_once()


#: Task ARN the fake ECS metadata endpoint reports.
_TASK_ARN = "arn:aws:ecs:us-east-2:605134458779:task/cfdb-cluster/abc123"


@pytest_asyncio.fixture()
async def ecs_metadata_endpoint():
    """Serve a stand-in for the ECS task metadata endpoint.

    Yields the base URL to export as ``ECS_CONTAINER_METADATA_URI_V4``.
    A real HTTP server rather than a patched fetch, so the request and
    the ``TaskARN`` parsing are both exercised.

    A loopback socket makes these tests borderline integration by the
    letter of the test guide, but they stay in the unit suite
    deliberately: the server stands in for a link-local endpoint that
    only exists inside an ECS task (there is no real counterpart to
    integrate with off ECS), everything else in the tests is mocked,
    and the alternative — patching ``aiohttp.ClientSession`` — would
    un-test the two things this fixture exists to exercise.
    """
    from aiohttp import web

    async def _task(_request: web.Request) -> web.Response:
        return web.json_response({"TaskARN": _TASK_ARN, "Cluster": "cfdb-cluster"})

    app = web.Application()
    app.router.add_get("/task", _task)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


def _arrange_started_worker(mocker, *, version: str = "1.2.3", secure: bool = False):
    """Patch ``wool.LocalWorker`` with one that starts and reports metadata."""
    worker_instance = mocker.Mock()
    worker_instance.start = mocker.AsyncMock()
    worker_instance.stop = mocker.AsyncMock()
    worker_instance.metadata = mocker.Mock(version=version, secure=secure)
    mocker.patch.object(
        worker_main.wool, "LocalWorker", return_value=worker_instance
    )
    return worker_instance


class TestServeMetadataPublishing:
    @pytest.mark.asyncio
    async def test_serve_should_not_publish_metadata_when_not_running_on_ecs(
        self, mocker, monkeypatch
    ):
        """Test that a worker off ECS makes no tagging call.

        Given:
            ``ECS_CONTAINER_METADATA_URI_V4`` unset, as on a laptop.
        When:
            ``serve`` runs to its max-lifetime exit.
        Then:
            No ECS client is built and nothing is tagged, so running the
            entrypoint locally is unaffected by the publish step.
        """
        # Arrange
        monkeypatch.delenv("ECS_CONTAINER_METADATA_URI_V4", raising=False)
        _arrange_started_worker(mocker)
        build_client = mocker.patch.object(worker_main, "build_ecs_client")

        # Act
        await worker_main.serve(
            worker_port=0, health_port=0, max_lifetime_seconds=0.01
        )

        # Assert
        build_client.assert_not_called()

    @pytest.mark.asyncio
    async def test_serve_should_publish_the_metadata_the_worker_authored(
        self, mocker, monkeypatch, ecs_metadata_endpoint
    ):
        """Test that the worker tags its own task with wool's metadata.

        Given:
            A running ECS task metadata endpoint and a worker whose wool
            metadata reports a version and a TLS flag.
        When:
            ``serve`` runs to its max-lifetime exit.
        Then:
            It should tag its own task ARN with exactly those values —
            they are what ``EcsDiscovery`` reads back, and wool admits
            the worker only if they are right.
        """
        # Arrange
        monkeypatch.setenv("ECS_CONTAINER_METADATA_URI_V4", ecs_metadata_endpoint)
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        _arrange_started_worker(mocker, version="9.9.9", secure=True)
        client = mocker.Mock()
        build_client = mocker.patch.object(
            worker_main, "build_ecs_client", return_value=client
        )

        # Act
        await worker_main.serve(
            worker_port=0, health_port=0, max_lifetime_seconds=0.01
        )

        # Assert
        client.tag_resource.assert_called_once_with(
            resourceArn=_TASK_ARN,
            tags=[
                {"key": "wool.version", "value": "9.9.9"},
                {"key": "wool.secure", "value": "true"},
            ],
        )
        # With AWS_REGION unset the region comes from the task ARN
        # itself, so publishing cannot die of NoRegionError on a task
        # definition that lost the variable.
        build_client.assert_called_once_with(
            endpoint_url=None, region_name="us-east-2"
        )

    @pytest.mark.asyncio
    async def test_serve_should_raise_when_metadata_cannot_be_published(
        self, mocker, monkeypatch, ecs_metadata_endpoint
    ):
        """Test that an unpublishable worker exits instead of serving.

        Given:
            A metadata endpoint but an ECS client whose ``tag_resource``
            is throttled on every attempt, exhausting the retry budget.
        When:
            ``serve`` runs with a two-attempt publish budget.
        Then:
            It should retry the transient failure once and then
            propagate rather than serve, because a worker whose metadata
            never lands is invisible to the API and would hold a Fargate
            slot without being able to receive work.
        """
        # Arrange
        monkeypatch.setenv("ECS_CONTAINER_METADATA_URI_V4", ecs_metadata_endpoint)
        _arrange_started_worker(mocker)
        client = mocker.Mock()
        client.tag_resource.side_effect = ClientError(
            {"Error": {"Code": "ThrottlingException", "Message": "slow down"}},
            "TagResource",
        )
        mocker.patch.object(worker_main, "build_ecs_client", return_value=client)

        # Act & assert
        with pytest.raises(ClientError, match="Throttling"):
            await worker_main.serve(
                worker_port=0,
                health_port=0,
                max_lifetime_seconds=0.01,
                publish_attempts=2,
                publish_backoff_seconds=0.0,
            )
        assert client.tag_resource.call_count == 2

    @pytest.mark.asyncio
    async def test_serve_should_raise_without_retrying_when_tagging_is_denied(
        self, mocker, monkeypatch, ecs_metadata_endpoint, caplog
    ):
        """Test that a missing ecs:TagResource grant fails in one attempt.

        Given:
            An ECS client whose ``tag_resource`` raises AccessDenied —
            the signature of a workers stack deployed without the
            ``ecs:TagResource`` grant.
        When:
            ``serve`` runs with the full five-attempt budget available.
        Then:
            It should raise after a single attempt — authorization
            failures are permanent, so burning the backoff budget only
            delays the exit — and the error log should name the grant
            and the stack that provides it, so the operator's first
            grep answers "what do I deploy".
        """
        # Arrange
        monkeypatch.setenv("ECS_CONTAINER_METADATA_URI_V4", ecs_metadata_endpoint)
        _arrange_started_worker(mocker)
        client = mocker.Mock()
        client.tag_resource.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "no"}},
            "TagResource",
        )
        mocker.patch.object(worker_main, "build_ecs_client", return_value=client)

        # Act & assert
        with caplog.at_level(logging.ERROR, logger=worker_main.__name__):
            with pytest.raises(ClientError, match="AccessDenied"):
                await worker_main.serve(
                    worker_port=0,
                    health_port=0,
                    max_lifetime_seconds=0.01,
                    publish_backoff_seconds=0.0,
                )
        assert client.tag_resource.call_count == 1
        assert "ecs:TagResource" in caplog.text
        assert "workers.yml" in caplog.text


class _StopAtCredentials(Exception):
    """Sentinel raised from the patched credentials builder.

    ``serve`` builds credentials as its very first statement, so raising
    here exits the coroutine before the signal, health, worker, or
    publish machinery is touched — the call arguments are the entire
    subject.
    """


class TestServeIdentityWiring:
    @pytest.mark.asyncio
    async def test_serve_should_pass_the_configured_identity_to_the_credentials_builder(
        self, mocker, monkeypatch
    ):
        """Test that the ECS worker gets the environment identity.

        Given:
            ``CFDB_WORKER_TLS_IDENTITY`` set to a non-default name.
        When:
            ``serve`` builds its worker credentials.
        Then:
            It should pass that identity through, because wool's
            graceful-stop RPC dials this worker's own subprocess and
            verifies an identity-only certificate on that channel —
            dropping the kwarg turns every graceful drain into a
            force-reap that loses in-flight work, with no TLS error
            anywhere.
        """
        # Arrange
        monkeypatch.setenv(TLS_IDENTITY_ENV, "custom-name")
        build = mocker.patch.object(
            worker_main, "build_worker_credentials", side_effect=_StopAtCredentials
        )

        # Act
        with pytest.raises(_StopAtCredentials):
            await worker_main.serve(
                worker_port=0, health_port=0, tls_ca="/ca", tls_cert="/c", tls_key="/k"
            )

        # Assert
        build.assert_called_once_with("/ca", "/c", "/k", identity="custom-name")

    @pytest.mark.asyncio
    async def test_serve_should_verify_by_address_when_identity_opted_out(
        self, mocker, monkeypatch
    ):
        """Test that the empty-string opt-out reaches the ECS worker.

        Given:
            ``CFDB_WORKER_TLS_IDENTITY`` exported as the empty string,
            the documented opt-out.
        When:
            ``serve`` builds its worker credentials.
        Then:
            It should pass ``identity=None`` so verification falls back
            to the dialed address on both sides of the channel.
        """
        # Arrange
        monkeypatch.setenv(TLS_IDENTITY_ENV, "")
        build = mocker.patch.object(
            worker_main, "build_worker_credentials", side_effect=_StopAtCredentials
        )

        # Act
        with pytest.raises(_StopAtCredentials):
            await worker_main.serve(
                worker_port=0, health_port=0, tls_ca="/ca", tls_cert="/c", tls_key="/k"
            )

        # Assert
        build.assert_called_once_with("/ca", "/c", "/k", identity=None)
