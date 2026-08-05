"""Tests for the ECS worker entrypoint argument parsing."""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest
import pytest_asyncio
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
