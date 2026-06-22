"""Tests for the lifespan wiring of ``wool.WorkerPool`` (issue #54 Bug 2).

Bug 2 was that the lifespan constructed ``wool.WorkerPool`` without
``quorum=0``, so the default quorum readiness gate (quorum=1, ~30s
timeout) fired on a cold-start dispatch while the Fargate worker was
still booting and raised an un-retried ``TimeoutError`` out of
``WorkerPool.start``. Nothing exercised the enabled-workflow path of the
lifespan, so the missing kwarg had no coverage. These tests drive the
lifespan with the workflow subsystem ENABLED and assert the pool is
constructed once with ``quorum=0`` and the discovery / credentials
forwarded.
"""

from __future__ import annotations

import contextlib
import inspect

import pytest
import wool

from cfdb.api import main
from cfdb.api import profile as profile_mod


@pytest.mark.asyncio
async def test_lifespan_should_construct_worker_pool_with_quorum_zero(
    mocker, tmp_path
):
    """Test that the enabled-workflow lifespan builds the pool with quorum=0.

    Given:
        The workflow subsystem enabled (a local ``WorkflowProfile``), a
        mongomock database, the cache / provisioner / discovery builders
        stubbed, and ``wool.WorkerPool`` replaced by an async-context
        spy.
    When:
        The app lifespan starts up and tears down.
    Then:
        ``wool.WorkerPool`` is constructed exactly once with ``quorum=0``
        (the Bug 2 fix that stops the quorum gate from failing cold-start
        dispatches), with the discovery yielded by ``_build_discovery``
        and the credentials from ``build_worker_credentials`` forwarded.
    """
    # Arrange
    from mongomock_motor import AsyncMongoMockClient

    client = AsyncMongoMockClient()
    profile = profile_mod.WorkflowProfile(
        kind="local",
        cache_root=tmp_path / "cache",
        workdir_root=tmp_path / "jobs",
    )

    pool_calls: list[dict] = []

    class _PoolSpy:
        def __init__(self, **kwargs):
            pool_calls.append(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return None

    sentinel_discovery = object()

    @contextlib.asynccontextmanager
    async def _fake_build_discovery(_profile):
        yield sentinel_discovery

    fake_executor = mocker.MagicMock()
    fake_executor.drain = mocker.AsyncMock(return_value=0)

    mocker.patch.object(main, "create_mongodb_client", return_value=client)
    mocker.patch.object(main.WorkflowProfile, "from_env", return_value=profile)
    mocker.patch.object(
        main, "_build_cache", new=mocker.AsyncMock(return_value=mocker.MagicMock())
    )
    mocker.patch.object(main, "_build_provisioner", return_value=None)
    mocker.patch.object(main, "_build_discovery", new=_fake_build_discovery)
    mocker.patch.object(main, "default_registry", return_value=mocker.MagicMock())
    mocker.patch.object(
        main, "build_worker_credentials", return_value="CREDS-SENTINEL"
    )
    mocker.patch.object(main, "WoolExecutor", return_value=fake_executor)
    mocker.patch.object(main.wool, "WorkerPool", _PoolSpy)

    # Act
    async with main.lifespan(main.app):
        pass

    # Assert
    assert len(pool_calls) == 1
    kwargs = pool_calls[0]
    assert kwargs["quorum"] == 0
    assert kwargs["discovery"] is sentinel_discovery
    assert kwargs["credentials"] == "CREDS-SENTINEL"
    # The priority/leaky-bucket balancer must be wired in (issue #45). This
    # file exists to pin pool kwargs against silent regressions, so the
    # load balancer belongs here alongside quorum/discovery/credentials.
    from cfdb.workflows.loadbalancer import PriorityLoadBalancer

    assert isinstance(kwargs["loadbalancer"], PriorityLoadBalancer)
    # The durable retry scheduler must be started inside the pool context
    # (issue #45) so it inherits wool's dispatch contextvars; pin it here
    # alongside the other load-bearing lifespan wiring.
    fake_executor.start_scheduler.assert_called_once()


def test_worker_pool_signature_should_accept_quorum():
    """Test that ``wool.WorkerPool`` actually exposes a ``quorum`` parameter.

    Given:
        The installed ``wool.WorkerPool``.
    When:
        Its constructor signature is introspected.
    Then:
        It declares a ``quorum`` parameter — a guard so a wool upgrade
        that renames or drops the kwarg fails here loudly rather than the
        lifespan silently passing an unknown keyword.
    """
    # Act
    params = inspect.signature(wool.WorkerPool).parameters

    # Assert
    assert "quorum" in params


@pytest.mark.asyncio
async def test_build_discovery_should_yield_ecs_discovery_un_entered(
    mocker, tmp_path
):
    """Test that the ECS profile yields the discovery un-entered.

    Given:
        An ECS ``WorkflowProfile`` (so ``_build_discovery`` takes its
        ``EcsDiscovery`` branch), with ``build_ecs_client`` stubbed so no
        real boto3 client is constructed.
    When:
        ``_build_discovery`` is driven as an async context manager and the
        yielded discovery is inspected.
    Then:
        The yielded ``EcsDiscovery`` is NOT yet entered — its background
        poller has not started (``_poll_task is None``). wool's
        ``WorkerProxy.__wool_reduce__`` rejects a *context-manager*
        discovery instance, and ``wool.WorkerPool.__aenter__`` enters the
        discovery itself; so ``_build_discovery`` MUST hand it over
        un-entered or the pool would double-enter it and trip
        ``EcsDiscovery``'s re-entry guard (the original Bug 2 startup
        crash).
    """
    # Arrange
    from cfdb.api import profile as profile_mod
    from cfdb.workflows.discovery import EcsDiscovery

    mocker.patch(
        "cfdb.workflows.discovery.build_ecs_client",
        return_value=object(),
    )
    ecs = profile_mod._EcsConfig(
        cluster="cfdb-cluster",
        task_definition="cfdb-worker",
        task_family="cfdb-worker",
        subnets=("subnet-1",),
        security_groups=(),
        assign_public_ip="DISABLED",
    )
    profile = profile_mod.WorkflowProfile(
        kind="ecs",
        cache_root=tmp_path / "cache",
        workdir_root=tmp_path / "jobs",
        ecs=ecs,
    )

    # Act & assert
    async with main._build_discovery(profile) as discovery:
        assert isinstance(discovery, EcsDiscovery)
        # Un-entered: __aenter__ would have started the poll loop.
        assert discovery._poll_task is None
