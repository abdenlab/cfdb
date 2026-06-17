"""Tests for the lifespan's discovery builder."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from cfdb.api import main


@pytest.mark.asyncio
async def test_build_discovery_should_yield_ecs_discovery_unentered(mocker):
    """Test the ECS profile yields an un-entered EcsDiscovery.

    Given:
        An ECS profile and a stubbed EcsDiscovery.
    When:
        ``_build_discovery`` is entered.
    Then:
        It should construct and yield the EcsDiscovery without entering
        it, leaving the enter/exit to wool.WorkerPool — otherwise the
        discovery would be entered twice and trip its re-entry guard.
    """
    # Arrange
    ecs_instance = mocker.MagicMock()
    ecs_instance.__aenter__ = mocker.AsyncMock(return_value=ecs_instance)
    ecs_instance.__aexit__ = mocker.AsyncMock(return_value=None)
    ecs_cls = mocker.patch.object(main, "EcsDiscovery", return_value=ecs_instance)
    profile = SimpleNamespace(
        ecs=SimpleNamespace(cluster="cfdb-cluster", task_family="cfdb-worker"),
        aws_endpoint_url=None,
        aws_region="us-east-2",
    )

    # Act
    async with main._build_discovery(profile) as discovery:
        entered_during = ecs_instance.__aenter__.await_count

    # Assert
    assert discovery is ecs_instance
    ecs_cls.assert_called_once()
    assert entered_during == 0
    ecs_instance.__aenter__.assert_not_awaited()
    ecs_instance.__aexit__.assert_not_awaited()


@pytest.mark.asyncio
async def test_build_discovery_should_yield_lan_discovery_unentered(mocker):
    """Test the local profile yields an un-entered LanDiscovery.

    Given:
        A non-ECS profile and a stubbed LanDiscovery.
    When:
        ``_build_discovery`` is entered.
    Then:
        It should yield the LanDiscovery instance without entering it,
        symmetric with the ECS branch.
    """
    # Arrange
    lan_instance = mocker.MagicMock()
    lan_cls = mocker.patch.object(main, "LanDiscovery", return_value=lan_instance)
    profile = SimpleNamespace(ecs=None, aws_endpoint_url=None, aws_region="us-east-2")

    # Act
    async with main._build_discovery(profile) as discovery:
        pass

    # Assert
    assert discovery is lan_instance
    lan_cls.assert_called_once()
