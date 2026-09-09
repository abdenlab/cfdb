"""Tests for the processor registry the lifespan wires at startup.

``test_lifespan_workerpool`` replaces ``default_registry`` with a
``MagicMock``, so the ``register(BamIndexProcessor())`` and
``register(TabixIntervalProcessor())`` calls in ``cfdb.api.main`` land on
a mock and their real effect is never exercised. That mattered little
until issue #109 gave ``ProcessorRegistry.register`` a failure mode:
registering two processors that share a ``processor_id`` now raises. The
shipped wiring is the guard's only production caller, so these tests
drive the lifespan with ``default_registry`` left real.
"""

from __future__ import annotations

import contextlib

import pytest

from cfdb import api
from cfdb.api import main
from cfdb.api import profile as profile_mod
from cfdb.workflows.processors.bam import BamIndexProcessor
from cfdb.workflows.processors.passthrough import PassthroughProcessor
from cfdb.workflows.processors.tabix import TabixIntervalProcessor


@contextlib.asynccontextmanager
async def _stubbed_lifespan(mocker, tmp_path):
    """Yield the app lifespan with everything but the registry stubbed."""
    from mongomock_motor import AsyncMongoMockClient

    profile = profile_mod.WorkflowProfile(
        kind="local",
        cache_root=tmp_path / "cache",
        workdir_root=tmp_path / "jobs",
    )

    class _PoolStub:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return None

    @contextlib.asynccontextmanager
    async def _fake_build_discovery(_profile):
        yield object()

    executor = mocker.MagicMock()
    executor.drain = mocker.AsyncMock(return_value=0)

    mocker.patch.object(
        main, "create_mongodb_client", return_value=AsyncMongoMockClient()
    )
    mocker.patch.object(main.WorkflowProfile, "from_env", return_value=profile)
    mocker.patch.object(
        main, "_build_cache", new=mocker.AsyncMock(return_value=mocker.MagicMock())
    )
    mocker.patch.object(main, "_build_provisioner", return_value=None)
    mocker.patch.object(main, "_build_discovery", new=_fake_build_discovery)
    mocker.patch.object(main, "build_worker_credentials", return_value="CREDS-SENTINEL")
    mocker.patch.object(main, "WoolExecutor", return_value=executor)
    mocker.patch.object(main.wool, "WorkerPool", _PoolStub)

    async with main.lifespan(main.app):
        yield


@pytest.mark.asyncio
async def test_lifespan_should_wire_every_shipped_processor(mocker, tmp_path):
    """Test that startup builds a registry resolving all shipped formats.

    Given:
        The workflow subsystem enabled and ``default_registry`` left
        unpatched, so the real registrations run.
    When:
        The app lifespan starts up.
    Then:
        ``api.processor_registry`` should resolve BAM, BED, and bigWig to
        their processors — proving the three shipped identities are
        distinct enough for the duplicate guard to admit them all, which
        no other test exercises.
    """
    # Act
    async with _stubbed_lifespan(mocker, tmp_path):
        registry = api.processor_registry

        # Assert
        assert isinstance(
            registry.lookup_for({"file_format": {"name": "BAM"}}), BamIndexProcessor
        )
        assert isinstance(
            registry.lookup_for({"file_format": {"name": "BED"}}),
            TabixIntervalProcessor,
        )
        assert isinstance(
            registry.lookup_for({"file_format": {"name": "bigWig"}}),
            PassthroughProcessor,
        )


@pytest.mark.asyncio
async def test_lifespan_should_build_a_fresh_registry_on_every_startup(
    mocker, tmp_path
):
    """Test that a second startup does not re-register onto the first.

    Given:
        The same enabled lifespan, with ``default_registry`` unpatched.
    When:
        The lifespan is entered and exited twice in sequence, as a
        reload or a worker restart does.
    Then:
        Neither run should raise and the second registry should still be
        well-formed. Startup assigns a fresh registry immediately before
        registering, and that one assignment is the only reason the
        duplicate guard cannot turn a benign restart into a boot
        crash-loop.
    """
    # Arrange
    async with _stubbed_lifespan(mocker, tmp_path):
        first = api.processor_registry

    # Act
    async with _stubbed_lifespan(mocker, tmp_path):
        second = api.processor_registry

        # Assert
        assert second is not first
        assert isinstance(
            second.lookup_for({"file_format": {"name": "BAM"}}), BamIndexProcessor
        )
