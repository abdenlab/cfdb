"""Tests for the lifespan wiring of the matrix tile service (issue #82)."""

from __future__ import annotations

import contextlib

import pytest

from cfdb import api
from cfdb.api import main
from cfdb.api import profile as profile_mod


@pytest.fixture()
def stub_lifespan(request, mocker, tmp_path):
    """Drive the enabled-workflow lifespan with everything else stubbed out.

    Indirectly parametrizable over the ``WorkflowProfile`` kind —
    ``"local"`` (the default) or ``"s3-cached"`` — built through the real
    ``from_env`` so the profile shape matches what a deployment gets.
    Returns the profile in use so a test can assert on paths derived
    from it.
    """
    from mongomock_motor import AsyncMongoMockClient

    kind = getattr(request, "param", "local")
    mocker.patch.object(api, "SYNC_DATA_DIR", str(tmp_path))
    mocker.patch.object(api, "ECS_CLUSTER", None)
    mocker.patch.object(
        api,
        "WORKFLOW_S3_BUCKET",
        "cfdb-test-artifacts" if kind == "s3-cached" else None,
    )
    profile = profile_mod.WorkflowProfile.from_env()
    assert profile is not None and profile.kind == kind

    @contextlib.asynccontextmanager
    async def _fake_build_discovery(_profile):
        yield object()

    class _PoolStub:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return None

    fake_executor = mocker.MagicMock()
    fake_executor.drain = mocker.AsyncMock(return_value=0)

    mocker.patch.object(
        main, "create_mongodb_client", return_value=AsyncMongoMockClient()
    )
    mocker.patch.object(main.WorkflowProfile, "from_env", return_value=profile)
    mocker.patch.object(
        main, "_build_cache", new=mocker.AsyncMock(return_value=mocker.MagicMock())
    )
    mocker.patch.object(main, "_build_provisioner", return_value=None)
    mocker.patch.object(main, "_build_discovery", new=_fake_build_discovery)
    mocker.patch.object(main, "WoolExecutor", return_value=fake_executor)
    mocker.patch.object(main.wool, "WorkerPool", _PoolStub)
    return profile


@pytest.mark.asyncio
async def test_lifespan_should_build_the_tile_service_when_clodius_is_present(
    stub_lifespan,
):
    """Test that the tile service is wired on the enabled-workflow path.

    Given:
        The workflow subsystem enabled and clodius installed.
    When:
        The app lifespan starts up.
    Then:
        ``api.tileset_service`` should be populated, so the tile routes
        serve rather than answering 501.
    """
    # Arrange
    pytest.importorskip("clodius")

    # Act
    async with main.lifespan(main.app):
        service = api.tileset_service

        # Assert
        assert service is not None


@pytest.mark.asyncio
async def test_lifespan_should_register_the_matrix_processor(stub_lifespan):
    """Test that contact maps become preparable.

    Given:
        The workflow subsystem enabled.
    When:
        The app lifespan starts up.
    Then:
        The registry should match an HDF5 contact map. Without this
        registration the preparation channel could never build an
        artifact, and every tile request would 404 forever.
    """
    # Arrange
    from cfdb.workflows.processors.matrix import MatrixTilesetProcessor

    doc = {"file_format": {"name": "HDF5"}, "filename": "sample.mcool"}

    # Act
    async with main.lifespan(main.app):
        processor = api.processor_registry.lookup_for(doc)

        # Assert
        assert isinstance(processor, MatrixTilesetProcessor)


@pytest.mark.asyncio
async def test_lifespan_should_close_and_clear_the_tile_service_on_teardown(
    stub_lifespan,
):
    """Test that teardown releases the service's resources.

    Given:
        A lifespan that started the tile service.
    When:
        It tears down.
    Then:
        The service should be closed and the global nulled. Nulling alone
        would leak an open h5py handle per pooled tileset and a live
        thread pool into the next app instantiation — the exact class of
        leak ``_reset_api_globals`` exists to prevent.
    """
    # Arrange
    pytest.importorskip("clodius")
    closed: list[bool] = []

    # Act
    async with main.lifespan(main.app):
        service = api.tileset_service
        original = service.aclose

        async def _tracking_close():
            closed.append(True)
            await original()

        service.aclose = _tracking_close

    # Assert
    assert closed == [True]
    assert api.tileset_service is None


@pytest.mark.asyncio
async def test_lifespan_should_leave_the_service_unset_without_clodius(
    stub_lifespan, mocker
):
    """Test the degraded path for images built without the tile backend.

    Given:
        The workflow subsystem enabled but no clodius — the state of
        every image built today, since the fork is private and the
        Dockerfiles cannot fetch it.
    When:
        The app lifespan starts up.
    Then:
        Startup should succeed with the service unset, so the tile routes
        answer 501 instead of the whole application failing to boot.
    """
    # Arrange
    mocker.patch.object(main.tileset_backend, "is_available", return_value=False)

    # Act
    async with main.lifespan(main.app):
        # Assert
        assert api.tileset_service is None


@pytest.mark.asyncio
async def test_lifespan_should_leave_the_service_unset_without_a_profile(mocker):
    """Test that a disabled workflow subsystem means no tile service.

    Given:
        No ``WorkflowProfile`` — SYNC_DATA_DIR unset, so no artifact
        cache is built.
    When:
        The app lifespan starts up.
    Then:
        ``api.tileset_service`` should stay None. There would be no cache
        for it to read artifacts from, and the tile routes report that as
        503 rather than as a permanently empty dataset.
    """
    # Arrange
    from mongomock_motor import AsyncMongoMockClient

    mocker.patch.object(
        main, "create_mongodb_client", return_value=AsyncMongoMockClient()
    )
    mocker.patch.object(main.WorkflowProfile, "from_env", return_value=None)

    # Act
    async with main.lifespan(main.app):
        # Assert
        assert api.tileset_service is None


@pytest.mark.asyncio
async def test_lifespan_should_propagate_config_to_the_tile_service(
    stub_lifespan, mocker
):
    """Test that the lifespan hands the service builder the configured knobs.

    Given:
        The workflow subsystem enabled and clodius installed, with the
        tile-service builder spied at the lifespan boundary.
    When:
        The app lifespan starts up.
    Then:
        It should call the builder exactly once with a store root of
        ``tilesets`` beside the profile's cache root and the five
        module-level knob values — the only observable pin for the
        ``CFDB_TILESET_*`` / ``CFDB_TILE_*`` env knobs.
    """
    # Arrange
    pytest.importorskip("clodius")
    spy = mocker.spy(main, "build_tileset_service")

    # Act
    async with main.lifespan(main.app):
        pass

    # Assert
    spy.assert_called_once()
    kwargs = spy.call_args.kwargs
    assert kwargs["root"] == stub_lifespan.cache_root.parent / "tilesets"
    assert kwargs["disk_cache_bytes"] == api.TILESET_DISK_CACHE_BYTES
    assert kwargs["open_max"] == api.TILESET_OPEN_MAX
    assert kwargs["tile_cache_bytes"] == api.TILE_CACHE_BYTES
    assert kwargs["threads"] == api.TILE_THREADS
    assert kwargs["hydrate_timeout_s"] == api.TILESET_HYDRATE_TIMEOUT_S


@pytest.mark.asyncio
async def test_lifespan_should_null_all_globals_when_service_close_raises(
    stub_lifespan,
):
    """Test that a failing service close cannot poison the teardown chain.

    Given:
        A started lifespan whose tile service's ``aclose`` raises at
        teardown.
    When:
        The lifespan exits.
    Then:
        It should swallow the failure and still null ``tileset_service``
        and every other ``cfdb.api`` global, so one bad h5py handle
        cannot leak the executor, cache, registry, or wool context into
        the next app instantiation.
    """
    # Arrange
    pytest.importorskip("clodius")
    original_close = None

    # Act
    async with main.lifespan(main.app):
        service = api.tileset_service
        original_close = service.aclose

        async def _exploding_close():
            raise RuntimeError("h5py refused to die")

        service.aclose = _exploding_close

    # Assert
    assert api.tileset_service is None
    assert api.executor is None
    assert api.cache is None
    assert api.processor_registry is None
    assert api.wool_context is None

    # Release the real handles the exploding stub skipped, so this test
    # does not itself leak a thread pool into the next one.
    await original_close()


@pytest.mark.parametrize("stub_lifespan", ["s3-cached"], indirect=True)
@pytest.mark.asyncio
async def test_lifespan_should_build_the_tile_service_when_the_cache_is_s3(
    stub_lifespan,
):
    """Test the S3 cell of the profile matrix.

    Given:
        The workflow subsystem enabled with an ``s3-cached`` profile and
        clodius installed.
    When:
        The app lifespan starts up.
    Then:
        ``api.tileset_service`` should be wired exactly as on the local
        profile — the S3 profile changes the artifact cache, not whether
        tiles are served.
    """
    # Arrange
    pytest.importorskip("clodius")
    assert stub_lifespan.kind == "s3-cached"

    # Act
    async with main.lifespan(main.app):
        service = api.tileset_service

        # Assert
        assert service is not None


@pytest.mark.parametrize("stub_lifespan", ["local", "s3-cached"], indirect=True)
@pytest.mark.asyncio
async def test_lifespan_should_wire_the_cache_provider_to_late_bind(
    stub_lifespan, mocker
):
    """Test that the tile service reads ``api.cache`` at call time.

    Given:
        A started lifespan whose ``api.cache`` is swapped for a fake
        cache after startup, on both the local and s3-cached profiles.
    When:
        A tileset_info request resolves the service's cache provider.
    Then:
        It should consult the swapped object rather than the cache
        captured at boot — the documented late-binding contract, so a
        lifespan (or test) that replaces the cache never leaves the
        service pointed at a stale backend.
    """
    # Arrange
    pytest.importorskip("clodius")
    from cfdb.tilesets.errors import TilesetNotReady

    doc = {
        "dcc": {"dcc_abbreviation": "4DN"},
        "local_id": "abc123",
        "md5": "d41d8cd98f00b204e9800998ecf8427e",
        "filename": "sample.mcool",
        "file_format": {"name": "HDF5"},
    }
    swapped = mocker.MagicMock()
    swapped.head = mocker.AsyncMock(return_value=None)

    # Act
    async with main.lifespan(main.app):
        api.cache = swapped
        with pytest.raises(TilesetNotReady):
            await api.tileset_service.tileset_info("4dn/abc123", doc)

    # Assert
    swapped.head.assert_awaited_once()
