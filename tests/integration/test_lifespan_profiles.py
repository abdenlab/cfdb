"""Lifespan profile matrix: real ``_build_cache`` composition per cell.

Every other tile test hands the lifespan a pre-stubbed cache. These tests
run the real profile resolution instead — ``WorkflowProfile.from_env``
over patched env globals, the real ``_build_cache`` (moto-backed for the
S3 cell), and the real ``_build_tileset_service`` — so what is pinned is
the *composition* the deployment actually gets: which cache class each
profile wires, where the tileset store lands relative to ``SYNC_DATA_DIR``,
and that a request served after boot reads the cache the lifespan
assigned rather than a stale capture.

Only the boundaries the guide permits are stubbed: MongoDB (mongomock),
the wool discovery/pool/executor stack (no worker fleet exists here), and
— for the ABSENT rows — ``cfdb.tilesets.backend.is_available``, the one
axis that cannot vary for real within a single installed environment.
"""

from __future__ import annotations

import contextlib
from contextlib import asynccontextmanager
from enum import Enum
from pathlib import Path

import pytest

pytest.importorskip("cooler")
pytest.importorskip("clodius.tiles_v2")

import boto3
import httpx
from mongomock_motor import AsyncMongoMockClient
from moto import mock_aws

from cfdb import api
from cfdb.api import main
from cfdb.tilesets import backend as tiles_backend
from cfdb.workflows.cache import LocalFsCache, S3Cache
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.matrix import MatrixTilesetProcessor
from tests.fixtures.coolers import build_mcool
from tests.test_workflows import FIXTURE_MD5

pytestmark = pytest.mark.integration


class Profile(Enum):
    """Workflow profile axis of the lifespan matrix."""

    DISABLED = "disabled"
    LOCAL = "local"
    S3_CACHED = "s3-cached"


class TileBackend(Enum):
    """Clodius availability axis of the lifespan matrix."""

    PRESENT = "present"
    ABSENT = "absent"


#: The full factorial — six cells, kept as a flat parametrize per the
#: plan rather than a new Scenario dimension (the S3 axis cannot cross
#: the wool boundary, so it could never be swept anyway).
CELLS = [
    pytest.param(profile, backend, id=f"{profile.value}-{backend.value}")
    for profile in Profile
    for backend in TileBackend
]

_BUCKET = "cfdb-lifespan-profiles"
_LOCAL_ID = "4DNFILP012AB"
UID = f"4dn/{_LOCAL_ID}"


def _file_doc() -> dict:
    """Return a projected 4DN contact-map document for the served cells."""
    return {
        "submission": "4dn",
        "local_id": _LOCAL_ID,
        "md5": FIXTURE_MD5,
        "filename": "sample.mcool",
        "file_format": {"name": "HDF5"},
        "genome_assembly": "GRCh38",
        "dcc": {"dcc_abbreviation": "4DN_DCIC"},
        "data_access_level": "public",
    }


def _tileset_key() -> str:
    """The cache key the serving chain derives for ``_file_doc()``."""
    return MatrixTilesetProcessor().cache_key_for(_file_doc(), ArtifactKind.TILESET)


@asynccontextmanager
async def _booted(
    profile: Profile,
    backend_state: TileBackend,
    mocker,
    tmp_path: Path,
    *,
    seed: Path | None = None,
):
    """Enter the real lifespan for one (profile, backend) cell.

    Patches the env-derived ``cfdb.api`` globals so the lifespan's own
    ``WorkflowProfile.from_env`` call resolves the requested profile, and
    lets ``_build_cache`` run for real — against moto's in-process S3 for
    the s3-cached cells. ``seed`` (a local mcool path) is committed to the
    cell's real cache backend *before* boot under the tileset key for
    ``_file_doc()``.

    Yields the ``SYNC_DATA_DIR`` root (``None`` for the disabled cell).
    """
    if backend_state is TileBackend.ABSENT:
        mocker.patch.object(tiles_backend, "is_available", return_value=False)

    sync_root: Path | None = None
    mocker.patch.object(api, "ECS_CLUSTER", None)
    mocker.patch.object(api, "AWS_ENDPOINT_URL", None)
    mocker.patch.object(api, "AWS_REGION", "us-east-1")
    mocker.patch.object(api, "WORKFLOW_S3_PREFIX", "")
    if profile is Profile.DISABLED:
        mocker.patch.object(api, "SYNC_DATA_DIR", None)
        mocker.patch.object(api, "WORKFLOW_S3_BUCKET", None)
    else:
        sync_root = tmp_path / "sync"
        mocker.patch.object(api, "SYNC_DATA_DIR", str(sync_root))
        mocker.patch.object(
            api,
            "WORKFLOW_S3_BUCKET",
            _BUCKET if profile is Profile.S3_CACHED else None,
        )

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
    mocker.patch.object(main, "_build_discovery", _fake_build_discovery)
    mocker.patch.object(main, "WoolExecutor", return_value=fake_executor)
    mocker.patch.object(main.wool, "WorkerPool", _PoolStub)

    with contextlib.ExitStack() as stack:
        if profile is Profile.S3_CACHED:
            stack.enter_context(mock_aws())
            boto3.client("s3", region_name="us-east-1").create_bucket(
                Bucket=_BUCKET
            )
        if seed is not None and profile is not Profile.DISABLED:
            assert sync_root is not None
            if profile is Profile.S3_CACHED:
                seed_cache: LocalFsCache | S3Cache = S3Cache(
                    bucket=_BUCKET,
                    client=boto3.client("s3", region_name="us-east-1"),
                )
            else:
                seed_cache = LocalFsCache(sync_root / "cache")
            await seed_cache.put(_tileset_key(), seed)
        async with main.lifespan(main.app):
            yield sync_root


class TestLifespanProfileMatrix:
    """The six (profile x backend) cells of the lifespan composition."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("profile, backend_state", CELLS)
    async def test_lifespan_should_compose_the_cell_profile(
        self, profile, backend_state, mocker, tmp_path
    ):
        """Test the cache/service composition of one lifespan cell.

        Given:
            Env globals resolving one workflow profile (disabled, local,
            or s3-cached — the S3 cell against a real moto bucket) and a
            tile backend that is present or patched absent.
        When:
            The real app lifespan is entered and exited, with only the
            Mongo and wool boundaries stubbed.
        Then:
            It should wire the profile's real cache class (LocalFsCache
            for local, S3Cache for s3-cached, none when disabled), build
            the tile service only when a profile and the backend are both
            present — rooting its store at SYNC_DATA_DIR/tilesets, the
            sibling of cache/ — and on exit leave the service closed and
            the global nulled.
        """
        # Arrange
        expect_cache_type = {
            Profile.LOCAL: LocalFsCache,
            Profile.S3_CACHED: S3Cache,
        }.get(profile)
        expect_service = (
            profile is not Profile.DISABLED
            and backend_state is TileBackend.PRESENT
        )
        build_spy = mocker.spy(main, "build_tileset_service")
        closed: list[bool] = []

        # Act
        async with _booted(profile, backend_state, mocker, tmp_path) as sync_root:
            wired_service = api.tileset_service
            wired_cache = api.cache
            if wired_service is not None:
                original_aclose = wired_service.aclose

                async def _tracking_close():
                    closed.append(True)
                    await original_aclose()

                wired_service.aclose = _tracking_close

        # Assert
        if expect_cache_type is None:
            assert wired_cache is None
        else:
            assert type(wired_cache) is expect_cache_type
        if expect_service:
            assert wired_service is not None
            assert build_spy.call_args.kwargs["root"] == sync_root / "tilesets"
            assert closed == [True]
        else:
            assert wired_service is None
            build_spy.assert_not_called()
        assert api.tileset_service is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("profile, backend_state", CELLS)
    async def test_tileset_info_should_serve_through_the_booted_profile(
        self, profile, backend_state, mocker, tmp_path
    ):
        """Test serving /tileset_info through a really-booted lifespan.

        Given:
            One lifespan cell booted over the real cache composition,
            with a real mcool artifact pre-committed to the enabled
            cells' backend (local disk or moto S3) and its document
            seeded in the lifespan's database.
        When:
            GET /tileset_info/?d=<uid> is issued through the real router
            over ASGI, after boot.
        Then:
            The two fully-present cells should answer 200 with the
            file's real resolutions ladder — proving ``cache_provider``
            resolves the cache the lifespan actually assigned, the one
            place the stale-capture hazard is observable — while the
            backend-absent rows answer 501 and the disabled-with-backend
            row answers 503 with Retry-After.
        """
        # Arrange
        if profile is Profile.DISABLED:
            # The backend check outranks the missing-subsystem check in
            # ``_require_service``, so disabled-without-clodius is a 501.
            expected_status = (
                501 if backend_state is TileBackend.ABSENT else 503
            )
        elif backend_state is TileBackend.ABSENT:
            expected_status = 501
        else:
            expected_status = 200
        seed = (
            build_mcool(tmp_path / "upstream.mcool")
            if profile is not Profile.DISABLED
            else None
        )

        # Act
        async with _booted(
            profile, backend_state, mocker, tmp_path, seed=seed
        ):
            await api.db.files.insert_one(dict(_file_doc()))
            transport = httpx.ASGITransport(app=main.app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:
                response = await client.get(
                    "/tileset_info/", params=[("d", UID)]
                )

        # Assert
        assert response.status_code == expected_status
        if expected_status == 200:
            info = response.json()[UID]
            assert info["resolutions"] == [1, 2, 4]
            assert info["uuid"] == UID
        elif expected_status == 503:
            assert response.headers["retry-after"] == "30"
