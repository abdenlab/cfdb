"""Matrix tile serving over the S3 profile, moto-backed and in-process.

The local-profile e2e chain (``test_tiles_e2e.py``) opens the committed
artifact in place, so the hydration path — pull the artifact from S3 onto
the API task's disk before h5py can touch it — is never exercised there.
These tests commit a real mcool through the real processor into a
moto-backed :class:`~cfdb.workflows.cache.S3Cache` and serve tiles through
the store's single-flight LRU, covering the three behaviours only the S3
profile has: hydrate-once-then-serve-warm, the disk-budget refusal, and
the hydration timeout with clean recovery.

In-process rather than through a wool worker, deliberately: a moto mock
cannot cross the subprocess boundary, and nothing S3-specific lives on the
worker side of it.
"""

from __future__ import annotations

import asyncio
import base64

import numpy as np
import pytest
import pytest_asyncio

pytest.importorskip("cooler")
pytest.importorskip("moto")

import boto3
from fastapi import HTTPException
from moto import mock_aws

from cfdb import api
from cfdb.api.routers.tiles import tiles, tileset_info
from cfdb.services import locks
from cfdb.tilesets.service import TilesetService
from cfdb.tilesets.store import LocalTilesetStore
from cfdb.workflows.cache import CacheBackend, S3Cache
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors import matrix as matrix_module
from cfdb.workflows.processors.matrix import MatrixTilesetProcessor
from tests.fixtures.coolers import build_mcool
from tests.test_workflows import FIXTURE_MD5

pytestmark = pytest.mark.integration

_BUCKET = "cfdb-tiles-test-cache"

UID = "4dn/4DNFIABC123"


def _file_doc(filename: str) -> dict:
    """Return a projected 4DN document for a contact map."""
    return {
        "submission": "4dn",
        "local_id": "4DNFIABC123",
        "md5": FIXTURE_MD5,
        "filename": filename,
        "file_format": {"name": "HDF5"},
        "genome_assembly": "GRCh38",
        "dcc": {"dcc_abbreviation": "4DN_DCIC"},
    }


def _decode(payload: dict) -> np.ndarray:
    """Decode a dense tile payload the way a HiGlass client would."""
    flat = np.frombuffer(base64.b64decode(payload["dense"]), dtype=payload["dtype"])
    side = int(round(len(flat) ** 0.5))
    return flat.reshape(side, side)


class DelegatingS3Cache(CacheBackend):
    """Delegate every call to a real S3Cache, observing ``get``.

    The store branches only on ``isinstance(cache, LocalFsCache)``, so a
    wrapper keeps the hydration path honest while letting a test count S3
    GETs — via the delegation seam rather than moto internals — and,
    when ``stall`` is set, hold the byte stream open past the service's
    hydration timeout.
    """

    def __init__(self, inner: S3Cache) -> None:
        self.inner = inner
        self.get_calls: list[str] = []
        #: When set to an un-set event, ``get`` streams nothing until the
        #: event fires — the shape of a download that outlives its budget.
        self.stall: asyncio.Event | None = None

    async def head(self, key: str):
        return await self.inner.head(key)

    def get(self, key: str, byte_range=None):
        self.get_calls.append(key)
        inner, stall = self.inner, self.stall

        async def _stream():
            if stall is not None:
                await stall.wait()
            async for chunk in inner.get(key, byte_range):
                yield chunk

        return _stream()

    async def put(self, key: str, source_path):
        return await self.inner.put(key, source_path)

    async def delete(self, key: str) -> bool:
        return await self.inner.delete(key)


@pytest.fixture()
def s3_client():
    """Return a moto-backed boto3 S3 client with one created bucket."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=_BUCKET)
        yield client


@pytest.fixture()
def s3_cache(s3_client) -> S3Cache:
    """Return an S3Cache wired up to the moto-backed client."""
    return S3Cache(bucket=_BUCKET, client=s3_client)


@pytest_asyncio.fixture()
async def committed(s3_cache, mock_db, mocker, tmp_path):
    """Commit a real mcool through the processor into the S3 cache.

    Returns the projected file document, the tileset cache key, and the
    committed artifact's CacheEntry (whose size the store budgets by).
    """
    source = build_mcool(tmp_path / "upstream.mcool")
    doc = _file_doc("sample.mcool")
    mock_db.file.docs = [doc]

    async def _download(_file_meta, dest):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(source.read_bytes())
        return dest

    mocker.patch.object(matrix_module, "download_source", side_effect=_download)

    processor = MatrixTilesetProcessor()
    async for _event in processor.run(doc, tmp_path / "workdir", s3_cache):
        pass

    key = processor.cache_key_for(doc, ArtifactKind.TILESET)
    entry = await s3_cache.head(key)
    assert entry is not None
    return doc, key, entry


@pytest_asyncio.fixture()
async def serve(mocker, tmp_path):
    """Return a builder wiring a TilesetService over a given cache.

    The store is built by hand rather than through ``build_service`` so
    tests can assert against its public residency accounting and pick a
    per-test disk budget; teardown closes every service that was built.
    """
    mocker.patch.object(locks, "wait_for_cutover", return_value=None)
    services: list[TilesetService] = []
    store_root = tmp_path / "tilesets"

    def _build(cache, *, max_bytes: int = 10**9, hydrate_timeout_s: float = 60.0):
        store = LocalTilesetStore(store_root, max_bytes=max_bytes)
        service = TilesetService(
            store,
            open_max=4,
            tile_cache_bytes=10**7,
            threads=2,
            hydrate_timeout_s=hydrate_timeout_s,
            cache_provider=lambda: cache,
        )
        mocker.patch.object(api, "cache", cache)
        mocker.patch.object(api, "tileset_service", service)
        services.append(service)
        return service, store, store_root

    yield _build

    for service in services:
        await service.aclose()


class TestS3ProfileTileChain:
    @pytest.mark.asyncio
    async def test_should_hydrate_once_and_serve_warm_without_a_second_get(
        self, committed, s3_cache, serve
    ):
        """Test the cold-to-warm tile chain over a moto-backed S3 cache.

        Given:
            A real mcool committed by the processor into a moto-backed
            S3Cache, and a tileset store with an empty root.
        When:
            /tileset_info and /tiles are served cold, then the same tile
            is requested again warm.
        Then:
            It should hydrate exactly once — one file under the store
            root, resident_bytes equal to the artifact's size, and no
            .part residue — with the decoded tile matching a direct
            cooler read of the hydrated copy, and the warm request
            should make no second S3 GET.
        """
        # Arrange
        import cooler

        doc, key, entry = committed
        counting = DelegatingS3Cache(s3_cache)
        service, store, store_root = serve(counting)

        # Act
        info = (await tileset_info(d=[UID]))[UID]
        cold = (await tiles(d=[f"{UID}.0.0.0"]))[f"{UID}.0.0.0"]
        gets_after_cold = list(counting.get_calls)
        warm = (await tiles(d=[f"{UID}.0.0.0"]))[f"{UID}.0.0.0"]

        # Assert
        assert info["datatype"] == "matrix"
        assert info["resolutions"] == [1, 2, 4]

        hydrated = sorted(store_root.iterdir())
        assert [p.suffix for p in hydrated] == [".tileset"]
        assert not list(store_root.glob("*.part"))
        assert store.resident_bytes == entry.size
        assert hydrated[0].stat().st_size == entry.size

        assert gets_after_cold == [key]

        block = _decode(cold)
        assert block.shape == (256, 256)
        clr = cooler.Cooler(f"{hydrated[0]}::/resolutions/4")
        expected = clr.matrix(balance=False).fetch("c1")[:8, :8]
        np.testing.assert_allclose(block[:8, :8], expected.astype(np.float32))

        assert counting.get_calls == [key]
        assert warm == cold

    @pytest.mark.asyncio
    async def test_should_answer_503_when_artifact_exceeds_disk_budget(
        self, committed, s3_cache, serve
    ):
        """Test the disk-budget refusal against a real committed artifact.

        Given:
            A store whose max_bytes sits below the committed artifact's
            size, with nothing injected — the store learns the size from
            cache.head itself.
        When:
            /tileset_info is requested.
        Then:
            It should answer 503 with a Retry-After header, not 404 —
            the budget is an operator condition, not "dataset does not
            exist".
        """
        # Arrange
        doc, key, entry = committed
        serve(s3_cache, max_bytes=entry.size - 1)

        # Act & assert
        with pytest.raises(HTTPException) as excinfo:
            await tileset_info(d=[UID])
        assert excinfo.value.status_code == 503
        assert excinfo.value.headers is not None
        assert "Retry-After" in excinfo.value.headers

    @pytest.mark.asyncio
    async def test_should_leave_no_residue_and_recover_after_hydration_timeout(
        self, committed, s3_cache, serve
    ):
        """Test that a timed-out hydration cleans up and does not poison.

        Given:
            A delegating cache whose byte stream stalls past the
            service's hydrate_timeout_s, then has the stall removed.
        When:
            /tileset_info is requested, and then requested again.
        Then:
            It should first answer 503 with Retry-After leaving no .part
            file or store residue — the timeout's cancellation propagates
            into hydration cleanup — and the retry should succeed,
            proving the timed-out key was not poisoned.
        """
        # Arrange
        doc, key, entry = committed
        stalled = DelegatingS3Cache(s3_cache)
        stalled.stall = asyncio.Event()
        service, store, store_root = serve(stalled, hydrate_timeout_s=0.2)

        # Act & assert — the stalled hydration times out cleanly
        with pytest.raises(HTTPException) as excinfo:
            await tileset_info(d=[UID])
        assert excinfo.value.status_code == 503
        assert excinfo.value.headers is not None
        assert "Retry-After" in excinfo.value.headers
        assert list(store_root.iterdir()) == []
        assert store.resident_bytes == 0

        # Act — remove the delay and retry the same key
        stalled.stall.set()
        info = (await tileset_info(d=[UID]))[UID]

        # Assert — the retry hydrated and served
        assert info["resolutions"] == [1, 2, 4]
        assert store.resident_bytes == entry.size
        assert stalled.get_calls == [key, key]
