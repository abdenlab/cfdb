"""End-to-end matrix tile serving over real coolers.

Runs the whole chain against bytes rather than mocks: build a contact map,
put it through :class:`~cfdb.workflows.processors.matrix.MatrixTilesetProcessor`
into a real cache, then serve ``/tileset_info`` and ``/tiles`` from the
committed artifact and check the tile the client would decode against a
direct cooler read.

Marked ``integration`` because the flat-cooler case runs a real
``cooler.zoomify_cooler``, which is the only place coarsening is exercised
for real.
"""

from __future__ import annotations

import asyncio
import base64

import numpy as np
import pytest
import pytest_asyncio

from cfdb import api
from cfdb.api.routers.tiles import tiles, tileset_info
from cfdb.services import locks
from cfdb.tilesets.service import TilesetService, build_service
from cfdb.tilesets.store import LocalTilesetStore
from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors import matrix as matrix_module
from cfdb.workflows.processors.matrix import MatrixTilesetProcessor
from tests.fixtures.coolers import CANONICAL_CHROMSIZES, build_cool, build_mcool
from tests.fixtures.remote_cache import FakeRemoteCache
from tests.test_workflows import FIXTURE_MD5, FIXTURE_MD5_ALT

# None of the imports above pull in cooler or clodius at import time (the
# tile backend loads lazily), so on a checkout without the tiles extra the
# failure would otherwise land inside the first fixture as an error rather
# than a skip. Guard the whole module instead.
pytest.importorskip("cooler")
pytest.importorskip("clodius.tiles_v2.cooler")

pytestmark = pytest.mark.integration

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
    """Decode a dense tile payload the way a HiGlass client would.

    clodius emits no ``shape`` field, so the side length is recovered as
    the square root of the flat array's length.
    """
    flat = np.frombuffer(base64.b64decode(payload["dense"]), dtype=payload["dtype"])
    side = int(round(len(flat) ** 0.5))
    return flat.reshape(side, side)


@pytest_asyncio.fixture()
async def served(mock_db, mocker, tmp_path):
    """Materialize a contact map and stand up the routes over the result."""
    mocker.patch.object(locks, "wait_for_cutover", return_value=None)
    cache = LocalFsCache(tmp_path / "cache")
    service = build_service(
        root=tmp_path / "tilesets",
        cache_provider=lambda: cache,
        disk_cache_bytes=10**9,
        open_max=4,
        tile_cache_bytes=10**7,
        threads=2,
        hydrate_timeout_s=60,
    )
    mocker.patch.object(api, "cache", cache)
    mocker.patch.object(api, "tileset_service", service)

    async def _run(source, filename):
        doc = _file_doc(filename)
        mock_db.file.docs = [doc]

        async def _download(_file_meta, dest):
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(source.read_bytes())
            return dest

        mocker.patch.object(matrix_module, "download_source", side_effect=_download)

        processor = MatrixTilesetProcessor()
        events = [
            event
            async for event in processor.run(doc, tmp_path / "workdir", cache)
        ]
        return doc, events, cache

    yield _run

    await service.aclose()


@pytest.mark.asyncio
async def test_should_serve_tiles_matching_a_direct_cooler_read(served, tmp_path):
    """Test the full chain from upstream mcool to decoded tile.

    Given:
        A multi-resolution cooler put through the tileset processor into
        a real cache.
    When:
        /tileset_info and /tiles are served from the committed artifact.
    Then:
        The info document should describe the file's real ladder, and the
        decoded tile should match a direct cooler fetch of the same
        region. This is the only test that proves the artifact cfdb
        commits is one clodius can actually serve — every other test
        stops at one end or the other.
    """
    # Arrange
    import cooler

    source = build_mcool(tmp_path / "upstream.mcool")
    doc, events, cache = await served(source, "sample.mcool")

    # Act
    info = (await tileset_info(d=[UID]))[UID]
    payload = (await tiles(d=[f"{UID}.0.0.0"]))[f"{UID}.0.0.0"]
    block = _decode(payload)

    # Assert
    assert info["datatype"] == "matrix"
    assert info["resolutions"] == [1, 2, 4]
    assert info["max_pos"] == [3000, 3000]
    assert info["coordSystem"] == "GRCh38"
    assert block.shape == (256, 256)

    # The coarsest resolution is 4 bp/bin, and tile (0, 0) at zoom 0 covers
    # the first 256 bins of it — so the top-left corner is a direct read.
    key = MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)
    clr = cooler.Cooler(f"{cache.path_for(key)}::/resolutions/4")
    expected = clr.matrix(balance=False).fetch("c1")[:8, :8]
    np.testing.assert_allclose(block[:8, :8], expected.astype(np.float32))


@pytest.mark.asyncio
async def test_should_coarsen_a_flat_cooler_into_a_servable_artifact(
    served, tmp_path
):
    """Test that the 10 flat 4DN coolers become tileable.

    Given:
        A single-resolution ``.cool`` — the shape clodius refuses to serve,
        because it carries no ``resolutions`` group.
    When:
        It is put through the processor and then served.
    Then:
        A real ``cooler.zoomify_cooler`` should have produced a ladder
        whose base-resolution tile decodes to the same values a direct
        cooler read of the source flat file yields. Without this the ten
        flat 4DN files would be classified as contact maps, accepted for
        preparation, and then serve tiles that misrepresent the data.
    """
    # Arrange
    import cooler

    source = build_cool(tmp_path / "upstream.cool", binsize=4)

    # Act
    await served(source, "sample.cool")
    info = (await tileset_info(d=[UID]))[UID]
    # Resolutions are ascending and zoom levels count down from the
    # coarsest, so the base resolution — the one that must reproduce the
    # source verbatim — sits at the deepest zoom.
    base_zoom = len(info["resolutions"]) - 1
    payload = (await tiles(d=[f"{UID}.{base_zoom}.0.0"]))[f"{UID}.{base_zoom}.0.0"]
    block = _decode(payload)

    # Assert
    assert info["resolutions"][0] == 4
    assert len(info["resolutions"]) >= 1
    assert block.shape == (256, 256)

    # Tile (0, 0) at the base zoom covers the first 256 bins of the
    # 4 bp/bin base layer, so chromosome c1 (1000 bp = 250 bins) is a
    # direct read of the *source* flat cooler — value fidelity, not just
    # shape.
    clr = cooler.Cooler(str(source))
    expected = clr.matrix(balance=False).fetch("c1")
    np.testing.assert_allclose(block[:250, :250], expected.astype(block.dtype))


@pytest.mark.asyncio
async def test_should_report_the_genome_the_fixture_actually_declares(
    served, tmp_path
):
    """Test that chromsizes survive the round trip intact.

    Given:
        A contact map over a known three-chromosome genome.
    When:
        /tileset_info is served.
    Then:
        The chromsizes should match the source exactly. HiGlass positions
        every tile against this array, so a reordering or a dropped
        contig misplaces the whole heatmap rather than failing loudly.
    """
    # Arrange
    source = build_mcool(tmp_path / "upstream.mcool")
    await served(source, "sample.mcool")

    # Act
    info = (await tileset_info(d=[UID]))[UID]

    # Assert
    assert [tuple(pair) for pair in info["chromsizes"]] == CANONICAL_CHROMSIZES


class TestTilesOverHttp:
    """The full HTTP stack, which calling the handlers directly skips.

    FastAPI serializes the handler's return value to JSON on the way out,
    so a clodius payload carrying a numpy scalar would 500 here first —
    this is the only crossing of the "clodius payloads are JSON-ready"
    seam. Follows ``tests/test_tiles.py::TestOverHttp`` in neutering the
    lifespan, but patches the *real* service and cache into ``cfdb.api``.
    """

    @pytest.fixture()
    def http_client(self, mock_db, mocker, tmp_path):
        """A TestClient over the real app with a committed artifact behind it."""
        from mongomock_motor import AsyncMongoMockClient
        from starlette.testclient import TestClient

        from cfdb.api import main

        source = build_mcool(tmp_path / "upstream.mcool")
        doc = _file_doc("sample.mcool")
        mock_db.file.docs = [doc]
        cache = LocalFsCache(tmp_path / "cache")

        async def _download(_file_meta, dest):
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(source.read_bytes())
            return dest

        mocker.patch.object(matrix_module, "download_source", side_effect=_download)

        async def _build():
            processor = MatrixTilesetProcessor()
            async for _ in processor.run(doc, tmp_path / "workdir", cache):
                pass

        asyncio.run(_build())

        service = build_service(
            root=tmp_path / "tilesets",
            cache_provider=lambda: cache,
            disk_cache_bytes=10**9,
            open_max=4,
            tile_cache_bytes=10**7,
            threads=2,
            hydrate_timeout_s=60,
        )
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mocker.patch.object(api, "cache", cache)
        mocker.patch.object(api, "tileset_service", service)
        mocker.patch.object(
            main, "create_mongodb_client", return_value=AsyncMongoMockClient()
        )
        mocker.patch.object(main.WorkflowProfile, "from_env", return_value=None)

        with TestClient(main.app) as client:
            # Lifespan startup pointed api.db at the mongomock client;
            # point it back at the fake holding the contact-map document.
            # The lifespan teardown closes the patched service on the
            # client's own event loop when the block exits.
            mocker.patch.object(api, "db", mock_db)
            yield client, doc, cache

    def test_should_serve_json_tiles_over_http_matching_a_direct_read(
        self, http_client
    ):
        """Test the tile endpoints across the real ASGI wire.

        Given:
            A committed tileset artifact with the real service patched
            into the app and the lifespan otherwise neutered.
        When:
            /tileset_info and /tiles are requested over HTTP.
        Then:
            It should answer 200 with bodies that parse as JSON, and the
            decoded dense tile should match a direct cooler read of the
            committed artifact — proving the payloads survive FastAPI's
            JSON serialization intact.
        """
        # Arrange
        import cooler

        client, doc, cache = http_client
        tile_id = f"{UID}.0.0.0"

        # Act
        info_response = client.get(f"/tileset_info/?d={UID}")
        tiles_response = client.get(f"/tiles/?d={tile_id}")

        # Assert
        assert info_response.status_code == 200
        assert tiles_response.status_code == 200
        info = info_response.json()[UID]
        payload = tiles_response.json()[tile_id]
        assert info["datatype"] == "matrix"
        assert info["resolutions"] == [1, 2, 4]

        block = _decode(payload)
        assert block.shape == (256, 256)
        key = MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)
        clr = cooler.Cooler(f"{cache.path_for(key)}::/resolutions/4")
        expected = clr.matrix(balance=False).fetch("c1")[:8, :8]
        np.testing.assert_allclose(block[:8, :8], expected.astype(np.float32))


class TestConcurrentColdOpen:
    """The ``_acquire`` winner/loser contract over real h5py handles."""

    @pytest.mark.parametrize("n", [2, 10])
    @pytest.mark.asyncio
    async def test_should_serve_identical_tiles_under_concurrent_cold_open(
        self, served, tmp_path, n
    ):
        """Test that a cold dataset survives a burst of simultaneous opens.

        Given:
            A committed tileset artifact behind a fresh service no request
            has touched yet.
        When:
            N tile requests for the same tile are gathered concurrently.
        Then:
            It should return N identical, correct payloads — the open
            race resolves to one shared handle with every loser closed —
            and the service should close cleanly afterwards.
        """
        # Arrange
        import cooler

        source = build_mcool(tmp_path / "upstream.mcool")
        doc, _, cache = await served(source, "sample.mcool")
        tile_id = f"{UID}.0.0.0"

        # Act
        responses = await asyncio.gather(*(tiles(d=[tile_id]) for _ in range(n)))
        payloads = [response[tile_id] for response in responses]

        # Assert
        assert len(payloads) == n
        assert all(payload == payloads[0] for payload in payloads)

        block = _decode(payloads[0])
        key = MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)
        clr = cooler.Cooler(f"{cache.path_for(key)}::/resolutions/4")
        expected = clr.matrix(balance=False).fetch("c1")[:8, :8]
        np.testing.assert_allclose(block[:8, :8], expected.astype(np.float32))

        await api.tileset_service.aclose()
        assert api.tileset_service.tile_cache_bytes_used == 0


class TestEvictionUnderLiveRead:
    """The store↔service release-hook coupling with real handles."""

    @pytest.mark.asyncio
    async def test_should_overshoot_for_a_pinned_artifact_and_reclaim_after_release(
        self, mocker, tmp_path
    ):
        """Test disk eviction against live h5py handles on the S3-style path.

        Given:
            Three tileset artifacts in a remote-style cache behind a disk
            budget that fits exactly one, with a checkout of dataset A
            held open.
        When:
            B is served while A is pinned, then A is released and a third
            dataset forces an eviction pass, then B is served again.
        Then:
            It should serve B correctly both times, keep A's hydrated
            file on disk while it is pinned (the budget overshoots rather
            than unlinking under a live handle), and reclaim back inside
            the budget once A is released.
        """
        # Arrange
        import cooler

        third_md5 = "5d41402abc4b2a76b9719d911017c592"
        specs = [
            ("IDA", FIXTURE_MD5, 0),
            ("IDB", FIXTURE_MD5_ALT, 1),
            ("IDC", third_md5, 2),
        ]
        remote = FakeRemoteCache({})
        processor = MatrixTilesetProcessor()
        docs, sources, keys = {}, {}, {}
        for local_id, md5, seed in specs:
            doc = _file_doc("sample.mcool")
            doc["local_id"] = local_id
            doc["md5"] = md5
            docs[local_id] = doc
            sources[md5] = build_mcool(tmp_path / f"upstream-{local_id}.mcool", seed=seed)
            keys[local_id] = processor.cache_key_for(doc, ArtifactKind.TILESET)

        async def _download(file_meta, dest):
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(sources[file_meta["md5"]].read_bytes())
            return dest

        mocker.patch.object(matrix_module, "download_source", side_effect=_download)
        for local_id, _, _ in specs:
            async for _ in processor.run(
                docs[local_id], tmp_path / "workdir" / local_id, remote
            ):
                pass

        sizes = {local_id: len(remote.blobs[keys[local_id]]) for local_id in docs}
        budget = max(sizes.values())
        # The premise: any one artifact fits, no two do.
        assert min(sizes.values()) * 2 > budget

        store_root = tmp_path / "tilesets"
        store = LocalTilesetStore(store_root, max_bytes=budget)
        service = TilesetService(
            store,
            open_max=4,
            tile_cache_bytes=0,  # force every serve through a real read
            threads=2,
            hydrate_timeout_s=60,
            cache_provider=lambda: remote,
        )
        tile_b = "4dn/IDB.0.0.0"

        # Act & assert
        async with service.checkout("4dn/IDA", docs["IDA"]):
            # Open A's h5py file for real, so the pinned handle is live.
            await service.tileset_info("4dn/IDA", docs["IDA"])

            first = (await service.tiles("4dn/IDB", docs["IDB"], [tile_b]))[tile_b]

            # B hydrated beside pinned A: both files on disk, over budget.
            assert store.resident_bytes == sizes["IDA"] + sizes["IDB"]
            assert store.resident_bytes > budget
            assert len(list(store_root.iterdir())) == 2

        # A released; a third hydration triggers the eviction pass, which
        # now closes A's (and B's) real handle through the release hook.
        await service.tiles("4dn/IDC", docs["IDC"], ["4dn/IDC.0.0.0"])
        assert store.resident_bytes == sizes["IDC"]
        assert store.resident_bytes <= budget

        second = (await service.tiles("4dn/IDB", docs["IDB"], [tile_b]))[tile_b]

        assert second == first
        clr = cooler.Cooler(f"{sources[FIXTURE_MD5_ALT]}::/resolutions/4")
        expected = clr.matrix(balance=False).fetch("c1")[:8, :8]
        np.testing.assert_allclose(
            _decode(second)[:8, :8], expected.astype(np.float32)
        )
        # B was re-hydrated after its eviction, and back inside the budget.
        assert remote.get_calls.count(keys["IDB"]) == 2
        assert store.resident_bytes == sizes["IDB"]

        await service.aclose()


class TestBalancedServe:
    """Balancing transforms through the processor into a served tile."""

    @pytest.mark.asyncio
    async def test_should_serve_a_balanced_tile_matching_a_direct_balanced_read(
        self, served, tmp_path
    ):
        """Test that a named balancing transform reaches the pixels.

        Given:
            An mcool carrying two weight columns — two, because ``weight``
            is also clodius's default fallback, so only a second named
            column makes honouring the request observable — put through
            the processor into a real cache.
        When:
            A tile naming the ``alt_weight`` transform is requested.
        Then:
            It should decode to the values a direct balanced cooler fetch
            with that column yields, within dtype quantization, and
            differ from the default-weight balance — proving the named
            request was honoured rather than ignored.
        """
        # Arrange
        import cooler

        source = build_mcool(
            tmp_path / "upstream.mcool",
            weights={"weight": 0.5, "alt_weight": 2.0},
        )
        doc, _, cache = await served(source, "sample.mcool")
        tile_id = f"{UID}.0.0.0.alt_weight"

        # Act
        payload = (await tiles(d=[tile_id]))[tile_id]
        block = _decode(payload)

        # Assert
        key = MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)
        clr = cooler.Cooler(f"{cache.path_for(key)}::/resolutions/4")
        # Compare the whole of c1 (250 bins at 4 bp/bin) rather than a
        # corner, so the comparison actually crosses nonzero pixels.
        expected = clr.matrix(balance="alt_weight").fetch("c1")
        default = clr.matrix(balance=True).fetch("c1")
        assert (expected > 0).any()
        np.testing.assert_allclose(
            block[:250, :250], expected.astype(block.dtype), rtol=1e-3
        )
        assert not np.allclose(block[:250, :250], default.astype(block.dtype))
