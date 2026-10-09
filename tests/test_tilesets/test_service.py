"""Tests for :class:`cfdb.tilesets.service.TilesetService`."""

from __future__ import annotations

import asyncio
import base64

import numpy as np
import pytest
from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

from cfdb.tilesets.errors import (
    TilesetHydrationTimeout,
    TilesetIdentityIncomplete,
    TilesetNotReady,
    TilesetUnsupported,
)
from cfdb.tilesets.service import build_service
from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.matrix import MatrixTilesetProcessor
from tests.fixtures.remote_cache import FakeRemoteCache
from tests.test_workflows import FIXTURE_MD5, FIXTURE_MD5_ALT

UID = "4dn/4DNFIABC123"


def _file_doc(md5: str = FIXTURE_MD5, **overrides) -> dict:
    """Return a projected document for a 4DN mcool."""
    doc = {
        "submission": "4dn",
        "local_id": "4DNFIABC123",
        "md5": md5,
        "filename": "sample.mcool",
        "file_format": {"name": "HDF5"},
        "genome_assembly": "GRCh38",
        "dcc": {"dcc_abbreviation": "4DN_DCIC"},
    }
    doc.update(overrides)
    return doc


def _tileset_key(doc: dict) -> str:
    """The TILESET cache key for ``doc``, derived the way the service does."""
    return MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)


async def _seed(cache: LocalFsCache, mcool, doc: dict, tmp_path) -> str:
    """Commit ``mcool`` as the TILESET artifact for ``doc``; return the key."""
    key = _tileset_key(doc)
    staged = tmp_path / f"staged-{key.replace('/', '_')}"
    staged.write_bytes(mcool.read_bytes())
    await cache.put(key, staged)
    return key


@pytest.fixture()
def service_factory(tmp_path):
    """Build services sharing one LocalFsCache, and close them afterwards."""
    cache = LocalFsCache(tmp_path / "cache")
    built = []

    def _build(**overrides):
        kwargs = {
            "root": tmp_path / "tilesets",
            "cache_provider": lambda: cache,
            "disk_cache_bytes": 10**9,
            "open_max": 2,
            "tile_cache_bytes": 10**7,
            "threads": 2,
            "hydrate_timeout_s": 30,
        }
        kwargs.update(overrides)
        service = build_service(**kwargs)
        built.append(service)
        return service

    yield cache, _build

    async def _close():
        for service in built:
            await service.aclose()

    asyncio.run(_close())


class TestResolution:
    """What the service refuses to open, and why."""

    @pytest.mark.asyncio
    async def test_should_refuse_a_file_that_is_not_a_contact_map(
        self, service_factory
    ):
        """Test that a non-matrix file is permanently unsupported.

        Given:
            An AnnData matrix, which shares the HDF5 EDAM term with
            contact maps.
        When:
            tileset_info is requested.
        Then:
            It should raise TilesetUnsupported — a permanent condition,
            distinct from an artifact that has merely not been built yet.
        """
        # Arrange
        _, build = service_factory
        service = build()

        # Act & Assert
        with pytest.raises(TilesetUnsupported):
            await service.tileset_info(UID, _file_doc(filename="matrix.h5ad"))

    @pytest.mark.asyncio
    async def test_should_report_not_ready_when_no_artifact_is_cached(
        self, service_factory
    ):
        """Test that an unbuilt artifact is not-ready, not unsupported.

        Given:
            A contact map with nothing in the cache.
        When:
            tileset_info is requested.
        Then:
            It should raise TilesetNotReady and dispatch nothing. The tile
            endpoints are pure readers; a heatmap track cannot be handed a
            job id, so building is the preparation channel's job.
        """
        # Arrange
        _, build = service_factory
        service = build()

        # Act & Assert
        with pytest.raises(TilesetNotReady):
            await service.tileset_info(UID, _file_doc())

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method", ["tileset_info", "tiles"])
    async def test_should_refuse_a_document_with_no_md5(
        self, service_factory, method
    ):
        """Test the identity seam behind the router's 409.

        Given:
            A contact-map document carrying no md5, so no cache key can
            be derived for it.
        When:
            tileset_info or tiles is requested.
        Then:
            It should raise TilesetIdentityIncomplete — the service's own
            vocabulary, not the ValueError the key derivation raises — so
            the router can render a 409 instead of a 500.
        """
        # Arrange
        _, build = service_factory
        doc = _file_doc()
        del doc["md5"]
        service = build()

        # Act & Assert
        with pytest.raises(TilesetIdentityIncomplete):
            if method == "tileset_info":
                await service.tileset_info(UID, doc)
            else:
                await service.tiles(UID, doc, [f"{UID}.0.0.0"])

    @pytest.mark.asyncio
    async def test_should_report_not_ready_when_no_cache_is_configured(
        self, service_factory
    ):
        """Test the guard for a workflow subsystem that never came up.

        Given:
            A cache provider that resolves to None — the shape of a
            deployment with no cache configured.
        When:
            tileset_info is requested.
        Then:
            It should raise TilesetNotReady rather than an AttributeError
            from calling head on None — the router turns this into a 404,
            not a 500.
        """
        # Arrange
        _, build = service_factory
        service = build(cache_provider=lambda: None)

        # Act & Assert
        with pytest.raises(TilesetNotReady):
            await service.tileset_info(UID, _file_doc())


class TestTilesetInfo:
    """The HiGlass info document."""

    @pytest.mark.asyncio
    async def test_should_serve_info_from_a_cached_artifact(
        self, service_factory, tiny_mcool, tmp_path
    ):
        """Test the assembled tileset_info document.

        Given:
            A cached mcool artifact.
        When:
            tileset_info is requested.
        Then:
            It should carry clodius's ladder fields plus the four the
            library does not model, and omit the two clodius deliberately
            leaves unset for an explicit ladder.
        """
        # Arrange
        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build()

        # Act
        info = await service.tileset_info(UID, doc)

        # Assert
        assert info["datatype"] == "matrix"
        assert info["name"] == "sample.mcool"
        assert info["uuid"] == UID
        assert info["coordSystem"] == "GRCh38"
        assert info["resolutions"] == [1, 2, 4]
        assert info["tile_size"] == 256
        assert "max_width" not in info
        assert "max_zoom" not in info

    @pytest.mark.asyncio
    async def test_should_omit_coord_system_when_no_assembly_is_recorded(
        self, service_factory, tiny_mcool, tmp_path
    ):
        """Test the 21 4DN files with no genome assembly.

        Given:
            A contact map whose document records no genome_assembly.
        When:
            tileset_info is requested.
        Then:
            ``coordSystem`` should be absent entirely rather than empty.
            HiGlass matches it against chromosome-info tilesets, so a
            placeholder risks a silently misaligned track, whereas an
            absent key makes the client fall back to the chromsizes array
            the document always carries.
        """
        # Arrange
        cache, build = service_factory
        doc = _file_doc()
        del doc["genome_assembly"]
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build()

        # Act
        info = await service.tileset_info(UID, doc)

        # Assert
        assert "coordSystem" not in info
        assert info["chromsizes"]


class TestHydrationTimeout:
    """A hydration that overruns its budget must fail legibly."""

    @pytest.mark.asyncio
    async def test_should_raise_hydration_timeout_when_the_download_stalls(
        self, service_factory, tiny_mcool
    ):
        """Test the translation of a stalled hydration into 503 vocabulary.

        Given:
            A non-local cache whose download stream is gated shut, and a
            hydrate timeout of a few milliseconds.
        When:
            tileset_info is requested.
        Then:
            It should raise TilesetHydrationTimeout chained from the
            asyncio timeout — the type the router renders as 503 with
            Retry-After, because the download may well finish and a
            retry will find it.
        """
        # Arrange
        _, build = service_factory
        doc = _file_doc()
        remote = FakeRemoteCache({_tileset_key(doc): tiny_mcool.read_bytes()})
        remote.gate = asyncio.Event()  # never opened: the download stalls
        service = build(cache_provider=lambda: remote, hydrate_timeout_s=0.05)

        # Act & Assert
        with pytest.raises(TilesetHydrationTimeout) as excinfo:
            await service.tileset_info(UID, doc)
        assert isinstance(excinfo.value.__cause__, asyncio.TimeoutError)


class TestTiles:
    """Batch semantics, which are where the sharp edges are."""

    @pytest.mark.asyncio
    async def test_should_return_a_dense_payload_for_a_valid_tile(
        self, service_factory, tiny_mcool, tmp_path
    ):
        """Test the tile payload shape.

        Given:
            A cached mcool artifact.
        When:
            A valid tile is requested.
        Then:
            The payload should be keyed on the request string and carry a
            base64 dense block with no ``shape`` field — the client
            recovers the side length as sqrt(len), which is the contract
            clodius emits.
        """
        # Arrange
        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build()

        # Act
        payloads = await service.tiles(UID, doc, [f"{UID}.0.0.0"])

        # Assert
        payload = payloads[f"{UID}.0.0.0"]
        assert set(payload) == {"dense", "dtype", "min_value", "max_value", "size"}
        flat = np.frombuffer(base64.b64decode(payload["dense"]), payload["dtype"])
        assert len(flat) == 256 * 256

    @pytest.mark.asyncio
    async def test_should_report_an_out_of_ladder_tile_as_a_per_tile_error(
        self, service_factory, tiny_mcool, tmp_path
    ):
        """Test the one-entry-per-id contract for a position off the ladder.

        Given:
            A batch pairing a valid tile with one past the resolution
            ladder.
        When:
            tiles is called.
        Then:
            Both ids should appear in the response — the valid tile
            returns tile data, and the out-of-ladder one gets a per-tile
            ``{"error": ...}`` object rather than being omitted. clodius
            now returns exactly one entry per requested id, so a response
            built by zipping request to result is safe.
        """
        # Arrange
        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build()

        # Act
        payloads = await service.tiles(
            UID, doc, [f"{UID}.0.0.0", f"{UID}.9.0.0"]
        )

        # Assert
        assert set(payloads) == {f"{UID}.0.0.0", f"{UID}.9.0.0"}
        assert "dense" in payloads[f"{UID}.0.0.0"]
        assert set(payloads[f"{UID}.9.0.0"]) == {"error"}

    @pytest.mark.asyncio
    async def test_should_report_a_malformed_id_per_tile(
        self, service_factory, tiny_mcool, tmp_path
    ):
        """Test that one bad id does not blank the whole track.

        Given:
            A batch pairing a valid tile id with a malformed one.
        When:
            tiles is called.
        Then:
            The malformed id should get an error object while its sibling
            returns data, matching HiGlass's per-tile error convention.
        """
        # Arrange
        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build()

        # Act
        payloads = await service.tiles(
            UID, doc, [f"{UID}.0.0.0", f"{UID}.notanint.0"]
        )

        # Assert
        assert "error" in payloads[f"{UID}.notanint.0"]
        assert "dense" in payloads[f"{UID}.0.0.0"]

    @pytest.mark.asyncio
    async def test_should_report_an_unavailable_transform_per_tile(
        self, service_factory, tiny_mcool, tmp_path
    ):
        """Test that a bad balancing column is a tile error, not a 500.

        Given:
            A tile id naming a balancing column the cooler does not carry.
        When:
            tiles is called.
        Then:
            It should return an error object. clodius propagates this
            rather than skipping it — a client asking for a column that
            does not exist made a different kind of mistake than one
            asking for a tile off the end of the genome — and the boundary
            renders it rather than failing the request.
        """
        # Arrange
        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build()

        # Act
        payloads = await service.tiles(UID, doc, [f"{UID}.0.0.0.nosuchcolumn"])

        # Assert
        assert "error" in payloads[f"{UID}.0.0.0.nosuchcolumn"]

    @pytest.mark.asyncio
    async def test_should_treat_two_spellings_of_one_tile_as_two_entries(
        self, service_factory, tiny_mcool, tmp_path
    ):
        """Test that the response key is the request string, verbatim.

        Given:
            The same tile requested twice, once with a trailing options
            separator.
        When:
            tiles is called.
        Then:
            Both spellings should appear. clodius makes the raw request
            string part of a tile id's identity precisely because the
            client matches responses to requests by exact string.
        """
        # Arrange
        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build()

        # Act
        payloads = await service.tiles(UID, doc, [f"{UID}.0.0.0", f"{UID}.0.0.0,"])

        # Assert
        assert set(payloads) == {f"{UID}.0.0.0", f"{UID}.0.0.0,"}

    @pytest.mark.asyncio
    async def test_should_return_empty_without_opening_for_an_empty_batch(
        self, service_factory, mocker
    ):
        """Test the zero-input short-circuit.

        Given:
            A contact-map document and an empty tile-id sequence.
        When:
            tiles is called.
        Then:
            It should return an empty mapping without checking out a
            tileset — there is nothing to read, so nothing should be
            hydrated or opened, even when no artifact exists at all.
        """
        # Arrange
        _, build = service_factory
        service = build()
        checkout = mocker.spy(service, "checkout")

        # Act
        payloads = await service.tiles(UID, _file_doc(), [])

        # Assert
        assert payloads == {}
        checkout.assert_not_called()

    @pytest.mark.asyncio
    async def test_should_scope_a_missing_balancing_column_to_its_modifier_group(
        self, service_factory, tiny_mcool, tmp_path
    ):
        """Test the blast radius of an unusable transform.

        Given:
            A batch pairing a valid (untransformed) tile id with one
            naming a balancing column the cooler does not carry — an id
            that parses fine, so clodius raises TileError only once it
            tries to resolve the transform for that id's (zoom, modifier)
            group.
        When:
            tiles is called.
        Then:
            Only the id sharing the bad modifier's group should get an
            error payload. clodius batches per (zoom, modifier) rather
            than per whole request, so a valid sibling requesting a
            different (in this case, no) transform is unaffected.
        """
        # Arrange
        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build()
        valid = f"{UID}.0.0.0"
        bad = f"{UID}.0.0.0.nosuchcolumn"

        # Act
        payloads = await service.tiles(UID, doc, [valid, bad])

        # Assert
        assert "error" in payloads[bad]
        assert "dense" in payloads[valid]

    @pytest.mark.asyncio
    async def test_should_reraise_a_non_tile_error_from_the_backend(
        self, service_factory, tiny_mcool, tmp_path, mocker
    ):
        """Test that a genuine bug is not laundered into a tile error.

        Given:
            A tile backend whose batch read raises RuntimeError — not a
            clodius TileError.
        When:
            tiles is called.
        Then:
            The RuntimeError should propagate. Per the errors contract,
            only clodius's own per-tile hierarchy becomes error payloads;
            anything else is a bug and must surface as a 500.
        """
        # Arrange
        from clodius.tiles_v2.cooler import CoolerTileset

        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build()
        mocker.patch.object(
            CoolerTileset, "tiles", side_effect=RuntimeError("h5py went away")
        )

        # Act & Assert
        with pytest.raises(RuntimeError, match="h5py went away"):
            await service.tiles(UID, doc, [f"{UID}.0.0.0"])

    @pytest.mark.asyncio
    async def test_should_serve_a_named_balancing_transform(
        self, service_factory, balanced_mcool, tmp_path
    ):
        """Test the balancing happy path against a weighted cooler.

        Given:
            A cached mcool carrying both a default ``weight`` column and
            a distinct ``alt_weight`` column.
        When:
            The same tile is requested with no modifier and with the
            ``alt_weight`` transform named.
        Then:
            Both should return dense payloads, and the transformed values
            should differ from the default — proving the named column was
            honoured rather than silently falling back to ``weight``.
        """
        # Arrange
        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, balanced_mcool, doc, tmp_path)
        service = build()
        default_id = f"{UID}.0.0.0"
        alt_id = f"{UID}.0.0.0.alt_weight"

        # Act
        payloads = await service.tiles(UID, doc, [default_id, alt_id])

        # Assert
        assert "dense" in payloads[default_id]
        assert "dense" in payloads[alt_id]
        default_block = np.frombuffer(
            base64.b64decode(payloads[default_id]["dense"]),
            payloads[default_id]["dtype"],
        ).astype(np.float64)
        alt_block = np.frombuffer(
            base64.b64decode(payloads[alt_id]["dense"]),
            payloads[alt_id]["dtype"],
        ).astype(np.float64)
        assert not np.array_equal(default_block, alt_block, equal_nan=True)

    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        positions=st.lists(
            st.tuples(
                st.integers(min_value=0, max_value=2),
                st.integers(min_value=0, max_value=2),
                st.integers(min_value=0, max_value=2),
            ),
            min_size=1,
            max_size=4,
            unique=True,
        )
    )
    @pytest.mark.asyncio
    async def test_should_emit_the_wire_contract_for_any_valid_batch(
        self, service_factory, tiny_mcool, tmp_path, positions
    ):
        """Test the HiGlass wire-contract invariant over valid batches.

        Given:
            Generated batches of (z, x, y) positions within tiny_mcool's
            ladder — resolutions [1, 2, 4] over a 3000 bp genome, so z in
            0..2 and x, y in 0..2 are valid at every zoom.
        When:
            tiles is called.
        Then:
            It should key every payload on a requested id, carry exactly
            the five contractual fields with no ``shape``, declare a
            dtype of float16 or float32, and encode a base64 dense block
            that decodes to 256x256 values of the declared dtype.
        """
        # Arrange
        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build()
        ids = [f"{UID}.{z}.{x}.{y}" for z, x, y in positions]

        # Act
        payloads = await service.tiles(UID, doc, ids)

        # Assert
        assert set(payloads) <= set(ids)
        for payload in payloads.values():
            assert set(payload) == {
                "dense",
                "dtype",
                "min_value",
                "max_value",
                "size",
            }
            assert payload["dtype"] in {"float16", "float32"}
            flat = np.frombuffer(
                base64.b64decode(payload["dense"]), payload["dtype"]
            )
            assert flat.shape == (256 * 256,)

    @settings(
        max_examples=15,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        suffix=st.one_of(
            st.text(
                alphabet=st.sampled_from(list("0123456789.,:-_ weight")),
                max_size=12,
            ),
            st.text(max_size=12),
        )
    )
    @example(suffix="")
    @example(suffix="0.0.0,")
    @example(suffix=" 1.0.0")
    @example(suffix="0.0.0.")
    @pytest.mark.asyncio
    async def test_should_contain_any_garbage_id_within_the_batch_contract(
        self, service_factory, tiny_mcool, tmp_path, suffix
    ):
        """Test the parse-boundary robustness of a mixed batch.

        Given:
            Batches pairing one valid tile id with a generated garbage
            suffix appended to the valid uid prefix — empty, dotted,
            comma-optioned, unicode, whitespace-padded, or naming a
            transform.
        When:
            tiles is called.
        Then:
            No exception should escape; every returned key should be one
            of the requested ids, and every payload either tile data or
            an error object. This is a deliberate bug-hunter: a parse
            failure outside clodius's TileError hierarchy would re-raise
            out of the error translator and 500 the batch.
        """
        # Arrange
        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build()
        valid = f"{UID}.0.0.0"
        garbage = f"{UID}.{suffix}"

        # Act
        payloads = await service.tiles(UID, doc, [valid, garbage])

        # Assert
        assert set(payloads) <= {valid, garbage}
        for payload in payloads.values():
            assert "dense" in payload or "error" in payload


class TestTileCache:
    """Repeat pans must not re-read the file."""

    @pytest.mark.asyncio
    async def test_should_serve_a_repeat_request_from_cache(
        self, service_factory, tiny_mcool, tmp_path, mocker
    ):
        """Test that a second identical request does not reach the tileset.

        Given:
            A tile already read once.
        When:
            The same tile is requested again.
        Then:
            The tileset should not be checked out again — h5py serializes
            on a global lock, so re-reading is the cost the cache exists
            to avoid.
        """
        # Arrange
        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build()
        await service.tiles(UID, doc, [f"{UID}.0.0.0"])
        checkout = mocker.spy(service, "checkout")

        # Act
        payloads = await service.tiles(UID, doc, [f"{UID}.0.0.0"])

        # Assert
        assert "dense" in payloads[f"{UID}.0.0.0"]
        checkout.assert_not_called()

    @pytest.mark.asyncio
    async def test_should_evict_by_bytes_rather_than_entry_count(
        self, service_factory, tiny_mcool, tmp_path
    ):
        """Test that the tile cache respects a byte budget.

        Given:
            A tile cache sized to hold roughly one tile.
        When:
            Several distinct tiles are read.
        Then:
            The cache should stay within its budget. Bounding by entry
            count instead would let ~350 KiB base64 payloads accumulate
            into gigabytes on a 2 GiB task.
        """
        # Arrange
        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build(tile_cache_bytes=200_000)

        # Act
        await service.tiles(
            UID,
            doc,
            [f"{UID}.2.0.0", f"{UID}.2.1.0", f"{UID}.2.0.1", f"{UID}.2.1.1"],
        )

        # Assert
        assert 0 < service.tile_cache_bytes_used <= 200_000

    @pytest.mark.asyncio
    async def test_should_miss_when_the_source_bytes_change(
        self, service_factory, tiny_mcool, tmp_path, mocker
    ):
        """Test that the tile cache key follows the artifact's identity.

        Given:
            A tile cached for a document, and then the same file with a
            different upstream md5.
        When:
            The same tile position is requested.
        Then:
            It should be re-read rather than served from cache. The tile
            key embeds the artifact cache key, which embeds the md5 and
            the processor version — so an upstream byte change or a
            version bump invalidates every tile for free.
        """
        # Arrange
        cache, build = service_factory
        original = _file_doc()
        await _seed(cache, tiny_mcool, original, tmp_path)
        revised = _file_doc(md5=FIXTURE_MD5_ALT)
        await _seed(cache, tiny_mcool, revised, tmp_path)
        service = build()
        await service.tiles(UID, original, [f"{UID}.0.0.0"])
        checkout = mocker.spy(service, "checkout")

        # Act
        await service.tiles(UID, revised, [f"{UID}.0.0.0"])

        # Assert
        checkout.assert_called_once()

    @pytest.mark.asyncio
    async def test_should_serve_but_not_cache_an_oversized_payload(
        self, service_factory, tiny_mcool, tmp_path, mocker
    ):
        """Test that a payload larger than the whole budget is skipped.

        Given:
            A tile cache whose byte budget is smaller than a single tile
            payload.
        When:
            The same tile is requested twice.
        Then:
            The first response should carry data while the cache stays
            at zero bytes, and the second request should check the
            tileset out again — served but uncacheable, rather than
            evicting the world to admit one tile that cannot fit.
        """
        # Arrange
        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build(tile_cache_bytes=1_000)
        tile = f"{UID}.0.0.0"
        first = await service.tiles(UID, doc, [tile])
        checkout = mocker.spy(service, "checkout")

        # Act
        second = await service.tiles(UID, doc, [tile])

        # Assert
        assert "dense" in first[tile]
        assert "dense" in second[tile]
        assert service.tile_cache_bytes_used == 0
        checkout.assert_called_once()

    @pytest.mark.asyncio
    async def test_should_read_only_the_cold_tile_of_a_partial_hit_batch(
        self, service_factory, tiny_mcool, tmp_path, mocker
    ):
        """Test that cached entries are merged with a narrowed file read.

        Given:
            Tile A already cached, and a batch pairing A with a cold
            tile B.
        When:
            tiles is called for both.
        Then:
            Both payloads should be returned, but only B should reach
            the backend — the batch handed to the tileset carries
            exactly the one id the cache could not answer.
        """
        # Arrange
        from clodius.tiles_v2.cooler import CoolerTileset

        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build()
        tile_a = f"{UID}.0.0.0"
        tile_b = f"{UID}.1.0.0"
        await service.tiles(UID, doc, [tile_a])
        tiles_spy = mocker.spy(CoolerTileset, "tiles")

        # Act
        payloads = await service.tiles(UID, doc, [tile_a, tile_b])

        # Assert
        assert "dense" in payloads[tile_a]
        assert "dense" in payloads[tile_b]
        tiles_spy.assert_called_once()
        parsed = tiles_spy.call_args.args[1]
        assert [tile_id.raw for tile_id in parsed] == [tile_b]

    @pytest.mark.asyncio
    async def test_should_release_cached_tiles_with_their_evicted_handle(
        self, service_factory, tiny_mcool, tmp_path
    ):
        """Test the coupling between the tile cache and the handle pool.

        Given:
            An open-tileset cap of one, tiles cached for dataset A, and
            a second dataset B.
        When:
            Opening B evicts A's handle past the cap.
        Then:
            The tile cache should drop by exactly A's bytes — cached
            tiles are released with their handle, so an evicted dataset
            cannot pin stale payloads in memory.
        """
        # Arrange
        cache, build = service_factory
        doc_a = _file_doc()
        doc_b = _file_doc(md5=FIXTURE_MD5_ALT, local_id="4DNFIXYZ789")
        await _seed(cache, tiny_mcool, doc_a, tmp_path)
        await _seed(cache, tiny_mcool, doc_b, tmp_path)
        service = build(open_max=1)
        await service.tiles(UID, doc_a, [f"{UID}.0.0.0"])
        cached_bytes = service.tile_cache_bytes_used
        assert cached_bytes > 0

        # Act
        await service.tileset_info("4dn/4DNFIXYZ789", doc_b)

        # Assert — only A's tiles were cached, so dropping A's bytes is zero
        assert service.tile_cache_bytes_used == 0


class TestOpenTilesetLifetime:
    """Handles are pooled, and closing one under a reader is a segfault."""

    @pytest.mark.asyncio
    async def test_should_close_a_handle_evicted_past_the_open_cap(
        self, service_factory, make_mcool, tmp_path, mocker
    ):
        """Test that the open-handle pool is bounded.

        Given:
            An open-tileset cap of one and two distinct contact maps.
        When:
            Both are opened.
        Then:
            The first handle should be closed — observed as exactly one
            ``close()`` at the clodius boundary, on the handle the first
            checkout yielded. Each is an open h5py handle over a
            multi-GB file, so this is a file-descriptor bound.
        """
        # Arrange
        from clodius.tiles_v2.cooler import CoolerTileset

        cache, build = service_factory
        first_doc = _file_doc()
        second_doc = _file_doc(md5=FIXTURE_MD5_ALT, local_id="4DNFIXYZ789")
        await _seed(cache, make_mcool(), first_doc, tmp_path)
        await _seed(cache, make_mcool(), second_doc, tmp_path)
        service = build(open_max=1)
        close_spy = mocker.spy(CoolerTileset, "close")

        async with service.checkout(UID, first_doc) as first:
            assert first.info() is not None  # force the handle open
        # Constructing `first` already released its own validation-time
        # handle — clodius's construction reads the file's chromsizes
        # then releases, reopening lazily on the first real call
        # (clodius#14/#18's "registration is free" property) — not the
        # eviction this test is about.
        close_spy.assert_called_once()
        assert close_spy.call_args.args[0] is first
        close_spy.reset_mock()

        # Act
        await service.tileset_info("4dn/4DNFIXYZ789", second_doc)

        # Assert — `second`'s own construction releases itself the same
        # way; the eviction this test targets is the call right after it,
        # against `first`.
        assert close_spy.call_count == 2
        assert close_spy.call_args_list[-1].args[0] is first

    @pytest.mark.asyncio
    async def test_should_defer_closing_a_handle_that_is_being_read(
        self, service_factory, make_mcool, tmp_path, mocker
    ):
        """Test the reference-counted close that prevents a segfault.

        Given:
            A tileset checked out by a reader, and a second tileset
            opened that evicts it past the cap.
        When:
            The eviction lands while the first reader is still inside its
            checkout.
        Then:
            The handle must stay open until that reader leaves — no
            ``close()`` reaches the clodius boundary and the held handle
            keeps serving reads — and be closed exactly once afterwards.
            Closing an h5py handle under a live numpy read in a worker
            thread is a segfault, not an exception, so this cannot be
            left to chance.
        """
        # Arrange
        from clodius.tiles_v2.cooler import CoolerTileset

        cache, build = service_factory
        first_doc = _file_doc()
        second_doc = _file_doc(md5=FIXTURE_MD5_ALT, local_id="4DNFIXYZ789")
        await _seed(cache, make_mcool(), first_doc, tmp_path)
        await _seed(cache, make_mcool(), second_doc, tmp_path)
        service = build(open_max=1)
        close_spy = mocker.spy(CoolerTileset, "close")

        # Act & Assert
        async with service.checkout(UID, first_doc) as held:
            assert held.info() is not None  # force the handle open
            # Constructing `held` already released its own
            # validation-time handle (clodius#14/#18's "registration is
            # free" property) — not the eviction this test is about.
            close_spy.assert_called_once()
            close_spy.reset_mock()

            await service.tileset_info("4dn/4DNFIXYZ789", second_doc)
            # `second`'s own construction releases itself the same way.
            # The real eviction must still defer, because `held` is
            # still checked out — it must not be among these calls.
            close_spy.assert_called_once()
            assert close_spy.call_args.args[0] is not held
            close_spy.reset_mock()

            assert held.info() is not None  # still serving the reader
            close_spy.assert_not_called()

        # Assert — the last reader out performs the (deferred) close
        close_spy.assert_called_once()
        assert close_spy.call_args.args[0] is held

    @pytest.mark.asyncio
    async def test_should_share_one_handle_between_concurrent_cold_opens(
        self, service_factory, tiny_mcool, mocker
    ):
        """Test the winner/loser dedup of a concurrent cold open.

        Given:
            Two concurrent checkouts of the same cold uid over a gated
            remote cache, so both miss the pool and both hydrate.
        When:
            The gate opens and both complete.
        Then:
            Both readers should be handed the identical tileset object,
            and at most one close should reach the clodius boundary —
            the loser's redundant handle, never the shared one — so no
            second h5py handle leaks onto the same file.
        """
        # Arrange
        from clodius.tiles_v2.cooler import CoolerTileset

        _, build = service_factory
        doc = _file_doc()
        remote = FakeRemoteCache({_tileset_key(doc): tiny_mcool.read_bytes()})
        remote.gate = asyncio.Event()
        service = build(cache_provider=lambda: remote)
        close_spy = mocker.spy(CoolerTileset, "close")
        seen = []

        async def grab():
            async with service.checkout(UID, doc) as tileset:
                seen.append(tileset)

        first = asyncio.create_task(grab())
        second = asyncio.create_task(grab())
        await asyncio.sleep(0.05)  # let both miss the pool and reach the gate

        # Act
        remote.gate.set()
        await asyncio.gather(first, second)

        # Assert — the winner is closed exactly once: its own
        # construction-time self-release (clodius#14/#18's
        # "registration is free" property), never a second time while
        # it's the shared, still-open handle. The loser is closed twice
        # (that same self-release, plus the explicit dedup cleanup) —
        # this test only cares that those never land on the winner.
        assert seen[0] is seen[1]
        winner = seen[0]
        winner_closes = [
            call for call in close_spy.call_args_list if call.args[0] is winner
        ]
        assert len(winner_closes) == 1

    @pytest.mark.asyncio
    async def test_should_survive_aclose_while_a_reader_is_inside_checkout(
        self, service_factory, tiny_mcool, tmp_path
    ):
        """Test that teardown under a live reader is orderly.

        Given:
            A tile cached over a remote-style cache, and a reader inside
            checkout when aclose is called.
        When:
            aclose completes while the reader is still in its block.
        Then:
            The handle should stay live for the reader, the tile cache
            should be emptied, and the store's hydrated residue should
            be gone — teardown must not yank the file out from under
            the reader's h5py handle.
        """
        # Arrange
        _, build = service_factory
        doc = _file_doc()
        remote = FakeRemoteCache({_tileset_key(doc): tiny_mcool.read_bytes()})
        service = build(cache_provider=lambda: remote)
        await service.tiles(UID, doc, [f"{UID}.0.0.0"])
        assert service.tile_cache_bytes_used > 0

        # Act & Assert
        async with service.checkout(UID, doc) as held:
            await service.aclose()
            assert held.info() is not None  # the reader's handle stays live

        # Assert
        assert service.tile_cache_bytes_used == 0
        assert not any((tmp_path / "tilesets").iterdir())

    @pytest.mark.xfail(
        strict=False,
        reason=(
            "aclose shuts the thread pool down before the last reader "
            "leaves, so the deferred close is submitted to an "
            "already-shutdown executor: _close swallows the RuntimeError "
            "('cannot schedule new futures after shutdown') and the h5py "
            "handle leaks at teardown instead of being closed"
        ),
    )
    @pytest.mark.asyncio
    async def test_should_close_the_held_handle_after_aclose_under_a_reader(
        self, service_factory, tiny_mcool, tmp_path, mocker
    ):
        """Test the deferred close of a handle held across aclose.

        Given:
            A reader inside checkout when aclose is called.
        When:
            aclose completes and the reader then exits its block.
        Then:
            The held handle should be closed exactly once by the last
            reader out — the same deferral contract eviction honours —
            rather than leaking because the close could no longer be
            scheduled.
        """
        # Arrange
        from clodius.tiles_v2.cooler import CoolerTileset

        cache, build = service_factory
        doc = _file_doc()
        await _seed(cache, tiny_mcool, doc, tmp_path)
        service = build()
        close_spy = mocker.spy(CoolerTileset, "close")

        # Act
        async with service.checkout(UID, doc) as held:
            await service.aclose()
            close_spy.assert_not_called()

        # Assert
        close_spy.assert_called_once()
        assert close_spy.call_args.args[0] is held
