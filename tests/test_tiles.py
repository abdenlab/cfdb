"""Tests for the HiGlass tile endpoints in ``cfdb.api.routers.tiles``."""

from __future__ import annotations

import asyncio
import base64
import itertools

import pytest
from fastapi import HTTPException
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

# numpy arrives transitively with clodius, which is only installed for the
# tiles extra (Python >= 3.12). Guard rather than let a bare import fail
# collection outright on 3.11.
np = pytest.importorskip("numpy")

from cfdb import api
from cfdb.api.routers._helpers import PATH_PARAM_MAX_LEN
from cfdb.api.routers.tiles import MAX_TILE_IDS, tiles, tileset_info
from cfdb.services import locks
from cfdb.tilesets import backend
from cfdb.tilesets.errors import (
    TileBackendUnavailable,
    TilesetHydrationTimeout,
    TilesetTooLarge,
)
from cfdb.tilesets.service import build_service
from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.matrix import MatrixTilesetProcessor
from tests.test_workflows import FIXTURE_MD5

UID = "4dn/4DNFIABC123"


def _file_doc(**overrides) -> dict:
    """Return a projected document for a 4DN mcool."""
    doc = {
        "submission": "4dn",
        "local_id": "4DNFIABC123",
        "md5": FIXTURE_MD5,
        "filename": "sample.mcool",
        "file_format": {"name": "HDF5"},
        "genome_assembly": "GRCh38",
        "dcc": {"dcc_abbreviation": "4DN_DCIC"},
    }
    doc.update(overrides)
    return doc


class _FailIfDispatched:
    """An executor stand-in that fails the test if a tile route dispatches."""

    async def ensure_workflow(self, _file_doc):
        raise AssertionError("tile endpoints must never dispatch a workflow")


class _FailIfConsulted:
    """A service stand-in that fails the test if a tile route reaches it."""

    async def tileset_info(self, *_args, **_kwargs):
        raise AssertionError("a malformed uid must never reach the service")

    async def tiles(self, *_args, **_kwargs):
        raise AssertionError("a malformed uid must never reach the service")


#: Characters that are legal in both halves of a tileset uid, minus ``.``
#: (rejected by the dot guard) — the alphabet well-formed parts draw from.
_PART_ALPHABET = (
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
)

#: Characters outside ``PATH_PARAM_PATTERN`` (and not ``/``, which would
#: change how the uid splits rather than making a part illegal).
_ILLEGAL_CHARS = "!@#$%^&*()+= ~\t\n\"'<>?,;:|\\[]{}éλ"

_wellformed_part = st.text(alphabet=_PART_ALPHABET, min_size=1, max_size=12)


@st.composite
def _malformed_uids(draw):
    """Generate uid strings violating exactly one shape rule each."""
    part = draw(_wellformed_part)
    other = draw(_wellformed_part)
    kind = draw(
        st.sampled_from(
            [
                "no-slash",
                "empty-dcc",
                "empty-local-id",
                "dotted-part",
                "oversized-part",
                "illegal-char",
            ]
        )
    )
    if kind == "no-slash":
        return part
    if kind == "empty-dcc":
        return f"/{part}"
    if kind == "empty-local-id":
        return f"{part}/"
    if kind == "dotted-part":
        dotted = f"{part}.{other}"
        return f"{dotted}/{other}" if draw(st.booleans()) else f"{part}/{dotted}"
    if kind == "oversized-part":
        oversized = part.ljust(PATH_PARAM_MAX_LEN + 1, "x")
        return (
            f"{oversized}/{other}" if draw(st.booleans()) else f"{part}/{oversized}"
        )
    position = draw(st.integers(min_value=0, max_value=len(part)))
    bad = part[:position] + draw(st.sampled_from(_ILLEGAL_CHARS)) + part[position:]
    return f"{bad}/{other}" if draw(st.booleans()) else f"{part}/{bad}"


@pytest.fixture()
def tile_env(mock_db, mocker, tmp_path):
    """Wire a real tile service over a LocalFsCache, plus a seeded document.

    Returns a callable that seeds a document's artifact from an mcool.
    """
    mocker.patch.object(locks, "wait_for_cutover", return_value=None)
    cache = LocalFsCache(tmp_path / "cache")
    service = build_service(
        root=tmp_path / "tilesets",
        cache_provider=lambda: cache,
        disk_cache_bytes=10**9,
        open_max=4,
        tile_cache_bytes=10**7,
        threads=2,
        hydrate_timeout_s=30,
    )
    mocker.patch.object(api, "cache", cache)
    mocker.patch.object(api, "tileset_service", service)
    mocker.patch.object(api, "executor", _FailIfDispatched())

    async def _seed(doc, mcool):
        key = MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)
        staged = tmp_path / f"staged-{key.replace('/', '_')}"
        staged.write_bytes(mcool.read_bytes())
        await cache.put(key, staged)

    yield _seed, mock_db, service


class TestTilesetInfo:
    """``GET /tileset_info/?d=<uid>``."""

    @pytest.mark.asyncio
    async def test_should_return_a_document_keyed_by_uid(
        self, tile_env, tiny_mcool
    ):
        """Test the response envelope and the injected fields.

        Given:
            A 4DN mcool whose tileset artifact is cached.
        When:
            tileset_info is requested for its uid.
        Then:
            The response should be keyed by the uid and carry the four
            fields clodius does not model — datatype, name, uuid, and
            coordSystem — alongside the resolution ladder.
        """
        # Arrange
        seed, mock_db, _ = tile_env
        doc = _file_doc()
        mock_db.file.docs = [doc]
        await seed(doc, tiny_mcool)

        # Act
        response = await tileset_info(d=[UID])

        # Assert
        info = response[UID]
        assert info["datatype"] == "matrix"
        assert info["name"] == "sample.mcool"
        assert info["uuid"] == UID
        assert info["coordSystem"] == "GRCh38"
        assert info["resolutions"] == [1, 2, 4]

    @pytest.mark.asyncio
    async def test_should_404_a_contact_map_whose_artifact_is_absent(
        self, tile_env
    ):
        """Test the not-yet-tileable answer.

        Given:
            A contact map with nothing in the cache.
        When:
            tileset_info is requested.
        Then:
            It should raise 404 and dispatch nothing. A heatmap track has
            nowhere to put a job id, so there is no useful 202 here — the
            preparation channel owns building the artifact.
        """
        # Arrange
        _, mock_db, _ = tile_env
        mock_db.file.docs = [_file_doc()]

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_info(d=[UID])
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_should_404_a_file_that_is_not_a_contact_map(self, tile_env):
        """Test that a non-matrix file is permanently untileable.

        Given:
            An AnnData matrix, which shares the HDF5 EDAM term.
        When:
            tileset_info is requested.
        Then:
            It should raise 404.
        """
        # Arrange
        _, mock_db, _ = tile_env
        mock_db.file.docs = [_file_doc(filename="matrix.h5ad")]

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_info(d=[UID])
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_should_403_a_non_public_hubmap_file(self, tile_env):
        """Test that the access guard covers this new egress path.

        Given:
            A non-public HuBMAP contact map.
        When:
            tileset_info is requested.
        Then:
            It should raise 403. These endpoints are a new way for bytes
            to leave the service, so they run the same guard /data and
            /index do.
        """
        # Arrange
        _, mock_db, _ = tile_env
        mock_db.file.docs = [
            _file_doc(
                submission="hubmap",
                local_id="HBM123",
                data_access_level="consortium",
            )
        ]

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_info(d=["hubmap/HBM123"])
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_should_400_an_unknown_dcc(self, tile_env):
        """Test that an unrecognized DCC is rejected.

        Given:
            A uid naming a DCC that does not exist.
        When:
            tileset_info is requested.
        Then:
            It should raise 400, mirroring /data and /index.
        """
        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_info(d=["nosuchdcc/abc"])
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_should_render_a_failed_sibling_as_an_error_object(
        self, tile_env, tiny_mcool
    ):
        """Test that a batch does not discard the datasets that worked.

        Given:
            Two uids, one cached and one not.
        When:
            tileset_info is requested for both.
        Then:
            The cached one should return its document and the other an
            error object — rather than the whole request 404ing and
            throwing away work that succeeded. A single-uid request, which
            is what a Gosling matrix track sends, still gets a clean 404.
        """
        # Arrange
        seed, mock_db, _ = tile_env
        ready = _file_doc()
        missing = _file_doc(local_id="4DNFIXYZ789", md5="0" * 32)
        mock_db.file.docs = [ready, missing]
        await seed(ready, tiny_mcool)

        # Act
        response = await tileset_info(d=[UID, "4dn/4DNFIXYZ789"])

        # Assert
        assert response[UID]["datatype"] == "matrix"
        assert "error" in response["4dn/4DNFIXYZ789"]

    _completeness_salt = itertools.count()

    @given(data=st.data())
    @settings(
        max_examples=8,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_should_answer_every_uid_of_a_wellformed_batch(
        self, tile_env, tiny_mcool, tmp_path, data
    ):
        """Test the completeness invariant of a tileset_info batch.

        Given:
            A batch of 2-8 well-formed uids, each resolving to a contact
            map document, with a generated subset holding a cached
            artifact.
        When:
            tileset_info is requested for the whole batch.
        Then:
            It should key the response by exactly the requested uids —
            an info document for every cached dataset and an error object
            for every uncached one, never omitting a uid the way /tiles
            omits an out-of-ladder position.
        """
        # Arrange
        _, mock_db, _ = tile_env
        salt = next(self._completeness_salt)
        size = data.draw(st.integers(min_value=2, max_value=8), label="batch size")
        seeded_mask = data.draw(
            st.lists(st.booleans(), min_size=size, max_size=size),
            label="has artifact",
        )
        docs = [
            _file_doc(
                local_id=f"4DNFIP{salt}N{index}",
                md5=f"{salt * 4096 + index:032x}",
            )
            for index in range(size)
        ]
        mock_db.file.docs = docs
        uids = [f"4dn/{doc['local_id']}" for doc in docs]

        async def scenario():
            cache = LocalFsCache(tmp_path / f"p2-cache-{salt}")
            service = build_service(
                root=tmp_path / f"p2-tilesets-{salt}",
                cache_provider=lambda: cache,
                disk_cache_bytes=10**9,
                open_max=4,
                tile_cache_bytes=10**7,
                threads=2,
                hydrate_timeout_s=30,
            )
            api.tileset_service = service
            try:
                processor = MatrixTilesetProcessor()
                for doc, is_seeded in zip(docs, seeded_mask):
                    if not is_seeded:
                        continue
                    key = processor.cache_key_for(doc, ArtifactKind.TILESET)
                    staged = tmp_path / f"p2-staged-{key.replace('/', '_')}"
                    staged.write_bytes(tiny_mcool.read_bytes())
                    await cache.put(key, staged)
                # Act
                return await tileset_info(d=uids)
            finally:
                await service.aclose()

        response = asyncio.run(scenario())

        # Assert
        assert set(response) == set(uids)
        for uid, is_seeded in zip(uids, seeded_mask):
            if is_seeded:
                assert response[uid]["resolutions"] == [1, 2, 4]
            else:
                assert "error" in response[uid]


class TestTiles:
    """``GET /tiles/?d=<uid>.<z>.<x>.<y>``."""

    @pytest.mark.asyncio
    async def test_should_return_a_dense_payload_keyed_by_request_string(
        self, tile_env, tiny_mcool
    ):
        """Test the tile response envelope.

        Given:
            A cached contact map.
        When:
            One tile is requested.
        Then:
            The response should be keyed by the exact request string and
            carry a base64 dense block that decodes to 256x256 — clodius
            emits no ``shape`` field, so the client recovers the side as
            sqrt(len).
        """
        # Arrange
        seed, mock_db, _ = tile_env
        doc = _file_doc()
        mock_db.file.docs = [doc]
        await seed(doc, tiny_mcool)

        # Act
        response = await tiles(d=[f"{UID}.0.0.0"])

        # Assert
        payload = response[f"{UID}.0.0.0"]
        assert "shape" not in payload
        flat = np.frombuffer(base64.b64decode(payload["dense"]), payload["dtype"])
        assert len(flat) == 256 * 256

    @pytest.mark.asyncio
    async def test_should_report_an_out_of_ladder_tile_from_a_mixed_batch(
        self, tile_env, tiny_mcool
    ):
        """Test the one-entry-per-id contract at the HTTP boundary.

        Given:
            A batch pairing a valid tile with one past the ladder.
        When:
            tiles is requested.
        Then:
            Both ids should appear in the response — the valid tile with
            real data, and the out-of-ladder one with a per-tile error
            object. clodius returns exactly one entry per requested id, so
            a response assembled by zipping request against result is
            safe.
        """
        # Arrange
        seed, mock_db, _ = tile_env
        doc = _file_doc()
        mock_db.file.docs = [doc]
        await seed(doc, tiny_mcool)

        # Act
        response = await tiles(d=[f"{UID}.0.0.0", f"{UID}.9.0.0"])

        # Assert
        assert set(response) == {f"{UID}.0.0.0", f"{UID}.9.0.0"}
        assert "dense" in response[f"{UID}.0.0.0"]
        assert "error" in response[f"{UID}.9.0.0"]

    @pytest.mark.asyncio
    async def test_should_report_a_malformed_id_without_failing_the_batch(
        self, tile_env, tiny_mcool
    ):
        """Test the per-tile error convention.

        Given:
            A batch pairing a valid tile id with a malformed one.
        When:
            tiles is requested.
        Then:
            The malformed id should get an error object while its sibling
            returns data, and the request should still be a 200.
        """
        # Arrange
        seed, mock_db, _ = tile_env
        doc = _file_doc()
        mock_db.file.docs = [doc]
        await seed(doc, tiny_mcool)

        # Act
        response = await tiles(d=[f"{UID}.0.0.0", f"{UID}.notanint.0"])

        # Assert
        assert "error" in response[f"{UID}.notanint.0"]
        assert "dense" in response[f"{UID}.0.0.0"]

    @pytest.mark.asyncio
    async def test_should_404_every_tile_of_an_unbuilt_dataset(self, tile_env):
        """Test that an unbuilt dataset 404s rather than dispatching.

        Given:
            A contact map with no cached artifact.
        When:
            A tile is requested.
        Then:
            It should raise 404 and never reach the executor — which the
            fixture's executor stand-in would fail the test over.
        """
        # Arrange
        _, mock_db, _ = tile_env
        mock_db.file.docs = [_file_doc()]

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tiles(d=[f"{UID}.0.0.0"])
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_should_open_a_dataset_once_for_a_multi_tile_batch(
        self, tile_env, tiny_mcool, mocker
    ):
        """Test that tiles are grouped by dataset before being read.

        Given:
            Four tiles of one dataset in a single request.
        When:
            tiles is requested.
        Then:
            The service should be asked once, not four times. A heatmap
            track requests a screenful at a time, and h5py serializes on a
            global lock, so re-opening per tile would be the dominant
            cost.
        """
        # Arrange
        seed, mock_db, service = tile_env
        doc = _file_doc()
        mock_db.file.docs = [doc]
        await seed(doc, tiny_mcool)
        spy = mocker.spy(service, "tiles")

        # Act
        await tiles(
            d=[
                f"{UID}.2.0.0",
                f"{UID}.2.1.0",
                f"{UID}.2.0.1",
                f"{UID}.2.1.1",
            ]
        )

        # Assert
        spy.assert_called_once()

    @pytest.mark.asyncio
    async def test_should_503_the_whole_batch_when_hydration_times_out(
        self, tile_env, mocker
    ):
        """Test that an infrastructure failure is never a per-tile error.

        Given:
            Tile ids spanning two resolvable datasets, with the service
            raising TilesetHydrationTimeout.
        When:
            tiles is requested.
        Then:
            It should raise 503 with Retry-After for the whole request —
            a hydration timeout is a condition of the server, not a fact
            about one dataset, so it must not render as an error object
            beside a sibling that happened to work.
        """
        # Arrange
        _, mock_db, service = tile_env
        mock_db.file.docs = [
            _file_doc(),
            _file_doc(local_id="4DNFIXYZ789", md5="1" * 32),
        ]
        mocker.patch.object(
            service,
            "tiles",
            side_effect=TilesetHydrationTimeout("hydration exceeded its budget"),
        )

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tiles(d=[f"{UID}.0.0.0", "4dn/4DNFIXYZ789.0.0.0"])
        assert exc_info.value.status_code == 503
        assert exc_info.value.headers["Retry-After"] == "30"

    @pytest.mark.asyncio
    async def test_should_fan_one_error_object_across_an_unbuilt_datasets_ids(
        self, tile_env, tiny_mcool
    ):
        """Test the per-uid failure fan-out in a mixed batch.

        Given:
            Two datasets, one cached and one unbuilt, with two tile ids
            for the unbuilt one and one for the cached one.
        When:
            tiles is requested for all three ids.
        Then:
            The cached dataset's tile should return data untouched, and
            the same error object should sit under each of the unbuilt
            dataset's ids — the failure is per dataset, fanned out to
            every tile that named it.
        """
        # Arrange
        seed, mock_db, _ = tile_env
        ready = _file_doc()
        unbuilt = _file_doc(local_id="4DNFIXYZ789", md5="0" * 32)
        mock_db.file.docs = [ready, unbuilt]
        await seed(ready, tiny_mcool)
        unbuilt_ids = ["4dn/4DNFIXYZ789.0.0.0", "4dn/4DNFIXYZ789.1.0.0"]

        # Act
        response = await tiles(d=[*unbuilt_ids, f"{UID}.0.0.0"])

        # Assert
        assert "dense" in response[f"{UID}.0.0.0"]
        assert "error" in response[unbuilt_ids[0]]
        assert response[unbuilt_ids[0]] == response[unbuilt_ids[1]]

    @pytest.mark.asyncio
    async def test_should_404_the_whole_batch_when_a_uid_matches_no_document(
        self, tile_env, tiny_mcool
    ):
        """Test that database absence fails at the request level.

        Given:
            One cached dataset plus a uid that matches no document at
            all.
        When:
            tiles is requested for a tile of each.
        Then:
            It should raise 404 for the whole request — a uid with no
            document fails before the per-dataset error rendering, unlike
            an unbuilt dataset, which gets an error object beside its
            siblings. This asymmetry is pinned as current behavior.
        """
        # Arrange
        seed, mock_db, _ = tile_env
        ready = _file_doc()
        mock_db.file.docs = [ready]
        await seed(ready, tiny_mcool)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tiles(d=[f"{UID}.0.0.0", "4dn/4DNFINODOC01.0.0.0"])
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_should_group_interleaved_ids_by_dataset_in_first_seen_order(
        self, tile_env, tiny_mcool, mocker
    ):
        """Test the grouping contract for a multi-dataset batch.

        Given:
            Two cached datasets with their tile ids interleaved A,B,A,B.
        When:
            tiles is requested.
        Then:
            The service should be asked exactly twice — once per dataset,
            in first-seen order, each call carrying that dataset's ids in
            request order — and the response should be keyed by the exact
            raw ids.
        """
        # Arrange
        seed, mock_db, service = tile_env
        first = _file_doc()
        second = _file_doc(local_id="4DNFIDEF456", md5="f" * 32)
        mock_db.file.docs = [first, second]
        await seed(first, tiny_mcool)
        await seed(second, tiny_mcool)
        spy = mocker.spy(service, "tiles")
        second_uid = "4dn/4DNFIDEF456"
        ids = [
            f"{UID}.0.0.0",
            f"{second_uid}.0.0.0",
            f"{UID}.1.0.0",
            f"{second_uid}.1.0.0",
        ]

        # Act
        response = await tiles(d=ids)

        # Assert
        assert spy.call_count == 2
        (first_call, second_call) = spy.call_args_list
        assert first_call.args[0] == UID
        assert first_call.args[2] == [f"{UID}.0.0.0", f"{UID}.1.0.0"]
        assert second_call.args[0] == second_uid
        assert second_call.args[2] == [
            f"{second_uid}.0.0.0",
            f"{second_uid}.1.0.0",
        ]
        assert set(response) == set(ids)

    @pytest.mark.asyncio
    async def test_should_404_a_multi_tile_request_for_one_unbuilt_dataset(
        self, tile_env
    ):
        """Test that the solo pivot counts datasets, not tile ids.

        Given:
            Several tile ids all naming one unbuilt dataset.
        When:
            tiles is requested.
        Then:
            It should raise 404 for the whole request — the solo-vs-batch
            pivot keys on the number of distinct uids, not the number of
            ids, so three tiles of one dataset still get the clean 404 a
            Gosling matrix track expects.
        """
        # Arrange
        _, mock_db, _ = tile_env
        mock_db.file.docs = [_file_doc()]

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tiles(d=[f"{UID}.0.0.0", f"{UID}.1.0.0", f"{UID}.2.0.0"])
        assert exc_info.value.status_code == 404


class TestRequestValidation:
    """Guards on the query parameters themselves."""

    @pytest.mark.asyncio
    async def test_should_400_a_request_with_no_ids(self, tile_env):
        """Test that a bare request is rejected.

        Given:
            No ``d`` parameter.
        When:
            tiles is requested.
        Then:
            It should raise 400 rather than returning an empty object,
            which would look like a successful read of nothing.
        """
        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tiles(d=[])
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_should_400_a_batch_beyond_the_ceiling(self, tile_env):
        """Test the cap on tile ids per request.

        Given:
            More tile ids than the ceiling allows.
        When:
            tiles is requested.
        Then:
            It should raise 400. Without a cap, one unauthenticated
            request turns into arbitrarily many h5py reads.
        """
        # Arrange
        ids = [f"{UID}.0.0.{i}" for i in range(MAX_TILE_IDS + 1)]

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tiles(d=ids)
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_should_admit_a_batch_at_exactly_the_ceiling(
        self, tile_env, tiny_mcool
    ):
        """Test that the tile-id cap is inclusive.

        Given:
            A cached contact map and exactly MAX_TILE_IDS tile ids.
        When:
            tiles is requested.
        Then:
            It should be admitted rather than 400ed — the guard rejects
            strictly more than the ceiling — and the in-ladder tile
            should come back as data.
        """
        # Arrange
        seed, mock_db, _ = tile_env
        doc = _file_doc()
        mock_db.file.docs = [doc]
        await seed(doc, tiny_mcool)
        ids = [f"{UID}.0.{i}.0" for i in range(MAX_TILE_IDS)]

        # Act
        response = await tiles(d=ids)

        # Assert
        assert "dense" in response[f"{UID}.0.0.0"]
        assert set(response) <= set(ids)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "uid",
        [
            "4DNFIABC123",
            "4dn/",
            "/4DNFIABC123",
            "4dn/a/b",
            "4dn/has.dot",
            "4dn/" + "a" * (PATH_PARAM_MAX_LEN + 1),
            "4dn/bad!id",
        ],
        ids=[
            "no-slash",
            "no-local-id",
            "no-dcc",
            "two-slashes",
            "dotted-id",
            "over-length",
            "illegal-char",
        ],
    )
    async def test_should_400_a_malformed_uid(self, tile_env, uid):
        """Test uid shape validation, including the dot guard.

        Given:
            A uid that is not ``<dcc>/<local_id>``, or whose local id
            contains a dot, exceeds the path-param length cap, or carries
            a character outside the allowed set.
        When:
            tileset_info is requested.
        Then:
            It should raise 400. The dot case matters specifically:
            clodius parses a tile id by splitting on ``.`` and taking the
            first field as the uid, so a dotted identifier would be
            silently read as a uid plus a zoom level.
        """
        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_info(d=[uid])
        assert exc_info.value.status_code == 400

    @given(uid=_malformed_uids())
    @settings(
        max_examples=50,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_should_400_every_malformed_uid_before_any_lookup(
        self, tile_env, uid
    ):
        """Test the totality of uid rejection.

        Given:
            Generated uid strings each violating one shape rule — no
            slash, an empty half, a dotted part, a part past the length
            cap, or an illegal character.
        When:
            tileset_info is requested.
        Then:
            It should raise exactly 400 from the uid validator — never a
            500, and never reaching the database or the service, which
            the stand-in service would fail the test over.
        """
        # Arrange
        _, mock_db, _ = tile_env
        mock_db.file.docs = []
        api.tileset_service = _FailIfConsulted()

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(tileset_info(d=[uid]))
        assert exc_info.value.status_code == 400
        assert "Malformed tileset uid" in exc_info.value.detail


class TestHicIsDeferred:
    """ENCODE .hic is recognized, and specifically refused."""

    @pytest.mark.asyncio
    async def test_should_501_a_hic_rather_than_404_it(self, tile_env):
        """Test that .hic is refused as unimplemented, not as unknown.

        Given:
            An ENCODE .hic — a genuine contact map, but one cfdb cannot
            serve: the clodius tileset is built on hictkpy, which reads
            local paths only, and cfdb does not materialize .hic
            artifacts because they run to hundreds of GB.
        When:
            tileset_info is requested.
        Then:
            It should raise 501, not the 404 an unrecognized format gets.
            The distinction is the whole reason MatrixSource still carries
            a HIC member: "we do not support this yet" and "this is not a
            contact map" are different answers, and a client can act on
            the first.
        """
        # Arrange
        _, mock_db, _ = tile_env
        mock_db.file.docs = [
            _file_doc(
                submission="encode", local_id="ENCFF123ABC", filename="contact.hic"
            )
        ]

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_info(d=["encode/ENCFF123ABC"])
        assert exc_info.value.status_code == 501
        assert ".hic" in exc_info.value.detail


class TestInfrastructureFailures:
    """The ``_reraise_infrastructure`` ladder: server conditions, not facts.

    A missing backend and a disk-budget or hydration problem describe the
    server, so they fail the whole request as a status code — never an
    error object rendered beside a sibling uid that happened to work.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("exc", "expected_status", "retry_after"),
        [
            (TilesetHydrationTimeout("hydration exceeded its budget"), 503, "30"),
            (TilesetTooLarge("artifact exceeds the disk budget"), 503, "30"),
            (TileBackendUnavailable("this build carries no tile backend"), 501, None),
        ],
        ids=["hydration-timeout", "too-large", "backend-unavailable"],
    )
    async def test_should_fail_the_whole_batch_on_an_infrastructure_error(
        self, tile_env, mocker, exc, expected_status, retry_after
    ):
        """Test the infrastructure-error ladder against a healthy sibling.

        Given:
            A two-uid batch whose first dataset answers cleanly and whose
            second raises an infrastructure error — a hydration timeout,
            an over-budget artifact, or a missing backend.
        When:
            tileset_info is requested for both.
        Then:
            It should raise for the whole request — 503 with Retry-After
            for the operator conditions, 501 for the missing backend —
            rather than rendering a per-entry error object beside the
            healthy sibling.
        """
        # Arrange
        _, mock_db, service = tile_env
        mock_db.file.docs = [
            _file_doc(),
            _file_doc(local_id="4DNFIXYZ789", md5="1" * 32),
        ]
        mocker.patch.object(
            service,
            "tileset_info",
            side_effect=[{"datatype": "matrix"}, exc],
        )

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_info(d=[UID, "4dn/4DNFIXYZ789"])
        assert exc_info.value.status_code == expected_status
        assert (exc_info.value.headers or {}).get("Retry-After") == retry_after


class TestOverHttp:
    """The wire layer, which calling the handlers directly cannot exercise.

    The repeatable ``d`` parameter and the trailing slash are both parts of
    the HiGlass protocol that a client constructs rather than cfdb, so they
    are only really tested through ASGI. Follows ``tests/test_cors.py`` in
    neutering the lifespan.
    """

    @pytest.fixture()
    def client(self, mocker):
        from mongomock_motor import AsyncMongoMockClient
        from starlette.testclient import TestClient

        from cfdb.api import main

        mocker.patch.object(
            main, "create_mongodb_client", return_value=AsyncMongoMockClient()
        )
        mocker.patch.object(main.WorkflowProfile, "from_env", return_value=None)
        with TestClient(main.app) as client:
            yield client

    @pytest.mark.parametrize(
        "url",
        [
            "/tileset_info/?d=4dn/ABC&d=4dn/DEF",
            "/tileset_info?d=4dn/ABC",
            "/tiles/?d=4dn/ABC.0.0.0&d=4dn/ABC.0.1.0",
            "/tiles?d=4dn/ABC.0.0.0",
        ],
    )
    def test_should_accept_repeated_d_parameters_with_or_without_a_slash(
        self, client, url
    ):
        """Test that the HiGlass request shape reaches the handler.

        Given:
            URLs in the shape a HiGlass client builds — repeated ``d``
            parameters, with and without the trailing slash.
        When:
            They are requested over HTTP.
        Then:
            They should reach the handler rather than 404ing on routing or
            422ing on parameter parsing. Here the lifespan is disabled, so
            the handler answers 503; what matters is that it was reached.
        """
        # Act
        response = client.get(url)

        # Assert
        assert response.status_code == 503

    def test_should_400_a_request_carrying_no_d_parameter(self, client):
        """Test that the empty-request guard survives the wire layer.

        Given:
            A tileset_info URL with no ``d`` parameter at all.
        When:
            It is requested over HTTP.
        Then:
            It should be a 400 rather than an empty 200, which would look
            like a successful read of nothing.
        """
        # Act
        response = client.get("/tileset_info/")

        # Assert
        assert response.status_code == 400


class TestDegradedModes:
    """What the routes say when there is no service at all."""

    @pytest.mark.asyncio
    async def test_should_501_when_the_tile_backend_is_absent(
        self, tile_env, mocker
    ):
        """Test the answer for an image built without clodius.

        Given:
            No tile service, and no clodius to build one from — the state
            of every image built today, since the fork is private.
        When:
            tileset_info is requested.
        Then:
            It should raise 501. The subsystem degrades to a clear "not
            implemented here" rather than half-working.
        """
        # Arrange
        mocker.patch.object(api, "tileset_service", None)
        mocker.patch.object(backend, "is_available", return_value=False)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_info(d=[UID])
        assert exc_info.value.status_code == 501

    @pytest.mark.asyncio
    async def test_should_503_when_the_workflow_subsystem_is_disabled(
        self, tile_env, mocker
    ):
        """Test the answer when there is no artifact cache to read.

        Given:
            clodius present but no tile service, as when SYNC_DATA_DIR is
            unset and no cache was built.
        When:
            tileset_info is requested.
        Then:
            It should raise 503 with Retry-After — a deployment
            configuration problem, distinct from an unsupported build.
        """
        # Arrange
        mocker.patch.object(api, "tileset_service", None)
        mocker.patch.object(backend, "is_available", return_value=True)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_info(d=[UID])
        assert exc_info.value.status_code == 503
        assert exc_info.value.headers["Retry-After"] == "30"
