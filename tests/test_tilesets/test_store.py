"""Tests for :class:`cfdb.tilesets.store.LocalTilesetStore`."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

from cfdb.tilesets.errors import TilesetTooLarge
from cfdb.tilesets.store import LocalTilesetStore
from cfdb.workflows.cache import LocalFsCache
from tests.fixtures.remote_cache import FakeRemoteCache


class TestLocalFsFastPath:
    """A local artifact is already a file; opening it in place is the point."""

    @pytest.mark.asyncio
    async def test_should_return_the_cache_path_without_copying(self, tmp_path):
        """Test that a LocalFsCache artifact is not duplicated on disk.

        Given:
            A LocalFsCache holding an artifact.
        When:
            path_for is called.
        Then:
            It should return the cache's own path and hydrate nothing.
            Copying would double the disk cost of every dataset for no
            gain, and it is this fast path that lets the whole tile
            subsystem be tested without an S3 stand-in.
        """
        # Arrange
        cache = LocalFsCache(tmp_path / "cache")
        source = tmp_path / "artifact.mcool"
        source.write_bytes(b"payload")
        await cache.put("4dn/x/tileset/abc-v0", source)
        store = LocalTilesetStore(tmp_path / "local", max_bytes=1024)

        # Act
        path = await store.path_for(
            cache, "4dn/x/tileset/abc-v0", expected_size=7
        )

        # Assert
        assert path == cache.path_for("4dn/x/tileset/abc-v0")
        assert store.resident_bytes == 0

    @pytest.mark.asyncio
    async def test_should_serve_a_local_artifact_larger_than_the_budget(
        self, tmp_path
    ):
        """Test that the disk budget does not apply to in-place artifacts.

        Given:
            A LocalFsCache artifact larger than the store's entire
            max_bytes budget.
        When:
            path_for is called.
        Then:
            It should return the in-place path without raising
            TilesetTooLarge — the budget governs hydrated copies only,
            and an artifact opened where it already lives costs the
            store no disk at all.
        """
        # Arrange
        cache = LocalFsCache(tmp_path / "cache")
        source = tmp_path / "artifact.mcool"
        source.write_bytes(b"x" * 100)
        await cache.put("4dn/x/tileset/abc-v0", source)
        store = LocalTilesetStore(tmp_path / "local", max_bytes=10)

        # Act
        path = await store.path_for(
            cache, "4dn/x/tileset/abc-v0", expected_size=100
        )

        # Assert
        assert path == cache.path_for("4dn/x/tileset/abc-v0")
        assert path.read_bytes() == b"x" * 100
        assert store.resident_bytes == 0


class TestHydration:
    """A remote artifact is pulled down once and reused."""

    @pytest.mark.asyncio
    async def test_should_hydrate_a_remote_artifact_onto_local_disk(
        self, tmp_path
    ):
        """Test that a non-local cache is downloaded before being opened.

        Given:
            A cache backend whose artifacts are not local files.
        When:
            path_for is called.
        Then:
            It should write the bytes to local disk and return that path,
            because h5py opens a file rather than consuming a stream.
        """
        # Arrange
        cache = FakeRemoteCache({"k": b"contents"})
        store = LocalTilesetStore(tmp_path / "local", max_bytes=1024)

        # Act
        path = await store.path_for(cache, "k", expected_size=8)

        # Assert
        assert path.read_bytes() == b"contents"
        assert store.resident_bytes == 8

    @pytest.mark.asyncio
    async def test_should_reuse_a_hydrated_artifact(self, tmp_path):
        """Test that a second request does not re-download.

        Given:
            An artifact already hydrated.
        When:
            path_for is called again.
        Then:
            It should return the resident path without touching the cache
            a second time.
        """
        # Arrange
        cache = FakeRemoteCache({"k": b"contents"})
        store = LocalTilesetStore(tmp_path / "local", max_bytes=1024)
        await store.path_for(cache, "k", expected_size=8)

        # Act
        await store.path_for(cache, "k", expected_size=8)

        # Assert
        assert cache.get_calls == ["k"]

    @pytest.mark.asyncio
    async def test_should_download_once_under_concurrent_requests(
        self, tmp_path
    ):
        """Test the single-flight guard around a cold dataset.

        Given:
            Two concurrent requests for an artifact that is not yet
            resident, with the download held open.
        When:
            Both call path_for.
        Then:
            Exactly one download should run. A heatmap track opens with a
            burst of tile requests, so without this a cold multi-GB
            dataset would be pulled down once per request in the burst.
        """
        # Arrange
        cache = FakeRemoteCache({"k": b"contents"})
        cache.gate = asyncio.Event()
        store = LocalTilesetStore(tmp_path / "local", max_bytes=1024)

        # Act
        first = asyncio.create_task(store.path_for(cache, "k", expected_size=8))
        second = asyncio.create_task(store.path_for(cache, "k", expected_size=8))
        await asyncio.sleep(0)
        cache.gate.set()
        paths = await asyncio.gather(first, second)

        # Assert
        assert cache.get_calls == ["k"]
        assert paths[0] == paths[1]

    @pytest.mark.asyncio
    async def test_should_not_leave_a_partial_file_behind_on_failure(
        self, tmp_path
    ):
        """Test that an interrupted download cannot be mistaken for complete.

        Given:
            A cache whose read raises partway through.
        When:
            path_for is called.
        Then:
            It should propagate and leave nothing resident, so a later
            request re-downloads rather than handing h5py a truncated
            file.
        """
        # Arrange
        class _Exploding(FakeRemoteCache):
            def get(self, key, byte_range=None):
                async def _stream():
                    raise OSError("connection reset")
                    yield b""  # pragma: no cover

                return _stream()

        cache = _Exploding({"k": b"contents"})
        store = LocalTilesetStore(tmp_path / "local", max_bytes=1024)

        # Act
        with pytest.raises(OSError):
            await store.path_for(cache, "k", expected_size=8)

        # Assert
        assert store.resident_bytes == 0
        assert list((tmp_path / "local").iterdir()) == []

    @pytest.mark.asyncio
    async def test_should_refuse_an_artifact_larger_than_the_whole_budget(
        self, tmp_path
    ):
        """Test that one oversized artifact fails instead of thrashing.

        Given:
            An artifact bigger than the entire local disk budget.
        When:
            path_for is called.
        Then:
            It should raise TilesetTooLarge rather than evicting
            everything and still not fitting.
        """
        # Arrange
        cache = FakeRemoteCache({"k": b"x" * 100})
        store = LocalTilesetStore(tmp_path / "local", max_bytes=10)

        # Act
        with pytest.raises(TilesetTooLarge):
            await store.path_for(cache, "k", expected_size=100)

        # Assert
        assert store.resident_bytes == 0

    @pytest.mark.asyncio
    async def test_should_rehydrate_when_the_resident_file_is_gone(
        self, tmp_path
    ):
        """Test that residency is revalidated against the filesystem.

        Given:
            A hydrated artifact whose file was deleted out-of-band.
        When:
            path_for is called again.
        Then:
            It should re-download and return a fresh readable path
            rather than handing h5py a path that no longer exists.
        """
        # Arrange
        cache = FakeRemoteCache({"k": b"contents"})
        store = LocalTilesetStore(tmp_path / "local", max_bytes=1024)
        first = await store.path_for(cache, "k", expected_size=8)
        first.unlink()

        # Act
        second = await store.path_for(cache, "k", expected_size=8)

        # Assert
        assert second.read_bytes() == b"contents"
        assert cache.get_calls == ["k", "k"]

    @pytest.mark.asyncio
    async def test_should_leave_no_residue_when_cancelled_mid_download(
        self, tmp_path
    ):
        """Test that a cancelled hydration cleans up after itself.

        Given:
            A path_for task cancelled while its download is blocked
            mid-stream.
        When:
            The task is cancelled and awaited.
        Then:
            It should leave no .part residue and an empty store
            directory, with resident_bytes at zero — cancellation runs
            the same cleanup as any other failure, so a timed-out
            request cannot poison the key.
        """
        # Arrange
        cache = FakeRemoteCache({"k": b"contents"})
        cache.gate = asyncio.Event()
        store = LocalTilesetStore(tmp_path / "local", max_bytes=1024)
        task = asyncio.create_task(
            store.path_for(cache, "k", expected_size=8)
        )
        for _ in range(200):
            if any((tmp_path / "local").iterdir()):
                break
            await asyncio.sleep(0.005)
        else:  # pragma: no cover - diagnostic guard
            pytest.fail("download never reached the gated stream")

        # Act
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # Assert
        assert list((tmp_path / "local").iterdir()) == []
        assert store.resident_bytes == 0

    @pytest.mark.asyncio
    async def test_should_recover_after_a_failed_hydration(self, tmp_path):
        """Test that one failed download does not poison the key's lock.

        Given:
            A cache whose first read fails with OSError and whose
            subsequent reads succeed.
        When:
            path_for is retried after the failure.
        Then:
            It should succeed and return the full bytes — the per-key
            single-flight lock is released on failure rather than left
            held or wedged in a failed state.
        """
        # Arrange
        class _FlakyOnce(FakeRemoteCache):
            def __init__(self, blobs):
                super().__init__(blobs)
                self.failed = False

            def get(self, key, byte_range=None):
                if not self.failed:
                    self.failed = True

                    async def _stream():
                        raise OSError("connection reset")
                        yield b""  # pragma: no cover

                    return _stream()
                return super().get(key, byte_range)

        cache = _FlakyOnce({"k": b"contents"})
        store = LocalTilesetStore(tmp_path / "local", max_bytes=1024)
        with pytest.raises(OSError):
            await store.path_for(cache, "k", expected_size=8)

        # Act
        path = await store.path_for(cache, "k", expected_size=8)

        # Assert
        assert path.read_bytes() == b"contents"
        assert store.resident_bytes == 8

    @pytest.mark.asyncio
    async def test_should_admit_at_the_budget_and_refuse_one_byte_over(
        self, tmp_path
    ):
        """Test that the whole-budget guard is strictly greater-than.

        Given:
            Artifacts sized exactly at max_bytes and at max_bytes + 1.
        When:
            path_for is called for each against a fresh store.
        Then:
            It should admit the exact-fit artifact and raise
            TilesetTooLarge only for the one byte over — an artifact
            that fills the budget precisely is still servable.
        """
        # Arrange
        cache = FakeRemoteCache({"fit": b"a" * 10, "over": b"b" * 11})
        exact = LocalTilesetStore(tmp_path / "exact", max_bytes=10)
        over = LocalTilesetStore(tmp_path / "over", max_bytes=10)

        # Act
        path = await exact.path_for(cache, "fit", expected_size=10)
        with pytest.raises(TilesetTooLarge):
            await over.path_for(cache, "over", expected_size=11)

        # Assert
        assert path.read_bytes() == b"a" * 10
        assert exact.resident_bytes == 10
        assert over.resident_bytes == 0

    @settings(
        max_examples=25,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        keys=st.lists(
            st.text(
                alphabet=st.sampled_from(list("ab_./\\-é🧬")),
                min_size=1,
                max_size=20,
            ),
            unique=True,
            min_size=2,
            max_size=6,
        )
    )
    @example(keys=["a/b", "a_b", "..", "é"])
    @pytest.mark.asyncio
    async def test_should_map_distinct_keys_to_distinct_paths_inside_root(
        self, tmp_path, keys
    ):
        """Test that local naming is injective and contained.

        Given:
            Generated sets of distinct path-ish cache keys — slashes,
            dots, backslashes, unicode — each with its own content.
        When:
            Every key is hydrated through path_for.
        Then:
            It should return pairwise-distinct paths, all strictly
            inside the store root, each reading back its own blob —
            keys like "a/b" and "a_b" may never collide, and no key
            shape may escape the root.
        """
        # Arrange
        root = tmp_path / uuid4().hex
        blobs = {key: key.encode() for key in keys}
        cache = FakeRemoteCache(blobs)
        store = LocalTilesetStore(root, max_bytes=1 << 20)

        # Act
        paths = {
            key: await store.path_for(
                cache, key, expected_size=len(blobs[key])
            )
            for key in keys
        }

        # Assert
        assert len(set(paths.values())) == len(keys)
        for key, path in paths.items():
            assert path.is_relative_to(root)
            assert path.read_bytes() == blobs[key]


#: Byte budget shared by the eviction property and its strategy, so every
#: generated artifact is guaranteed to fit the budget on its own.
_PROPERTY_BUDGET = 20


@st.composite
def _access_sequences(draw):
    """Draw an access sequence plus a fixed per-key artifact size."""
    sequence = draw(
        st.lists(
            st.sampled_from(["a", "b", "c", "d", "e"]),
            min_size=1,
            max_size=12,
        )
    )
    sizes = {
        key: draw(st.integers(min_value=1, max_value=_PROPERTY_BUDGET))
        for key in sorted(set(sequence))
    }
    return sequence, sizes


class TestEviction:
    """The disk budget is enforced, but never over a live handle."""

    @pytest.mark.asyncio
    async def test_should_evict_least_recently_used_to_make_room(self, tmp_path):
        """Test that the budget is enforced in LRU order.

        Given:
            Two resident artifacts filling the budget, with the first
            touched more recently than the second.
        When:
            A third is hydrated.
        Then:
            The least recently used should be unlinked.
        """
        # Arrange
        cache = FakeRemoteCache({"a": b"1" * 10, "b": b"2" * 10, "c": b"3" * 10})
        store = LocalTilesetStore(tmp_path / "local", max_bytes=20)
        path_a = await store.path_for(cache, "a", expected_size=10)
        await store.path_for(cache, "b", expected_size=10)
        await store.path_for(cache, "a", expected_size=10)  # touch a

        # Act
        await store.path_for(cache, "c", expected_size=10)

        # Assert
        assert path_a.exists()
        assert store.resident_bytes == 20

    @pytest.mark.asyncio
    async def test_should_not_unlink_an_artifact_that_is_in_use(self, tmp_path):
        """Test the guard that keeps eviction from segfaulting a reader.

        Given:
            A resident artifact whose release hook reports it is in use.
        When:
            Room is needed for another artifact.
        Then:
            It should be left on disk and the incoming hydration should
            still succeed, overshooting the budget rather than failing
            the request. Unlinking a file whose h5py handle is live, or
            closing that handle under a numpy read running in a worker
            thread, is a segfault rather than an exception — so this is
            a correctness guard, not an optimization.
        """
        # Arrange
        cache = FakeRemoteCache({"a": b"1" * 10, "b": b"2" * 10})
        store = LocalTilesetStore(tmp_path / "local", max_bytes=10)

        async def _pinned(_key: str) -> bool:
            return False

        store.set_release_hook(_pinned)
        path_a = await store.path_for(cache, "a", expected_size=10)

        # Act
        path_b = await store.path_for(cache, "b", expected_size=10)

        # Assert
        assert path_a.exists()
        assert path_b.read_bytes() == b"2" * 10
        # 20 resident bytes against a 10-byte budget: when every resident
        # is pinned, the documented contract is overshoot, not failure.
        assert store.resident_bytes == 20

    @pytest.mark.asyncio
    async def test_should_consult_the_release_hook_before_unlinking(
        self, tmp_path
    ):
        """Test that eviction gives the service a chance to close first.

        Given:
            A release hook that records which keys it was asked about.
        When:
            An eviction is required.
        Then:
            The hook should have been asked about the victim before the
            file was removed.
        """
        # Arrange
        cache = FakeRemoteCache({"a": b"1" * 10, "b": b"2" * 10})
        store = LocalTilesetStore(tmp_path / "local", max_bytes=10)
        asked: list[str] = []

        async def _hook(key: str) -> bool:
            asked.append(key)
            return True

        store.set_release_hook(_hook)
        path_a = await store.path_for(cache, "a", expected_size=10)

        # Act
        await store.path_for(cache, "b", expected_size=10)

        # Assert
        assert asked == ["a"]
        assert not path_a.exists()

    @pytest.mark.asyncio
    async def test_aclose_should_drop_every_hydrated_file(self, tmp_path):
        """Test that teardown does not leak the API task's disk.

        Given:
            Hydrated artifacts on local disk.
        When:
            aclose runs, as it does on lifespan teardown.
        Then:
            The files should be removed. They are a cache of a cache, and
            their hashed names carry no expiry of their own, so leaving
            them would accumulate across restarts.
        """
        # Arrange
        cache = FakeRemoteCache({"a": b"1" * 10})
        store = LocalTilesetStore(tmp_path / "local", max_bytes=100)
        path = await store.path_for(cache, "a", expected_size=10)

        # Act
        await store.aclose()

        # Assert
        assert not path.exists()
        assert store.resident_bytes == 0

    @settings(
        max_examples=25,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(accesses=_access_sequences())
    @pytest.mark.asyncio
    async def test_should_hold_resident_bytes_within_budget_for_any_sequence(
        self, tmp_path, accesses
    ):
        """Test the LRU byte-budget invariant over arbitrary access orders.

        Given:
            Generated access sequences over keys whose artifacts each
            fit the budget, with every resident releasable.
        When:
            The sequence is replayed through path_for.
        Then:
            It should keep resident_bytes within max_bytes after every
            call, and every returned path should hold its artifact's
            exact bytes — no interleaving of hits, misses, and
            evictions may overshoot the budget or serve a stale file.
        """
        # Arrange
        sequence, sizes = accesses
        blobs = {key: key.encode() * size for key, size in sizes.items()}
        cache = FakeRemoteCache(blobs)
        store = LocalTilesetStore(
            tmp_path / uuid4().hex, max_bytes=_PROPERTY_BUDGET
        )

        # Act & assert
        for key in sequence:
            path = await store.path_for(
                cache, key, expected_size=sizes[key]
            )
            assert path.read_bytes() == blobs[key]
            assert store.resident_bytes <= _PROPERTY_BUDGET
