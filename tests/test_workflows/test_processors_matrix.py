"""Tests for :class:`cfdb.workflows.processors.matrix.MatrixTilesetProcessor`."""

from __future__ import annotations

import uuid
from pathlib import Path, PurePosixPath

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.events import Complete, StageComplete
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors import matrix as matrix_module
from cfdb.workflows.processors.matrix import MatrixTilesetProcessor
from tests.fixtures.coolers import build_cool, build_variable_bin_cool
from tests.test_workflows import FIXTURE_MD5


def _file_doc(filename: str, **overrides) -> dict:
    """Return a projected file document for an HDF5-labelled file."""
    doc = {
        "submission": "4dn",
        "local_id": "4DNFIABC123",
        "md5": FIXTURE_MD5,
        "filename": filename,
        "file_format": {"name": "HDF5"},
        "access_url": "https://data.4dnucleome.org/files/" + filename,
        "dcc": {"dcc_abbreviation": "4DN_DCIC"},
    }
    doc.update(overrides)
    return doc


async def _drain(processor, file_doc, workdir, cache) -> list:
    """Collect the processor's whole event stream."""
    return [event async for event in processor.run(file_doc, workdir, cache)]


def _resolutions_of(path: Path) -> list[int]:
    """Read the sorted resolution ladder out of a committed artifact."""
    import h5py

    with h5py.File(path, "r") as handle:
        if "resolutions" not in handle:
            return []
        return sorted(int(name) for name in handle["resolutions"])


#: Suffixes sharing the HDF5 EDAM term upstream, plus non-claimed noise.
_SUFFIX_POOL = (".mcool", ".cool", ".hic", ".h5ad", ".h5", ".txt", "")


@st.composite
def _hdf5_filenames(draw) -> str:
    """Generate a filename from a free-text stem and a randomly-cased suffix."""
    stem = draw(st.text(max_size=16))
    suffix = draw(st.sampled_from(_SUFFIX_POOL))
    flips = draw(
        st.lists(st.booleans(), min_size=len(suffix), max_size=len(suffix))
    )
    cased = "".join(
        char.upper() if flip else char.lower()
        for char, flip in zip(suffix, flips)
    )
    return stem + cased


class TestApplicability:
    """Which HDF5 files this processor claims, and what it advertises."""

    @pytest.mark.parametrize(
        ("filename", "expected"),
        [
            ("sample.mcool", True),
            ("sample.cool", True),
            ("sample.hic", False),
            ("matrix.h5ad", False),
            ("raw.h5", False),
        ],
    )
    def test_needs_processing_should_claim_only_coolers(self, filename, expected):
        """Test that only the formats needing an artifact are claimed.

        Given:
            An HDF5-labelled document of each kind that shares the EDAM
            term.
        When:
            needs_processing is called.
        Then:
            Only coolers should be claimed. ``.hic`` is a contact map but
            is read in place, and AnnData is not a contact map at all.
        """
        # Act
        result = MatrixTilesetProcessor().needs_processing(_file_doc(filename))

        # Assert
        assert result is expected

    @pytest.mark.parametrize(
        ("filename", "expected"),
        [
            ("sample.mcool", (ArtifactKind.TILESET,)),
            ("sample.cool", (ArtifactKind.TILESET,)),
            ("sample.hic", ()),
            ("matrix.h5ad", ()),
        ],
    )
    def test_artifact_kinds_produced_should_be_empty_for_unclaimed_files(
        self, filename, expected
    ):
        """Test that an unclaimed file advertises no artifacts.

        Given:
            An HDF5-labelled document of each kind.
        When:
            artifact_kinds_produced is called.
        Then:
            Only coolers should advertise a TILESET. ``/index`` asks this
            question without first consulting needs_processing and reads
            an empty result as "this format has no index in any state of
            the world", so an .h5ad advertising a TILESET it will never
            produce would be a lie with a visible consequence.
        """
        # Act
        result = MatrixTilesetProcessor().artifact_kinds_produced(
            _file_doc(filename)
        )

        # Assert
        assert result == expected

    def test_artifact_kinds_produced_should_fall_back_without_a_document(self):
        """Test the generic-introspection call still answers.

        Given:
            No file document, as a caller doing generic introspection
            would pass.
        When:
            artifact_kinds_produced is called.
        Then:
            It should report the static class-level tuple.
        """
        # Act
        result = MatrixTilesetProcessor().artifact_kinds_produced()

        # Assert
        assert result == (ArtifactKind.TILESET,)

    def test_cache_key_should_carry_the_tileset_kind_and_version(self):
        """Test the artifact lands under a versioned, content-addressed key.

        Given:
            A cooler document with an md5.
        When:
            cache_key_for is called for the TILESET kind.
        Then:
            The key should embed the DCC, local id, kind, md5, and
            processor version — so an upstream byte change or a version
            bump invalidates both the artifact and every tile cached
            against it.
        """
        # Act
        key = MatrixTilesetProcessor().cache_key_for(
            _file_doc("sample.mcool"), ArtifactKind.TILESET
        )

        # Assert — the DCC segment comes from ``dcc.dcc_abbreviation``
        # (normalized), not ``submission``; ``extract_identity`` is the
        # single authority and every artifact kind shares its convention.
        assert key == f"4dn_dcic/4DNFIABC123/tileset/{FIXTURE_MD5}-v0"

    @settings(max_examples=50)
    @given(filename=_hdf5_filenames())
    def test_needs_processing_should_agree_with_artifact_kinds_produced(
        self, filename
    ):
        """Test classification consistency over arbitrary HDF5 filenames.

        Given:
            HDF5-labelled documents whose filenames combine free-text
            stems with contact-map and non-contact-map suffixes under
            random casing.
        When:
            needs_processing and artifact_kinds_produced are called.
        Then:
            It should never raise, claim exactly the filenames whose
            final suffix is a cooler regardless of casing, and advertise
            ``(TILESET,)`` if and only if the file is claimed — the
            consistency ``/index`` depends on.
        """
        # Arrange
        doc = _file_doc(filename)
        claimed = PurePosixPath(filename.lower()).suffix in {".mcool", ".cool"}

        # Act
        needs = MatrixTilesetProcessor().needs_processing(doc)
        kinds = MatrixTilesetProcessor().artifact_kinds_produced(doc)

        # Assert
        assert needs is claimed
        assert kinds == ((ArtifactKind.TILESET,) if needs else ())


#: Sentinel distinguishing "size_in_bytes key absent" from "present as None".
_ABSENT = object()


class TestSourceSizeGuard:
    """An oversized source is refused before a byte is downloaded."""

    @pytest.mark.asyncio
    async def test_should_refuse_a_source_above_the_cap_without_downloading(
        self, mocker, tmp_path
    ):
        """Test that the guard runs ahead of the fetch.

        Given:
            A cooler whose reported size exceeds the configured cap.
        When:
            The processor runs.
        Then:
            It should raise before calling download_source. The artifact
            for an .mcool is a copy of the upstream file, so an oversized
            or mislabelled source would otherwise fill the worker's disk
            and fail somewhere far less legible.
        """
        # Arrange
        mocker.patch.object(matrix_module, "TILESET_MAX_SOURCE_BYTES", 1024)
        download = mocker.patch.object(matrix_module, "download_source")
        cache = LocalFsCache(tmp_path / "cache")
        doc = _file_doc("huge.mcool", size_in_bytes=2048)

        # Act
        with pytest.raises(RuntimeError, match="tileset source cap"):
            await _drain(MatrixTilesetProcessor(), doc, tmp_path / "wd", cache)

        # Assert
        download.assert_not_called()

    @pytest.mark.asyncio
    async def test_should_not_apply_the_guard_when_it_is_disabled(
        self, mocker, tmp_path, tiny_mcool
    ):
        """Test that a zero cap means no cap.

        Given:
            ``CFDB_TILESET_MAX_SOURCE_BYTES`` set to 0 and a source
            reporting a very large size.
        When:
            The processor runs.
        Then:
            It should proceed to the download rather than refusing.
        """
        # Arrange
        mocker.patch.object(matrix_module, "TILESET_MAX_SOURCE_BYTES", 0)
        mocker.patch.object(
            matrix_module,
            "download_source",
            side_effect=_stage_copy(tiny_mcool),
        )
        cache = LocalFsCache(tmp_path / "cache")
        doc = _file_doc("huge.mcool", size_in_bytes=10**15)

        # Act
        events = await _drain(
            MatrixTilesetProcessor(), doc, tmp_path / "wd", cache
        )

        # Assert
        assert isinstance(events[-1], Complete)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "size",
        [1024, None, _ABSENT],
        ids=["int-exactly-at-cap", "none", "absent"],
    )
    async def test_should_admit_a_source_at_the_cap_or_of_unknown_size(
        self, mocker, tmp_path, tiny_mcool, size
    ):
        """Test the cap's inclusive boundary and its permissive default.

        Given:
            A 1024-byte cap and documents whose ``size_in_bytes`` is
            exactly 1024, present as None, or absent entirely.
        When:
            The processor runs on each.
        Then:
            It should proceed to the download every time — the cap is
            inclusive, and an unknown size admits rather than refuses.
        """
        # Arrange
        mocker.patch.object(matrix_module, "TILESET_MAX_SOURCE_BYTES", 1024)
        download = mocker.patch.object(
            matrix_module,
            "download_source",
            side_effect=_stage_copy(tiny_mcool),
        )
        cache = LocalFsCache(tmp_path / "cache")
        doc = _file_doc("ok.mcool")
        if size is not _ABSENT:
            doc["size_in_bytes"] = size

        # Act
        events = await _drain(
            MatrixTilesetProcessor(), doc, tmp_path / "wd", cache
        )

        # Assert
        download.assert_called_once()
        assert isinstance(events[-1], Complete)

    @pytest.mark.asyncio
    async def test_should_refuse_a_string_size_above_the_cap(
        self, mocker, tmp_path
    ):
        """Test that the guard coerces the 4DN string shape.

        Given:
            A 1024-byte cap and a document reporting ``size_in_bytes`` as
            the string ``"2048"`` — the shape 4DN's C2M2 TSV ingest
            actually stores, per the README's BigInt note.
        When:
            The processor runs.
        Then:
            It should refuse before calling download_source. An int-only
            check would leave the cap inert for exactly the DCC whose
            contact maps reach this processor.
        """
        # Arrange
        mocker.patch.object(matrix_module, "TILESET_MAX_SOURCE_BYTES", 1024)
        download = mocker.patch.object(matrix_module, "download_source")
        cache = LocalFsCache(tmp_path / "cache")
        doc = _file_doc("huge.mcool", size_in_bytes="2048")

        # Act
        with pytest.raises(RuntimeError, match="tileset source cap"):
            await _drain(MatrixTilesetProcessor(), doc, tmp_path / "wd", cache)

        # Assert
        download.assert_not_called()

    @pytest.mark.asyncio
    async def test_should_admit_a_source_whose_size_string_is_not_numeric(
        self, mocker, tmp_path, tiny_mcool
    ):
        """Test that an unparseable size degrades to admission.

        Given:
            A 1024-byte cap and a document whose ``size_in_bytes`` is a
            string that does not parse as an integer.
        When:
            The processor runs.
        Then:
            It should proceed to the download — a size the guard cannot
            read is treated as unknown, not as a failure.
        """
        # Arrange
        mocker.patch.object(matrix_module, "TILESET_MAX_SOURCE_BYTES", 1024)
        download = mocker.patch.object(
            matrix_module,
            "download_source",
            side_effect=_stage_copy(tiny_mcool),
        )
        cache = LocalFsCache(tmp_path / "cache")
        doc = _file_doc("odd.mcool", size_in_bytes="not-a-number")

        # Act
        events = await _drain(
            MatrixTilesetProcessor(), doc, tmp_path / "wd", cache
        )

        # Assert
        download.assert_called_once()
        assert isinstance(events[-1], Complete)


def _stage_copy(source: Path):
    """Return a download_source stand-in that copies ``source`` into place."""

    async def _download(file_meta, dest: Path) -> Path:
        import shutil

        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(source, dest)
        return dest

    return _download


def _stage_bytes(payload: bytes):
    """Return a download_source stand-in that writes ``payload`` into place."""

    async def _download(file_meta, dest: Path) -> Path:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(payload)
        return dest

    return _download


class TestRun:
    """The single materialization stage."""

    @pytest.mark.asyncio
    async def test_should_cache_a_multi_resolution_cooler_verbatim(
        self, mocker, tmp_path, tiny_mcool
    ):
        """Test that an already-pyramided cooler is not coarsened.

        Given:
            An upstream file that already carries a ``resolutions`` group.
        When:
            The processor runs.
        Then:
            It should commit the downloaded bytes as the artifact and
            never invoke coarsening — the file is already a tile pyramid,
            which is the whole reason issue #82 needs no pre-aggregation.
        """
        # Arrange
        mocker.patch.object(
            matrix_module, "download_source", side_effect=_stage_copy(tiny_mcool)
        )
        zoomify = mocker.patch.object(matrix_module, "_zoomify")
        cache = LocalFsCache(tmp_path / "cache")
        doc = _file_doc("sample.mcool")

        # Act
        events = await _drain(
            MatrixTilesetProcessor(), doc, tmp_path / "wd", cache
        )

        # Assert
        zoomify.assert_not_called()
        key = MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)
        assert await cache.head(key) is not None
        assert events == [
            StageComplete(kind=ArtifactKind.TILESET, key=key),
            Complete(artifacts={"tileset": key}),
        ]

    @pytest.mark.asyncio
    async def test_should_coarsen_a_flat_cooler_and_cache_the_output(
        self, mocker, tmp_path, make_cool
    ):
        """Test that a flat cooler is coarsened before being cached.

        Given:
            An upstream ``.cool`` with no ``resolutions`` group — the
            shape clodius refuses to serve.
        When:
            The processor runs.
        Then:
            It should coarsen it and commit the *coarsened* file, so the
            cached artifact is one clodius can open.
        """
        # Arrange
        flat = make_cool()
        mocker.patch.object(
            matrix_module, "download_source", side_effect=_stage_copy(flat)
        )
        cache = LocalFsCache(tmp_path / "cache")
        doc = _file_doc("sample.cool")

        # Act
        await _drain(MatrixTilesetProcessor(), doc, tmp_path / "wd", cache)

        # Assert — the committed artifact carries the ``resolutions``
        # group clodius requires, observed with h5py on the cached file.
        key = MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)
        cached = LocalFsCache(tmp_path / "cache").path_for(key)
        assert _resolutions_of(cached)

    @pytest.mark.asyncio
    async def test_should_coarsen_a_flat_file_despite_its_mcool_name(
        self, mocker, tmp_path, make_cool
    ):
        """Test that content, not filename, decides the coarsening.

        Given:
            An upstream file named ``sample.mcool`` whose bytes are a
            flat, single-resolution cooler — the mislabelled shape that
            exists in the wild.
        When:
            The processor runs.
        Then:
            It should coarsen it anyway and commit an artifact carrying a
            ``resolutions`` group, so a lying filename cannot poison the
            cache with a file clodius refuses to open.
        """
        # Arrange
        flat = make_cool()
        mocker.patch.object(
            matrix_module, "download_source", side_effect=_stage_copy(flat)
        )
        cache = LocalFsCache(tmp_path / "cache")
        doc = _file_doc("sample.mcool")

        # Act
        events = await _drain(
            MatrixTilesetProcessor(), doc, tmp_path / "wd", cache
        )

        # Assert
        assert isinstance(events[-1], Complete)
        key = MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)
        cached = LocalFsCache(tmp_path / "cache").path_for(key)
        assert _resolutions_of(cached)

    @pytest.mark.asyncio
    async def test_should_cache_a_pyramided_file_despite_its_cool_name(
        self, mocker, tmp_path, tiny_mcool
    ):
        """Test that a mislabelled pyramid is committed verbatim.

        Given:
            An upstream file named ``sample.cool`` whose bytes already
            carry a ``resolutions`` group — the reverse mislabelling.
        When:
            The processor runs, with coarsening spied at the module
            boundary.
        Then:
            It should never coarsen and commit the downloaded bytes
            byte-identical to the source.
        """
        # Arrange
        mocker.patch.object(
            matrix_module, "download_source", side_effect=_stage_copy(tiny_mcool)
        )
        zoomify = mocker.spy(matrix_module, "_zoomify")
        cache = LocalFsCache(tmp_path / "cache")
        doc = _file_doc("sample.cool")

        # Act
        events = await _drain(
            MatrixTilesetProcessor(), doc, tmp_path / "wd", cache
        )

        # Assert
        assert isinstance(events[-1], Complete)
        zoomify.assert_not_called()
        key = MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)
        cached = LocalFsCache(tmp_path / "cache").path_for(key)
        assert cached.read_bytes() == tiny_mcool.read_bytes()

    @pytest.mark.asyncio
    async def test_should_refuse_a_cooler_with_variable_length_bins(
        self, mocker, tmp_path
    ):
        """Test that a restriction-fragment cooler is refused, not mangled.

        Given:
            An upstream flat cooler whose bin table is irregular
            (``Cooler.binsize`` is None), so no doubling ladder exists.
        When:
            The processor runs.
        Then:
            It should raise a RuntimeError naming the variable-length
            bins and commit nothing — a ladder over irregular bins would
            misrepresent them.
        """
        # Arrange
        varbin = build_variable_bin_cool(tmp_path / "fragments.cool")
        mocker.patch.object(
            matrix_module, "download_source", side_effect=_stage_copy(varbin)
        )
        cache = LocalFsCache(tmp_path / "cache")
        doc = _file_doc("fragments.cool")

        # Act
        with pytest.raises(RuntimeError, match="variable-length bins"):
            await _drain(MatrixTilesetProcessor(), doc, tmp_path / "wd", cache)

        # Assert
        key = MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)
        assert await cache.head(key) is None

    @pytest.mark.asyncio
    async def test_should_short_circuit_when_the_artifact_is_already_cached(
        self, mocker, tmp_path, tiny_mcool
    ):
        """Test that a cached artifact is not rebuilt on retry.

        Given:
            The TILESET artifact already present in the cache.
        When:
            The processor runs again, as it does after a crash-and-retry.
        Then:
            It should download nothing and still emit the full event
            stream, so the executor records the artifact either way.
        """
        # Arrange
        cache = LocalFsCache(tmp_path / "cache")
        doc = _file_doc("sample.mcool")
        key = MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)
        seeded = tmp_path / "seed.mcool"
        seeded.write_bytes(tiny_mcool.read_bytes())
        await cache.put(key, seeded)

        download = mocker.patch.object(matrix_module, "download_source")

        # Act
        events = await _drain(
            MatrixTilesetProcessor(), doc, tmp_path / "wd", cache
        )

        # Assert
        download.assert_not_called()
        assert events == [
            StageComplete(kind=ArtifactKind.TILESET, key=key),
            Complete(artifacts={"tileset": key}),
        ]

    @pytest.mark.asyncio
    async def test_should_serve_from_cache_before_consulting_the_size_cap(
        self, mocker, tmp_path, tiny_mcool
    ):
        """Test that the cached short-circuit precedes the size guard.

        Given:
            The TILESET artifact already cached, and a cap set below the
            document's reported source size.
        When:
            The processor runs again.
        Then:
            It should complete from the cache without downloading — the
            guard exists to protect the worker's disk from a fetch, and
            an artifact already committed needs no fetch to protect
            against.
        """
        # Arrange
        mocker.patch.object(matrix_module, "TILESET_MAX_SOURCE_BYTES", 1024)
        cache = LocalFsCache(tmp_path / "cache")
        doc = _file_doc("sample.mcool", size_in_bytes=10**9)
        key = MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)
        seeded = tmp_path / "seed.mcool"
        seeded.write_bytes(tiny_mcool.read_bytes())
        await cache.put(key, seeded)

        download = mocker.patch.object(matrix_module, "download_source")

        # Act
        events = await _drain(
            MatrixTilesetProcessor(), doc, tmp_path / "wd", cache
        )

        # Assert
        download.assert_not_called()
        assert events == [
            StageComplete(kind=ArtifactKind.TILESET, key=key),
            Complete(artifacts={"tileset": key}),
        ]

    @pytest.mark.asyncio
    async def test_should_not_commit_an_artifact_when_the_source_is_not_hdf5(
        self, mocker, tmp_path
    ):
        """Test that a garbage download poisons nothing.

        Given:
            download_source staging bytes that are not an HDF5 container
            at all — a truncated or misrouted upstream response.
        When:
            The processor runs.
        Then:
            It should raise and leave the cache key unwritten, so a
            failed build can never be mistaken for a servable artifact.
        """
        # Arrange
        pytest.importorskip("h5py")
        mocker.patch.object(
            matrix_module,
            "download_source",
            side_effect=_stage_bytes(b"this is not an HDF5 container"),
        )
        cache = LocalFsCache(tmp_path / "cache")
        doc = _file_doc("sample.mcool")

        # Act
        with pytest.raises(OSError):
            await _drain(MatrixTilesetProcessor(), doc, tmp_path / "wd", cache)

        # Assert
        key = MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)
        assert await cache.head(key) is None

    @pytest.mark.asyncio
    @settings(
        max_examples=5,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(binsize=st.sampled_from([2, 4, 6, 10, 1000]))
    async def test_should_coarsen_onto_a_doubling_ladder_from_the_base_binsize(
        self, mocker, tmp_path, binsize
    ):
        """Test the resolution ladder a flat cooler is coarsened onto.

        Given:
            Flat coolers at sampled base bin sizes over the 3000 bp
            fixture genome — including one so coarse the whole genome
            already fits a single tile.
        When:
            The processor runs and the committed artifact's resolutions
            are read back with h5py.
        Then:
            It should start the ladder at the base bin size and double
            exactly at every step, never committing an empty ladder —
            the same binary progression ``cooler zoomify`` derives, so a
            cfdb-materialized mcool is indistinguishable from a
            hand-zoomified one.
        """
        # Arrange — a fresh root per example, so one example's committed
        # artifact cannot short-circuit the next.
        root = tmp_path / uuid.uuid4().hex
        root.mkdir()
        flat = build_cool(root / "flat.cool", binsize=binsize)
        mocker.patch.object(
            matrix_module, "download_source", side_effect=_stage_copy(flat)
        )
        cache = LocalFsCache(root / "cache")
        doc = _file_doc("sample.cool")

        # Act
        events = await _drain(
            MatrixTilesetProcessor(), doc, root / "wd", cache
        )
        key = MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)
        ladder = _resolutions_of(cache.path_for(key))

        # Assert
        assert isinstance(events[-1], Complete)
        assert ladder, "a coarsened artifact must carry at least one resolution"
        assert ladder[0] == binsize
        assert all(b == 2 * a for a, b in zip(ladder, ladder[1:]))
