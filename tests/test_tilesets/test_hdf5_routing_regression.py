"""Pins what /data and /index do once a processor claims the HDF5 format.

:class:`~cfdb.workflows.processors.matrix.MatrixTilesetProcessor` declares
``supported_formats = {"HDF5"}``, so ``ProcessorRegistry.lookup_for`` now
returns a processor for files it previously returned ``None`` for — every
``.h5ad``, every bare ``.h5``, and every contact map. Five call sites across
three modules read that result. These tests pin what each of them does
afterwards, because the change is invisible in the tile tests: it shows up
in endpoints that have nothing to do with tiles.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from cfdb import api
from cfdb.api.routers.cache_stream import (
    probe_workflow_readiness,
    serve_workflow_artifact_or_dispatch,
)
from cfdb.api.routers.data import stream_file_status
from cfdb.api.routers.index import stream_index_file
from cfdb.services import locks
from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.executor import WoolExecutor
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.matrix import MatrixTilesetProcessor
from cfdb.workflows.processors.registry import ProcessorRegistry
from tests.test_workflows import FIXTURE_MD5


def _make_request(method: str = "GET"):
    """Return a minimal mock request object."""

    class FakeRequest:
        def __init__(self):
            self.method = method

    return FakeRequest()


def _hdf5_doc(filename: str) -> dict:
    """Return an HDF5-labelled 4DN file document."""
    return {
        "submission": "4dn",
        "local_id": "4DNFIABC123",
        "md5": FIXTURE_MD5,
        "filename": filename,
        "file_format": {"name": "HDF5"},
        "access_url": "https://data.4dnucleome.org/files/" + filename,
        "dcc": {"dcc_abbreviation": "4DN_DCIC"},
    }


@pytest.fixture()
def wired_subsystem(mocker, tmp_path, mock_db):
    """Wire the workflow subsystem with only the matrix processor registered."""
    mocker.patch.object(locks, "wait_for_cutover", return_value=None)
    registry = ProcessorRegistry()
    registry.register(MatrixTilesetProcessor())
    cache = LocalFsCache(tmp_path / "cache")
    mocker.patch.object(api, "cache", cache)
    mocker.patch.object(api, "processor_registry", registry)
    mocker.patch.object(
        api,
        "executor",
        WoolExecutor(mock_db, cache, registry, workdir_root=tmp_path / "jobs"),
    )
    return cache


class TestDataFallThrough:
    """/data must be unaffected: no HDF5 file gains a DATA artifact."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("filename", ["sample.mcool", "matrix.h5ad"])
    async def test_dispatch_helper_should_fall_through_for_hdf5(
        self, wired_subsystem, filename
    ):
        """Test that /data still streams HDF5 files straight from upstream.

        Given:
            An HDF5 file — a contact map or an AnnData matrix — with the
            matrix processor registered.
        When:
            serve_workflow_artifact_or_dispatch is asked for a DATA
            artifact.
        Then:
            It should return None so the caller falls through to upstream
            streaming. A contact map is claimed by the processor but
            produces only a TILESET, and AnnData is not claimed at all —
            so neither can turn a /data GET into a 202.
        """
        # Act
        result = await serve_workflow_artifact_or_dispatch(
            _hdf5_doc(filename),
            ArtifactKind.DATA,
            _make_request(),
            None,
            head_404_detail="unused",
        )

        # Assert
        assert result is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("filename", ["sample.mcool", "matrix.h5ad"])
    async def test_readiness_probe_should_report_ready_for_hdf5(
        self, wired_subsystem, mock_db, filename
    ):
        """Test that /data/status still reports HDF5 files as ready.

        Given:
            An HDF5 file with the matrix processor registered.
        When:
            The /data readiness probe runs.
        Then:
            It should report ready — /data serves these as raw upstream
            bytes and always did. A contact map being separately tileable
            is not a fact about /data.
        """
        # Arrange
        mock_db.files.docs = []
        mock_db.file.docs = [_hdf5_doc(filename)]

        # Act
        result = await stream_file_status("4dn", "4DNFIABC123")

        # Assert
        assert result == {"ready": True}

    @pytest.mark.asyncio
    async def test_probe_should_not_claim_a_tileset_artifact_for_data(
        self, wired_subsystem
    ):
        """Test that a TILESET artifact is invisible to the DATA probe.

        Given:
            A contact map whose processor produces only a TILESET.
        When:
            probe_workflow_readiness is asked about DATA.
        Then:
            It should return None — "not a workflow question for this
            artifact kind" — rather than False, which the caller would
            render as ``ready: false`` and never become true.
        """
        # Act
        result = await probe_workflow_readiness(
            _hdf5_doc("sample.mcool"), ArtifactKind.DATA
        )

        # Assert
        assert result is None


class TestIndexFallThrough:
    """/index must keep 404ing, but the detail string moves for some files."""

    @pytest.mark.asyncio
    async def test_should_404_a_contact_map_as_a_file_without_an_index(
        self, wired_subsystem, mock_db
    ):
        """Test that a contact map still 404s from /index, unchanged.

        Given:
            An ``.mcool`` with no sidecar and the subsystem wired.
        When:
            stream_index_file is called.
        Then:
            It should 404 with the original "for this file" detail. The
            processor advertises a TILESET, so the
            no-artifacts-for-this-format branch does not fire and the
            terminal message is the one it always was.
        """
        # Arrange
        mock_db.files.docs = []
        mock_db.file.docs = [_hdf5_doc("sample.mcool")]

        # Act
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file("4dn", "4DNFIABC123", _make_request(), range=None)

        # Assert
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "No index file available for this file"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("filename", ["matrix.h5ad", "sample.hic"])
    async def test_should_404_an_unclaimed_hdf5_as_a_format_without_an_index(
        self, wired_subsystem, mock_db, filename
    ):
        """Test the one detail string this change moves.

        Given:
            An HDF5 file the matrix processor does not claim — AnnData,
            or a ``.hic``, which cfdb does not serve tiles from and does
            not materialize.
        When:
            stream_index_file is called.
        Then:
            It should still 404, but now with the "for this file format"
            detail rather than "for this file". Before the processor
            existed, ``lookup_for`` returned None and the
            no-artifacts branch was unreachable; now a processor is
            matched and advertises no artifacts, which is exactly what
            that branch means. Same status code, different string —
            pinned here so the change stays deliberate.
        """
        # Arrange
        mock_db.files.docs = []
        mock_db.file.docs = [_hdf5_doc(filename)]

        # Act
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file("4dn", "4DNFIABC123", _make_request(), range=None)

        # Assert
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "No index file available for this file format"


class TestRegistryLookup:
    """The registry now matches HDF5, and the per-file hook does the rest."""

    def test_lookup_should_match_every_hdf5_file(self):
        """Test that format-name matching alone cannot discriminate.

        Given:
            An AnnData matrix, which shares the ``HDF5`` EDAM term with
            every contact map.
        When:
            ProcessorRegistry.lookup_for runs.
        Then:
            It should return the matrix processor — and needs_processing
            should then decline it. This two-step is the whole escape from
            the ontology collision: widening ``lookup_for``'s contract to
            discriminate would have meant re-verifying five call sites
            across three modules.
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(MatrixTilesetProcessor())
        doc = _hdf5_doc("matrix.h5ad")

        # Act
        processor = registry.lookup_for(doc)

        # Assert
        assert isinstance(processor, MatrixTilesetProcessor)
        assert processor.needs_processing(doc) is False
