"""Tests for the preparation channel in ``cfdb.api.routers.tilesets``."""

from __future__ import annotations

import json

import pytest
from fastapi import HTTPException

from cfdb import api
from cfdb.api.routers import tilesets
from cfdb.api.routers.tilesets import prepare_tileset, tileset_status
from cfdb.services import locks
from cfdb.tilesets import backend
from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.executor import (
    AdmissionRejected,
    ExecutorDraining,
    WorkflowNotApplicable,
)
from cfdb.workflows.models import ArtifactKind, JobStatus
from cfdb.workflows.processors.matrix import MatrixTilesetProcessor
from cfdb.workflows.processors.registry import ProcessorRegistry
from tests.test_workflows import FIXTURE_MD5


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


class _Record:
    """Minimal stand-in for the JobRecord ``ensure_workflow`` returns."""

    job_id = "job-abc-123"
    status = JobStatus.PENDING


class _StubExecutor:
    """Executor stand-in that returns a record or raises a chosen error."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls = 0

    async def ensure_workflow(self, _file_doc):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return _Record(), True


@pytest.fixture()
def prep_env(mock_db, mocker, tmp_path):
    """Wire the workflow subsystem with the matrix processor registered."""
    mocker.patch.object(locks, "wait_for_cutover", return_value=None)
    cache = LocalFsCache(tmp_path / "cache")
    registry = ProcessorRegistry()
    registry.register(MatrixTilesetProcessor())
    executor = _StubExecutor()
    mocker.patch.object(api, "cache", cache)
    mocker.patch.object(api, "processor_registry", registry)
    mocker.patch.object(api, "executor", executor)

    async def _seed(doc, mcool):
        key = MatrixTilesetProcessor().cache_key_for(doc, ArtifactKind.TILESET)
        staged = tmp_path / f"staged-{key.replace('/', '_')}"
        staged.write_bytes(mcool.read_bytes())
        await cache.put(key, staged)

    return _seed, mock_db, executor, mocker


def _body(response) -> dict:
    """Decode a JSONResponse body."""
    return json.loads(response.body)


class TestStatusProbe:
    """``GET /tilesets/{dcc}/{local_id}/status``."""

    @pytest.mark.asyncio
    async def test_should_report_not_ready_when_no_artifact_is_cached(
        self, prep_env
    ):
        """Test the pre-preparation state.

        Given:
            A contact map whose artifact has not been built.
        When:
            The status probe runs.
        Then:
            It should report not ready and dispatch nothing — this probe
            mirrors the /data and /index probes, which are documented as
            never dispatching.
        """
        # Arrange
        pytest.importorskip("clodius")
        _, mock_db, executor, _ = prep_env
        mock_db.file.docs = [_file_doc()]

        # Act
        result = await tileset_status("4dn", "4DNFIABC123")

        # Assert
        assert result == {"ready": False}
        assert executor.calls == 0

    @pytest.mark.asyncio
    async def test_should_report_ready_once_the_artifact_is_cached(
        self, prep_env, tiny_mcool
    ):
        """Test the post-preparation state.

        Given:
            A contact map whose tileset artifact is cached.
        When:
            The status probe runs.
        Then:
            It should report ready, which is a UI's signal that mounting
            the Gosling spec will now work.
        """
        # Arrange
        seed, mock_db, _, _ = prep_env
        doc = _file_doc()
        mock_db.file.docs = [doc]
        await seed(doc, tiny_mcool)

        # Act
        result = await tileset_status("4dn", "4DNFIABC123")

        # Assert
        assert result == {"ready": True}

    @pytest.mark.asyncio
    async def test_should_refuse_a_hic_rather_than_report_a_readiness(
        self, prep_env
    ):
        """Test that .hic is refused here, not reported as ready.

        Given:
            An ENCODE .hic, which cfdb does not serve — the tile backend
            reads local files only, and these run to hundreds of GB.
        When:
            The status probe runs.
        Then:
            It should raise 501, matching the tile routes. Reporting
            ``ready: true`` would be worse than useless: the whole point
            of this probe is to tell a UI whether mounting a Gosling spec
            will work, and a promise the tile endpoint then refuses sends
            the client straight into a 501.
        """
        # Arrange
        _, mock_db, _, _ = prep_env
        mock_db.file.docs = [
            _file_doc(submission="encode", local_id="ENCFF123ABC", filename="c.hic")
        ]

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_status("encode", "ENCFF123ABC")
        assert exc_info.value.status_code == 501

    @pytest.mark.asyncio
    async def test_should_404_a_file_that_is_not_a_contact_map(self, prep_env):
        """Test that non-matrix files are rejected here too.

        Given:
            An AnnData matrix.
        When:
            The status probe runs.
        Then:
            It should raise 404 with a message naming the supported
            formats.
        """
        # Arrange
        _, mock_db, _, _ = prep_env
        mock_db.file.docs = [_file_doc(filename="matrix.h5ad")]

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_status("4dn", "4DNFIABC123")
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_should_403_a_non_public_hubmap_file(self, prep_env):
        """Test that access control precedes any readiness answer.

        Given:
            A non-public HuBMAP contact map.
        When:
            The status probe runs.
        Then:
            It should raise 403 rather than disclosing readiness.
        """
        # Arrange
        _, mock_db, _, _ = prep_env
        mock_db.file.docs = [
            _file_doc(
                submission="hubmap",
                local_id="HBM123",
                data_access_level="protected",
            )
        ]

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_status("hubmap", "HBM123")
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_should_503_when_the_workflow_subsystem_is_disabled(
        self, prep_env
    ):
        """Test degraded mode.

        Given:
            No cache, as when SYNC_DATA_DIR is unset.
        When:
            The status probe runs.
        Then:
            It should raise 503 with Retry-After, matching how /index
            signals the same condition.
        """
        # Arrange
        pytest.importorskip("clodius")
        _, mock_db, _, mocker = prep_env
        mock_db.file.docs = [_file_doc()]
        mocker.patch.object(api, "cache", None)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_status("4dn", "4DNFIABC123")
        assert exc_info.value.status_code == 503

    @pytest.mark.asyncio
    async def test_should_409_a_document_with_no_md5(self, prep_env):
        """Test that an unaddressable document is a conflict, not a 500.

        Given:
            A contact map whose document carries no md5, so no cache key
            can be derived for it.
        When:
            The status probe runs.
        Then:
            It should raise 409 — a stale or hand-edited record, which no
            retry will fix and which is not the server's fault.
        """
        # Arrange
        pytest.importorskip("clodius")
        _, mock_db, _, _ = prep_env
        doc = _file_doc()
        del doc["md5"]
        mock_db.file.docs = [doc]

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_status("4dn", "4DNFIABC123")
        assert exc_info.value.status_code == 409

    @pytest.mark.asyncio
    async def test_should_501_when_the_tile_backend_is_absent(
        self, prep_env, mocker
    ):
        """Test the answer for a build without clodius.

        Given:
            A contact map, but a build whose tile backend is absent.
        When:
            The status probe runs.
        Then:
            It should raise 501 naming the tile backend and dispatch
            nothing — a readiness answer would promise a tile route this
            build can never serve.
        """
        # Arrange
        _, mock_db, executor, _ = prep_env
        mock_db.file.docs = [_file_doc()]
        mocker.patch.object(backend, "is_available", return_value=False)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_status("4dn", "4DNFIABC123")
        assert exc_info.value.status_code == 501
        assert "clodius" in exc_info.value.detail
        assert executor.calls == 0

    @pytest.mark.asyncio
    async def test_should_503_a_hic_when_the_workflow_subsystem_is_disabled(
        self, prep_env
    ):
        """Test guard ordering when two refusals apply at once.

        Given:
            An ENCODE .hic document AND a disabled workflow subsystem
            (api.cache is None), so both the 503 and the 501 branch match.
        When:
            The status probe runs.
        Then:
            It should raise 503, not 501 — the subsystem check precedes
            the .hic refusal, so an operator condition is reported ahead
            of a per-file limitation. This ordering is a visible decision:
            reordering the guards would silently change the answer.
        """
        # Arrange
        pytest.importorskip("clodius")
        _, mock_db, _, mocker = prep_env
        mock_db.file.docs = [
            _file_doc(submission="encode", local_id="ENCFF123ABC", filename="c.hic")
        ]
        mocker.patch.object(api, "cache", None)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_status("encode", "ENCFF123ABC")
        assert exc_info.value.status_code == 503
        assert exc_info.value.headers["Retry-After"] == "30"

    @pytest.mark.asyncio
    async def test_should_500_with_a_generic_detail_when_the_probe_raises(
        self, prep_env
    ):
        """Test the status probe's catch-all wrapper.

        Given:
            A readiness probe that raises RuntimeError — a bug, not a
            modelled condition.
        When:
            The status probe runs.
        Then:
            It should raise 500 with a generic detail rather than leaking
            the exception text to an unauthenticated caller.
        """
        # Arrange
        pytest.importorskip("clodius")
        _, mock_db, _, mocker = prep_env
        mock_db.file.docs = [_file_doc()]
        mocker.patch.object(
            tilesets,
            "probe_workflow_readiness",
            side_effect=RuntimeError("disk exploded at /var/secret/path"),
        )

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await tileset_status("4dn", "4DNFIABC123")
        assert exc_info.value.status_code == 500
        assert exc_info.value.detail == "Internal server error"


class TestPrepare:
    """``POST /tilesets/{dcc}/{local_id}``."""

    @pytest.mark.asyncio
    async def test_should_dispatch_and_point_at_the_job_endpoint(self, prep_env):
        """Test the dispatch response.

        Given:
            A contact map with no cached artifact.
        When:
            prepare_tileset is called.
        Then:
            It should return 202 with a Location header pointing at the
            existing /jobs endpoint, so the UI polls the same way it does
            for /data and /index preprocessing.
        """
        # Arrange
        pytest.importorskip("clodius")
        _, mock_db, executor, _ = prep_env
        mock_db.file.docs = [_file_doc()]

        # Act
        response = await prepare_tileset("4dn", "4DNFIABC123")

        # Assert
        assert response.status_code == 202
        assert response.headers["location"] == "/jobs/job-abc-123"
        assert response.headers["retry-after"] == "5"
        assert _body(response)["job_id"] == "job-abc-123"
        assert executor.calls == 1

    @pytest.mark.asyncio
    async def test_should_be_idempotent_once_the_artifact_exists(
        self, prep_env, tiny_mcool
    ):
        """Test that re-preparing a prepared file is not a job.

        Given:
            A contact map whose artifact is already cached.
        When:
            prepare_tileset is called.
        Then:
            It should return 200 ready and dispatch nothing. A UI that
            POSTs before checking should not create a no-op job.
        """
        # Arrange
        seed, mock_db, executor, _ = prep_env
        doc = _file_doc()
        mock_db.file.docs = [doc]
        await seed(doc, tiny_mcool)

        # Act
        response = await prepare_tileset("4dn", "4DNFIABC123")

        # Assert
        assert response.status_code == 200
        assert _body(response) == {"ready": True}
        assert executor.calls == 0

    @pytest.mark.asyncio
    async def test_should_refuse_a_hic_without_dispatching(self, prep_env):
        """Test that .hic cannot be queued for materialization.

        Given:
            An ENCODE .hic.
        When:
            prepare_tileset is called.
        Then:
            It should raise 501 and dispatch nothing. Accepting the job
            would queue a download of a file with a median size of 10 GB
            and a corpus maximum of 315 GB — past the Fargate ephemeral
            storage ceiling — to build an artifact no endpoint would then
            serve.
        """
        # Arrange
        _, mock_db, executor, _ = prep_env
        mock_db.file.docs = [
            _file_doc(submission="encode", local_id="ENCFF123ABC", filename="c.hic")
        ]

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await prepare_tileset("encode", "ENCFF123ABC")
        assert exc_info.value.status_code == 501
        assert executor.calls == 0

    @pytest.mark.asyncio
    async def test_should_429_when_the_admission_ceiling_is_reached(
        self, prep_env
    ):
        """Test load shedding.

        Given:
            An executor at the active-workflow ceiling.
        When:
            prepare_tileset is called.
        Then:
            It should raise 429 carrying the executor's own Retry-After,
            so tile preparation sheds under the same ceiling as /data and
            /index rather than queuing past it.
        """
        # Arrange
        pytest.importorskip("clodius")
        _, mock_db, _, mocker = prep_env
        mock_db.file.docs = [_file_doc()]
        mocker.patch.object(
            api,
            "executor",
            _StubExecutor(
                AdmissionRejected(active=1024, ceiling=1024, retry_after_seconds=17)
            ),
        )

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await prepare_tileset("4dn", "4DNFIABC123")
        assert exc_info.value.status_code == 429
        assert exc_info.value.headers["Retry-After"] == "17"

    @pytest.mark.asyncio
    async def test_should_503_while_the_executor_is_draining(self, prep_env):
        """Test that shutdown is caught ahead of inapplicability.

        Given:
            An executor that is draining for lifespan teardown.
        When:
            prepare_tileset is called.
        Then:
            It should raise 503, not 409. ExecutorDraining subclasses
            WorkflowNotApplicable, so this only holds if the catch order
            puts the subclass first — which is exactly the kind of thing
            a reorder would silently break.
        """
        # Arrange
        pytest.importorskip("clodius")
        _, mock_db, _, mocker = prep_env
        mock_db.file.docs = [_file_doc()]
        mocker.patch.object(
            api, "executor", _StubExecutor(ExecutorDraining("shutting down"))
        )

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await prepare_tileset("4dn", "4DNFIABC123")
        assert exc_info.value.status_code == 503

    @pytest.mark.asyncio
    async def test_should_409_when_the_executor_declines_the_file(self, prep_env):
        """Test the race where the executor disagrees with the router.

        Given:
            An executor that reports the workflow inapplicable.
        When:
            prepare_tileset is called.
        Then:
            It should raise 409 rather than 500 — a disagreement about
            this file, not a server fault.
        """
        # Arrange
        pytest.importorskip("clodius")
        _, mock_db, _, mocker = prep_env
        mock_db.file.docs = [_file_doc()]
        mocker.patch.object(
            api, "executor", _StubExecutor(WorkflowNotApplicable("no processor"))
        )

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await prepare_tileset("4dn", "4DNFIABC123")
        assert exc_info.value.status_code == 409

    @pytest.mark.asyncio
    async def test_should_501_without_dispatching_when_the_tile_backend_is_absent(
        self, prep_env, mocker
    ):
        """Test that a backendless build refuses to queue work.

        Given:
            A contact map, but a build whose tile backend is absent.
        When:
            prepare_tileset is called.
        Then:
            It should raise 501 naming the tile backend and dispatch
            nothing — building an artifact no route in this build could
            ever read would be pure waste.
        """
        # Arrange
        _, mock_db, executor, _ = prep_env
        mock_db.file.docs = [_file_doc()]
        mocker.patch.object(backend, "is_available", return_value=False)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await prepare_tileset("4dn", "4DNFIABC123")
        assert exc_info.value.status_code == 501
        assert "clodius" in exc_info.value.detail
        assert executor.calls == 0

    @pytest.mark.asyncio
    async def test_should_409_a_document_with_no_md5_without_dispatching(
        self, prep_env
    ):
        """Test the POST side of the unaddressable-document conflict.

        Given:
            A contact map whose document carries no md5, so no cache key
            can be derived for its artifact.
        When:
            prepare_tileset is called.
        Then:
            It should raise 409 and dispatch nothing — a workflow whose
            output could never be addressed must not be queued.
        """
        # Arrange
        pytest.importorskip("clodius")
        _, mock_db, executor, _ = prep_env
        doc = _file_doc()
        del doc["md5"]
        mock_db.file.docs = [doc]

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await prepare_tileset("4dn", "4DNFIABC123")
        assert exc_info.value.status_code == 409
        assert executor.calls == 0

    @pytest.mark.asyncio
    async def test_should_503_when_the_workflow_subsystem_is_disabled(
        self, prep_env
    ):
        """Test the POST side of degraded mode.

        Given:
            The tile backend present but no cache, as when SYNC_DATA_DIR
            is unset.
        When:
            prepare_tileset is called.
        Then:
            It should raise 503 with Retry-After and leave the executor
            untouched — nothing could store the artifact a dispatch would
            build.
        """
        # Arrange
        pytest.importorskip("clodius")
        _, mock_db, executor, mocker = prep_env
        mock_db.file.docs = [_file_doc()]
        mocker.patch.object(api, "cache", None)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await prepare_tileset("4dn", "4DNFIABC123")
        assert exc_info.value.status_code == 503
        assert exc_info.value.headers["Retry-After"] == "30"
        assert executor.calls == 0

    @pytest.mark.asyncio
    async def test_should_500_with_a_generic_detail_when_the_probe_raises(
        self, prep_env
    ):
        """Test prepare_tileset's catch-all wrapper.

        Given:
            A readiness probe that raises RuntimeError — a bug, not a
            modelled condition.
        When:
            prepare_tileset is called.
        Then:
            It should raise 500 with a generic detail rather than leaking
            the exception text to an unauthenticated caller.
        """
        # Arrange
        pytest.importorskip("clodius")
        _, mock_db, _, mocker = prep_env
        mock_db.file.docs = [_file_doc()]
        mocker.patch.object(
            tilesets,
            "probe_workflow_readiness",
            side_effect=RuntimeError("disk exploded at /var/secret/path"),
        )

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await prepare_tileset("4dn", "4DNFIABC123")
        assert exc_info.value.status_code == 500
        assert exc_info.value.detail == "Internal server error"

    @pytest.mark.asyncio
    async def test_should_500_with_a_generic_detail_when_the_executor_raises(
        self, prep_env
    ):
        """Test that a non-workflow executor error is not misclassified.

        Given:
            An executor raising RuntimeError — outside the dispatch
            ladder's modelled exceptions (draining, admission, not
            applicable).
        When:
            prepare_tileset is called.
        Then:
            It should raise 500 with a generic detail: the ladder must
            let a genuine bug escape to the catch-all rather than dress
            it up as a 409 or 503 a client would retry forever.
        """
        # Arrange
        pytest.importorskip("clodius")
        _, mock_db, _, mocker = prep_env
        mock_db.file.docs = [_file_doc()]
        mocker.patch.object(
            api, "executor", _StubExecutor(RuntimeError("wool imploded"))
        )

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await prepare_tileset("4dn", "4DNFIABC123")
        assert exc_info.value.status_code == 500
        assert exc_info.value.detail == "Internal server error"


class TestOverHttp:
    """The wire layer, which calling the handlers directly cannot exercise.

    The Path(...) length and pattern constraints are enforced by FastAPI's
    validation layer, so they are only really tested through ASGI. Follows
    ``tests/test_tiles.py::TestOverHttp`` in neutering the lifespan.
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
        ("method", "url"),
        [
            ("GET", "/tilesets/4dn/4DNFIABC123/status"),
            ("POST", "/tilesets/4dn/4DNFIABC123"),
        ],
    )
    def test_should_mount_both_routes_on_the_app(self, client, method, url):
        """Test that the router is wired into the application.

        Given:
            Well-formed probe and prepare URLs for a file the (empty)
            database does not hold.
        When:
            They are requested over HTTP.
        Then:
            They should reach the handlers — proven by the handlers' own
            404 "File not found" rather than a routing 404 or a 405.
        """
        # Act
        response = client.request(method, url)

        # Assert
        assert response.status_code == 404
        assert response.json()["detail"] == "File not found"

    @pytest.mark.parametrize(
        ("method", "url_template"),
        [
            ("GET", "/tilesets/4dn/{local_id}/status"),
            ("POST", "/tilesets/4dn/{local_id}"),
        ],
    )
    @pytest.mark.parametrize(
        "local_id",
        [
            pytest.param("a" * 257, id="overlong-257-chars"),
            pytest.param("bad!id", id="illegal-character"),
        ],
    )
    def test_should_422_a_local_id_violating_the_path_constraints(
        self, client, method, url_template, local_id
    ):
        """Test the Path(...) constraints at the wire.

        Given:
            A local_id one character past PATH_PARAM_MAX_LEN, and one
            carrying a character outside PATH_PARAM_PATTERN.
        When:
            The probe and prepare routes are requested over HTTP.
        Then:
            It should be rejected by FastAPI's validation layer with 422
            before any handler code runs. (The README's table says 400;
            main.py registers no RequestValidationError handler, so the
            actual behavior — pinned here — is FastAPI's default 422.)
        """
        # Act
        response = client.request(method, url_template.format(local_id=local_id))

        # Assert
        assert response.status_code == 422
