"""Tests for simplified REST access control in data router."""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from wool.runtime.routine.task import do_dispatch

from cfdb import api
from cfdb.api.routers.data import stream_file
from cfdb.services import drs, locks
from cfdb.workflows.executor import WoolExecutor
from cfdb.workflows.models import ACTIVE_STATUSES
from cfdb.workflows.processors.bam import BamIndexProcessor
from tests.test_workflows import FIXTURE_MD5
from cfdb.workflows.processors.registry import ProcessorRegistry, default_registry


def _make_request(method: str = "GET"):
    """Return a minimal mock request object."""

    class FakeRequest:
        def __init__(self):
            self.method = method

    return FakeRequest()


def _make_dcc_doc() -> dict:
    return {
        "dcc_abbreviation": "HuBMAP",
        "project_id_namespace": "tag:hubmapconsortium.org,2023:",
    }


def _make_file_doc(*, access_level: str = "public") -> dict:
    return {
        "id_namespace": "tag:hubmapconsortium.org,2023:",
        "local_id": "file-1",
        "filename": "data.bam",
        "access_url": "drs://drs.hubmapconsortium.org/abc",
        "data_access_level": access_level,
        "submission": "hubmap",
    }


class TestStreamFile:
    @pytest.mark.asyncio
    async def test_non_public_file_returns_403(self, mock_db, mocker):
        """
        GIVEN a HuBMAP file with data_access_level="consortium" in the database
        WHEN stream_file is called for that file
        THEN a 403 HTTPException is raised without any Search API calls
        """
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        mock_db.dcc.docs = [_make_dcc_doc()]
        mock_db.file.docs = [_make_file_doc(access_level="consortium")]

        with pytest.raises(HTTPException) as exc_info:
            await stream_file("hubmap", "file-1", _make_request(), range=None)

        assert exc_info.value.status_code == 403
        assert "consortium" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_public_file_proceeds_past_access_check(self, mock_db, mocker):
        """
        GIVEN a public HuBMAP file in the database
        WHEN stream_file is called
        THEN the access check passes (no 403) and execution continues to DRS
        """
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        mock_db.dcc.docs = [_make_dcc_doc()]
        mock_db.file.docs = [_make_file_doc(access_level="public")]

        # Let it fail at DRS resolution (proves it passed the access check)
        mocker.patch.object(
            drs, "fetch_drs_object", side_effect=Exception("DRS unavailable")
        )

        with pytest.raises(HTTPException) as exc_info:
            await stream_file("hubmap", "file-1", _make_request(), range=None)

        # Should fail at DRS, not at access control
        assert exc_info.value.status_code != 403


class TestStreamFileWorkflowPath:
    @pytest.mark.asyncio
    async def test_stream_file_should_dispatch_workflow_when_processor_applies(
        self, mock_db, mocker
    ):
        """Test that /data dispatches a workflow on cache miss for SAM.

        Given:
            A 4DN SAM file with no extra_files sidecar, a registered
            BAM/SAM processor, a cache reporting a miss, and an executor.
            (SAM is used here rather than BAM because BAMs in the corpus
            come pre-sorted from upstream, so ``BamIndexProcessor`` only
            advertises an INDEX artifact for them — ``/data`` falls
            through to direct streaming and never dispatches. SAMs still
            require convert+sort+index, so ``/data`` does dispatch.)
        When:
            stream_file is called.
        Then:
            It should return a 202 JSONResponse with Location set to
            ``/jobs/{job_id}`` rather than attempting to stream the
            upstream SAM directly.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mock_db.jobs.create_index(
            {"workflow_key": 1},
            unique=True,
            partialFilterExpression={
                "status": {"$in": [s.value for s in ACTIVE_STATUSES]}
            },
        )

        mock_db.dcc.docs = [
            {
                "dcc_abbreviation": "4DN_DCIC",
                "project_id_namespace": "tag:4dn.org,2015:",
            }
        ]
        sam_doc = {
            "submission": "4dn",
            "id_namespace": "tag:4dn.org,2015:",
            "local_id": "4DNFISAM01",
            "filename": "x.sam",
            "md5": FIXTURE_MD5,
            "access_url": "https://example.com/x.sam",
            "dcc": {"dcc_abbreviation": "4DN_DCIC"},
            "file_format": {"name": "SAM"},
        }
        mock_db.file.docs = [sam_doc]

        class _NoopCache:
            async def head(self, _k):
                return None

            def get(self, _k, _r=None):
                raise AssertionError("cache miss path should not stream")

            async def put(self, *_a, **_kw):
                raise AssertionError

            async def delete(self, _k):
                return False

        registry = ProcessorRegistry()
        registry.register(BamIndexProcessor())
        mocker.patch.object(api, "cache", _NoopCache())
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(
            api,
            "executor",
            WoolExecutor(
                mock_db,
                api.cache,
                "/tmp/cfdb/cache",
                registry,
                workdir_root="/tmp/cfdb/jobs",
            ),
        )

        # Act
        with do_dispatch(False):
            resp = await stream_file(
                "4dn", "4DNFISAM01", _make_request(), range=None
            )

        # Assert
        assert isinstance(resp, JSONResponse)
        assert resp.status_code == 202
        assert resp.headers["location"].startswith("/jobs/")

    @pytest.mark.asyncio
    async def test_stream_file_should_fall_through_when_processor_skips_data(
        self, mock_db, mocker
    ):
        """Test that BAM /data falls through to direct streaming.

        Given:
            A 4DN BAM file with a registered ``BamIndexProcessor``.
            Per-file ``artifact_kinds_produced`` returns only INDEX for
            BAM (the source is already coordinate-sorted upstream and
            doesn't need re-caching), so the workflow helper SHOULD
            return None for the DATA path.
        When:
            stream_file is called.
        Then:
            The cache MUST NOT be consulted (workflow helper bails
            before cache lookup), the executor MUST NOT be dispatched,
            and execution MUST reach the existing direct-streaming
            branch — observed here by letting the DRS lookup fail with
            a clear marker exception.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        class _FailIfConsulted:
            async def head(self, _k):
                raise AssertionError("BAM /data must not consult the cache")

            def get(self, *_a, **_k):
                raise AssertionError

            async def put(self, *_a, **_k):
                raise AssertionError

            async def delete(self, _k):
                return False

        class _FailIfDispatched:
            async def ensure_workflow(self, _file_doc):
                raise AssertionError("BAM /data must not dispatch a workflow")

        registry = ProcessorRegistry()
        registry.register(BamIndexProcessor())
        mocker.patch.object(api, "cache", _FailIfConsulted())
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "executor", _FailIfDispatched())

        mock_db.dcc.docs = [
            {
                "dcc_abbreviation": "4DN_DCIC",
                "project_id_namespace": "tag:4dn.org,2015:",
            }
        ]
        mock_db.file.docs = [
            {
                "submission": "4dn",
                "id_namespace": "tag:4dn.org,2015:",
                "local_id": "4DNFIBAM01",
                "filename": "x.bam",
                "md5": FIXTURE_MD5,
                "access_url": "drs://4dn/abc",
                "dcc": {"dcc_abbreviation": "4DN_DCIC"},
                "file_format": {"name": "BAM"},
            }
        ]

        drs_calls: list = []

        async def fake_drs(*args, **kwargs):
            drs_calls.append((args, kwargs))
            raise Exception("DRS stub — proves we reached direct-stream path")

        mocker.patch.object(drs, "fetch_drs_object", side_effect=fake_drs)

        # Act & assert — DRS path is reached, not the workflow path.
        with pytest.raises(HTTPException) as exc_info:
            await stream_file(
                "4dn", "4DNFIBAM01", _make_request(), range=None
            )
        assert exc_info.value.status_code != 202
        assert len(drs_calls) == 1, (
            "BAM /data must reach direct DRS streaming, not the workflow path"
        )

    @pytest.mark.asyncio
    async def test_stream_file_should_return_404_on_head_cache_miss(
        self, mock_db, mocker
    ):
        """Test that HEAD on /data for SAM cache miss does not dispatch.

        Given:
            A SAM file with no cached artifact and a registered
            BAM/SAM processor. (SAM rather than BAM because BAM's
            /data path falls through to upstream regardless of cache
            state — see the fall-through test for that case.)
        When:
            stream_file is called with method HEAD.
        Then:
            It should raise HTTPException(404) without calling
            ensure_workflow — HEAD is side-effect-free by contract.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        class _MissCache:
            async def head(self, _k):
                return None

            def get(self, _k, _r=None):
                raise AssertionError

            async def put(self, *_a, **_kw):
                raise AssertionError

            async def delete(self, _k):
                return False

        class _FailIfDispatched:
            async def ensure_workflow(self, _file_doc):
                raise AssertionError("HEAD must not dispatch a workflow")

        registry = ProcessorRegistry()
        registry.register(BamIndexProcessor())
        mocker.patch.object(api, "cache", _MissCache())
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "executor", _FailIfDispatched())

        mock_db.dcc.docs = [
            {
                "dcc_abbreviation": "4DN_DCIC",
                "project_id_namespace": "tag:4dn.org,2015:",
            }
        ]
        mock_db.file.docs = [
            {
                "submission": "4dn",
                "id_namespace": "tag:4dn.org,2015:",
                "local_id": "4DNFISAM01",
                "filename": "x.sam",
                "md5": FIXTURE_MD5,
                "access_url": "https://example.com/x.sam",
                "dcc": {"dcc_abbreviation": "4DN_DCIC"},
                "file_format": {"name": "SAM"},
            }
        ]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_file(
                "4dn", "4DNFISAM01", _make_request("HEAD"), range=None
            )
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_stream_file_should_bypass_workflow_when_raw_true(
        self, mock_db, mocker
    ):
        """Test that ?raw=true skips the workflow branch entirely.

        Given:
            A BAM file with a registered processor and a client passing raw=True.
        When:
            stream_file is called.
        Then:
            The cache and executor must NOT be consulted, and the request
            falls through to the direct-streaming path (here DRS is
            stubbed to raise so we observe that path was reached).
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        class _FailIfConsulted:
            async def head(self, _k):
                raise AssertionError("raw=true must not consult the cache")

            def get(self, *_a, **_k):
                raise AssertionError

            async def put(self, *_a, **_k):
                raise AssertionError

            async def delete(self, _k):
                return False

        class _FailIfDispatched:
            async def ensure_workflow(self, _file_doc):
                raise AssertionError("raw=true must not dispatch a workflow")

        registry = ProcessorRegistry()
        registry.register(BamIndexProcessor())
        mocker.patch.object(api, "cache", _FailIfConsulted())
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "executor", _FailIfDispatched())

        mock_db.dcc.docs = [
            {
                "dcc_abbreviation": "4DN_DCIC",
                "project_id_namespace": "tag:4dn.org,2015:",
            }
        ]
        mock_db.file.docs = [
            {
                "submission": "4dn",
                "id_namespace": "tag:4dn.org,2015:",
                "local_id": "4DNFIBAM01",
                "filename": "x.bam",
                "md5": FIXTURE_MD5,
                "access_url": "drs://4dn/abc",
                "dcc": {"dcc_abbreviation": "4DN_DCIC"},
                "file_format": {"name": "BAM"},
            }
        ]

        # Stub DRS so we observe that the direct-streaming branch ran.
        drs_calls: list = []

        async def fake_drs(*args, **kwargs):
            drs_calls.append((args, kwargs))
            raise Exception("DRS stub — proves we reached direct-stream path")

        mocker.patch.object(drs, "fetch_drs_object", side_effect=fake_drs)

        # Act & assert — DRS is consulted (non-workflow path).
        with pytest.raises(HTTPException) as exc_info:
            await stream_file(
                "4dn", "4DNFIBAM01", _make_request(), range=None, raw=True
            )
        assert exc_info.value.status_code != 202
        assert len(drs_calls) == 1, "raw=True must reach the DRS streaming path"

    @pytest.mark.asyncio
    async def test_stream_file_should_serve_cached_data_artifact_for_sam(
        self, mock_db, mocker, tmp_path
    ):
        """Test (DA-001) that a SAM /data cache hit short-circuits dispatch.

        Given:
            A SAM file with the DATA cache pre-populated and a wired
            workflow subsystem.
        When:
            ``stream_file`` is called for that file.
        Then:
            It should return a 200 ``StreamingResponse`` carrying the
            cached bytes, and ``ensure_workflow`` MUST NOT be called.
        """
        # Arrange
        from cfdb.workflows import keys as key_utils
        from cfdb.workflows.cache import LocalFsCache
        from cfdb.workflows.models import ArtifactKind
        from starlette.responses import StreamingResponse

        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        cache = LocalFsCache(tmp_path / "cache")
        src = tmp_path / "src"
        src.write_bytes(b"sam-cached-bytes")
        processor = BamIndexProcessor()
        # The cache key is derived from the normalized dcc value from
        # extract_identity(file_doc), which reads
        # ``dcc.dcc_abbreviation`` → "4DN_DCIC" → normalized to
        # "4dn_dcic".
        data_key = key_utils.cache_key(
            dcc="4dn_dcic",
            local_id="4DNFISAM01",
            artifact_kind=ArtifactKind.DATA,
            md5=FIXTURE_MD5,
            processor_version=processor.processor_version,
        )
        await cache.put(data_key, src)

        class _FailIfDispatched:
            async def ensure_workflow(self, _file_doc):
                raise AssertionError("cache hit must NOT dispatch a workflow")

        registry = ProcessorRegistry()
        registry.register(processor)
        mocker.patch.object(api, "cache", cache)
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "executor", _FailIfDispatched())

        mock_db.dcc.docs = [
            {
                "dcc_abbreviation": "4DN_DCIC",
                "project_id_namespace": "tag:4dn.org,2015:",
            }
        ]
        mock_db.file.docs = [
            {
                "submission": "4dn",
                "id_namespace": "tag:4dn.org,2015:",
                "local_id": "4DNFISAM01",
                "filename": "x.sam",
                "md5": FIXTURE_MD5,
                "access_url": "https://example.com/x.sam",
                "dcc": {"dcc_abbreviation": "4DN_DCIC"},
                "file_format": {"name": "SAM"},
            }
        ]

        # Act
        resp = await stream_file(
            "4dn", "4DNFISAM01", _make_request(), range=None
        )

        # Assert
        assert isinstance(resp, StreamingResponse)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_stream_file_should_enforce_hubmap_access_before_workflow_branch(
        self, mock_db, mocker
    ):
        """Test (DA-002) that the HuBMAP 403 fires before workflow branch.

        Given:
            A HuBMAP file with ``data_access_level="protected"`` and a
            wired workflow subsystem.
        When:
            ``stream_file`` is called.
        Then:
            It should raise ``HTTPException(403)`` BEFORE the workflow
            branch — ``processor_registry.lookup_for`` MUST NOT be
            consulted.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        class _FailIfConsulted:
            def lookup_for(self, _doc):
                raise AssertionError(
                    "HuBMAP access guard must precede registry lookup"
                )

        mocker.patch.object(api, "processor_registry", _FailIfConsulted())
        mocker.patch.object(api, "cache", object())
        mocker.patch.object(api, "executor", object())

        mock_db.dcc.docs = [_make_dcc_doc()]
        mock_db.file.docs = [_make_file_doc(access_level="protected")]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_file("hubmap", "file-1", _make_request(), range=None)
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_stream_file_should_fall_through_when_format_is_passthrough(
        self, mock_db, mocker
    ):
        """Test that passthrough formats bypass the workflow subsystem.

        Given:
            A CSV file (a PassthroughProcessor format) and a wired
            workflow subsystem.
        When:
            stream_file is called.
        Then:
            It should NOT return a 202 from the workflow path; instead
            the existing DRS streaming path runs and (in this test) the
            mocked DRS resolution raises — proving the workflow branch
            did not intercept.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mocker.patch.object(api, "processor_registry", default_registry())

        class _Cache:
            async def head(self, _k):
                raise AssertionError("passthrough must not consult cache")

            def get(self, *_a, **_kw):
                raise AssertionError

            async def put(self, *_a, **_kw):
                raise AssertionError

            async def delete(self, _k):
                return False

        mocker.patch.object(api, "cache", _Cache())
        mocker.patch.object(api, "executor", object())

        mock_db.dcc.docs = [
            {
                "dcc_abbreviation": "4DN_DCIC",
                "project_id_namespace": "tag:4dn.org,2015:",
            }
        ]
        csv_doc = {
            "submission": "4dn",
            "id_namespace": "tag:4dn.org,2015:",
            "local_id": "4DNFICSV01",
            "filename": "x.csv",
            "md5": FIXTURE_MD5,
            "access_url": "drs://4dn/abc",
            "file_format": {"name": "CSV"},
        }
        mock_db.file.docs = [csv_doc]

        drs_calls: list = []

        async def fake_drs(*args, **kwargs):
            drs_calls.append((args, kwargs))
            raise Exception("DRS unavailable")

        mocker.patch.object(drs, "fetch_drs_object", side_effect=fake_drs)

        # Act & assert — should fail at DRS, not short-circuit to 202
        with pytest.raises(HTTPException) as exc_info:
            await stream_file("4dn", "4DNFICSV01", _make_request(), range=None)
        assert exc_info.value.status_code != 202
        assert len(drs_calls) == 1, (
            "passthrough must reach the direct DRS streaming path, "
            "not the workflow dispatch path"
        )
