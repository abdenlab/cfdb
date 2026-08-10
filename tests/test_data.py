"""Tests for simplified REST access control in data router."""

from __future__ import annotations

import logging

import pytest
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from wool.runtime.routine.task import do_dispatch

from cfdb import api
from cfdb.api.routers.data import stream_file, stream_file_status
from cfdb.services import drs, locks
from cfdb.workflows.executor import WoolExecutor
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
            partialFilterExpression={"active": True},
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


class TestStreamFileStatus:
    @pytest.mark.asyncio
    async def test_stream_file_status_should_raise_400_when_dcc_invalid(
        self, mock_db, mocker
    ):
        """Test that an unknown DCC is rejected the same as /data.

        Given:
            A status probe for a DCC name not in the registry.
        When:
            stream_file_status is called.
        Then:
            It should raise HTTPException(400).
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_file_status("nope", "file-1")
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_stream_file_status_should_raise_404_when_file_missing(
        self, mock_db, mocker
    ):
        """Test that a missing file mirrors /data's 404.

        Given:
            A valid DCC but no matching file document.
        When:
            stream_file_status is called.
        Then:
            It should raise HTTPException(404).
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mock_db.files.docs = []
        mock_db.file.docs = []

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_file_status("4dn", "missing")
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_stream_file_status_should_raise_403_when_hubmap_not_public(
        self, mock_db, mocker
    ):
        """Test that a protected HuBMAP file mirrors /data's 403.

        Given:
            A HuBMAP file with ``data_access_level="consortium"`` and an
            access_url.
        When:
            stream_file_status is called.
        Then:
            It should raise HTTPException(403).
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mock_db.dcc.docs = [_make_dcc_doc()]
        mock_db.file.docs = [_make_file_doc(access_level="consortium")]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_file_status("hubmap", "file-1")
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_stream_file_status_should_raise_501_when_no_access_url(
        self, mock_db, mocker
    ):
        """Test that a file with no access method mirrors /data's 501.

        Given:
            A 4DN file document with no ``access_url``.
        When:
            stream_file_status is called.
        Then:
            It should raise HTTPException(501).
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mock_db.files.docs = []
        mock_db.file.docs = [
            {
                "submission": "4dn",
                "id_namespace": "tag:4dn.org,2015:",
                "local_id": "4DNFINOURL",
                "filename": "x.csv",
                "file_format": {"name": "CSV"},
            }
        ]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_file_status("4dn", "4DNFINOURL")
        assert exc_info.value.status_code == 501

    @pytest.mark.asyncio
    async def test_stream_file_status_should_report_ready_on_cache_hit_for_sam(
        self, mock_db, mocker, tmp_path
    ):
        """Test that a cached DATA artifact reports ready without dispatch.

        Given:
            A SAM file with the DATA cache pre-populated and a wired
            workflow subsystem whose executor fails if dispatched.
        When:
            stream_file_status is called.
        Then:
            It should return ``{"ready": True}`` and ``ensure_workflow``
            MUST NOT be called.
        """
        # Arrange
        from cfdb.workflows import keys as key_utils
        from cfdb.workflows.cache import LocalFsCache
        from cfdb.workflows.models import ArtifactKind

        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        cache = LocalFsCache(tmp_path / "cache")
        src = tmp_path / "src"
        src.write_bytes(b"sam-cached-bytes")
        processor = BamIndexProcessor()
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
                raise AssertionError("status probe must NOT dispatch a workflow")

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
        result = await stream_file_status("4dn", "4DNFISAM01")

        # Assert
        assert result == {"ready": True}

    @pytest.mark.asyncio
    async def test_stream_file_status_should_report_not_ready_on_cache_miss_for_sam(
        self, mock_db, mocker
    ):
        """Test that a processable-but-uncached file reports not-ready.

        Given:
            A SAM file with an empty cache, a registered BAM/SAM
            processor, and an executor that fails if dispatched.
        When:
            stream_file_status is called.
        Then:
            It should return ``{"ready": False}`` and ``ensure_workflow``
            MUST NOT be called — the probe never dispatches.
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
                raise AssertionError("status probe must NOT dispatch a workflow")

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

        # Act
        result = await stream_file_status("4dn", "4DNFISAM01")

        # Assert
        assert result == {"ready": False}

    @pytest.mark.asyncio
    async def test_stream_file_status_should_report_ready_for_passthrough_format(
        self, mock_db, mocker
    ):
        """Test that a passthrough format reports ready immediately.

        Given:
            A CSV file (a PassthroughProcessor format) with a wired
            workflow subsystem whose cache and executor fail if consulted.
        When:
            stream_file_status is called.
        Then:
            It should return ``{"ready": True}`` — passthrough files
            stream directly, so the probe never consults the cache.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mocker.patch.object(api, "processor_registry", default_registry())

        class _FailIfConsulted:
            async def head(self, _k):
                raise AssertionError("passthrough must not consult cache")

            def get(self, *_a, **_kw):
                raise AssertionError

            async def put(self, *_a, **_kw):
                raise AssertionError

            async def delete(self, _k):
                return False

        mocker.patch.object(api, "cache", _FailIfConsulted())
        mocker.patch.object(api, "executor", object())

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
                "local_id": "4DNFICSV01",
                "filename": "x.csv",
                "md5": FIXTURE_MD5,
                "access_url": "drs://4dn/abc",
                "file_format": {"name": "CSV"},
            }
        ]

        # Act
        result = await stream_file_status("4dn", "4DNFICSV01")

        # Assert
        assert result == {"ready": True}

    @pytest.mark.asyncio
    async def test_stream_file_status_should_report_ready_for_bam_without_data_artifact(
        self, mock_db, mocker
    ):
        """Test that a BAM (no DATA artifact) reports ready for /data.

        Given:
            A 4DN BAM file with a registered ``BamIndexProcessor``. BAM
            advertises only an INDEX artifact (the source is already
            coordinate-sorted upstream), so /data falls through to direct
            streaming.
        When:
            stream_file_status is called.
        Then:
            It should return ``{"ready": True}`` without consulting the
            cache — the DATA path is a direct upstream stream.
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

        registry = ProcessorRegistry()
        registry.register(BamIndexProcessor())
        mocker.patch.object(api, "cache", _FailIfConsulted())
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "executor", object())

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

        # Act
        result = await stream_file_status("4dn", "4DNFIBAM01")

        # Assert
        assert result == {"ready": True}


def _make_encode_file_doc(**overrides) -> dict:
    doc = {
        "submission": "encode",
        "id_namespace": "tag:encodeproject.org,2017:",
        "local_id": "ENCFF268EYE",
        "filename": "ENCFF268EYE.bed",
        "md5": FIXTURE_MD5,
        "access_url": (
            "https://www.encodeproject.org/files/ENCFF268EYE"
            "/@@download/ENCFF268EYE.bed"
        ),
        "dcc": {"dcc_abbreviation": "ENCODE"},
        "file_format": {"name": "BED"},
        "size_in_bytes": 1024,
    }
    doc.update(overrides)
    return doc


def _cfdb_errors(caplog) -> list[logging.LogRecord]:
    # Matched across the whole cfdb logger tree rather than the data
    # router's own name, so the pin follows the reduction if it is ever
    # hoisted into _helpers alongside lookup_file_doc.
    return [
        record
        for record in caplog.records
        if record.name.startswith("cfdb.") and record.levelno >= logging.ERROR
    ]


class TestStreamFileMetadataReduction:
    """The file reference /data streams from is built off the document."""

    @pytest.mark.asyncio
    async def test_stream_file_should_log_no_error_when_the_request_succeeds(
        self, mock_db, mocker, caplog
    ):
        """Test that a served /data request produces no error signal.

        Given:
            A public ENCODE file document and an unwired workflow
            subsystem, so the request streams straight from upstream.
        When:
            stream_file is called for it.
        Then:
            It should return a 200 response and log no ERROR record from
            the data router.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mocker.patch.object(api, "processor_registry", None)
        mock_db.file.docs = [_make_encode_file_doc()]

        # Act
        with caplog.at_level(logging.INFO, logger="cfdb.api.routers.data"):
            response = await stream_file(
                "encode", "ENCFF268EYE", _make_request(), range=None
            )

        # Assert
        assert response.status_code == 200
        errors = _cfdb_errors(caplog)
        assert errors == [], [record.getMessage() for record in errors]

    @pytest.mark.asyncio
    async def test_stream_file_should_log_no_error_when_access_is_refused(
        self, mock_db, mocker, caplog
    ):
        """Test that a refused /data request produces only its own signal.

        Given:
            A HuBMAP file whose data_access_level is "consortium".
        When:
            stream_file is called for it.
        Then:
            It should raise the 403 and log no ERROR record from the data
            router, since the file reference is built before the access
            guard runs.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mocker.patch.object(api, "processor_registry", None)
        mock_db.dcc.docs = [_make_dcc_doc()]
        mock_db.file.docs = [_make_file_doc(access_level="consortium")]

        # Act
        with (
            caplog.at_level(logging.INFO, logger="cfdb.api.routers.data"),
            pytest.raises(HTTPException) as exc_info,
        ):
            await stream_file("hubmap", "file-1", _make_request(), range=None)

        # Assert
        assert exc_info.value.status_code == 403
        errors = _cfdb_errors(caplog)
        assert errors == [], [record.getMessage() for record in errors]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("access_url", [None, ""])
    async def test_stream_file_should_raise_501_when_no_access_url(
        self, mock_db, mocker, caplog, access_url
    ):
        """Test that a file with no access method mirrors /status's 501.

        Given:
            An ENCODE file document whose access_url is absent or empty.
        When:
            stream_file is called for it.
        Then:
            It should raise HTTPException(501) — the same code the
            /status probe reports for the same record — and log no ERROR
            record from the data router.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mocker.patch.object(api, "processor_registry", None)
        doc = _make_encode_file_doc()
        if access_url is None:
            del doc["access_url"]
        else:
            doc["access_url"] = access_url
        mock_db.file.docs = [doc]

        # Act
        with (
            caplog.at_level(logging.INFO, logger="cfdb.api.routers.data"),
            pytest.raises(HTTPException) as exc_info,
        ):
            await stream_file("encode", "ENCFF268EYE", _make_request(), range=None)

        # Assert
        assert exc_info.value.status_code == 501
        errors = _cfdb_errors(caplog)
        assert errors == [], [record.getMessage() for record in errors]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("filename", "expected_media_type"),
        [
            ("ENCFF268EYE.bed", "text/plain"),
            ("ENCFF268EYE.fastq.gz", "application/gzip"),
            (None, "application/octet-stream"),
            ("", "application/octet-stream"),
        ],
    )
    async def test_stream_file_should_carry_the_document_filename_downstream(
        self, mock_db, mocker, filename, expected_media_type
    ):
        """Test that the streamed response is labelled from the document.

        Given:
            An ENCODE file document whose filename is a BED name, a
            gzipped name, absent entirely, or present but empty — the
            last being the shape a bad upstream record produces.
        When:
            stream_file is called for it.
        Then:
            The response should carry the media type that name implies,
            falling back to a generic name and octet-stream whenever the
            document supplies no usable one.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mocker.patch.object(api, "processor_registry", None)
        doc = _make_encode_file_doc()
        if filename is None:
            del doc["filename"]
        else:
            doc["filename"] = filename
        mock_db.file.docs = [doc]

        # Act
        response = await stream_file(
            "encode", "ENCFF268EYE", _make_request(), range=None
        )

        # Assert
        assert response.media_type == expected_media_type
        assert response.headers["content-disposition"] == (
            f'attachment; filename="{filename or "file"}"'
        )

    @pytest.mark.asyncio
    async def test_stream_file_should_log_an_error_when_it_genuinely_fails(
        self, mock_db, mocker, caplog
    ):
        """Test that a real failure still produces an error signal.

        Given:
            A lookup that raises an unexpected exception.
        When:
            stream_file is called.
        Then:
            It should raise the 500 and log an ERROR record — the
            positive control that keeps the absence assertions above
            from passing on a broken capture path.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mocker.patch(
            "cfdb.api.routers.data.lookup_file_doc",
            side_effect=RuntimeError("lookup exploded"),
        )
        mock_db.file.docs = [_make_encode_file_doc()]

        # Act
        with (
            caplog.at_level(logging.INFO, logger="cfdb.api.routers.data"),
            pytest.raises(HTTPException) as exc_info,
        ):
            await stream_file("encode", "ENCFF268EYE", _make_request(), range=None)

        # Assert
        assert exc_info.value.status_code == 500
        assert _cfdb_errors(caplog) != []
