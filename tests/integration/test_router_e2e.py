"""End-to-end HTTP router tests backed by real Wool + real cache.

Exercises ``/data``, ``/index``, and ``/jobs`` flows through the
production router handlers with a live ``WoolExecutor`` dispatching
real samtools / tabix inside a ``wool.WorkerPool`` worker.

Router handlers are invoked directly (not through the ASGI layer) —
this avoids spinning up a full FastAPI TestClient while still
exercising the full workflow logic, cache lookup, and streaming path.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone

import pytest
from allpairspy import AllPairs
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse

from cfdb import api
from cfdb.api.routers.data import stream_file
from cfdb.api.routers.index import stream_index_file
from cfdb.api.routers.jobs import get_job_status
from cfdb.services import drs, locks
from cfdb.workflows.lock import JOBS_COLLECTION, get_job
from cfdb.workflows.models import JobRecord, JobStatus

from tests.integration.conftest import (
    CacheState,
    Endpoint,
    Format,
    Method,
    RangeShape,
    Scenario,
    _Request,
    _wait_for_terminal,
    make_file_meta,
)


pytestmark = pytest.mark.integration


async def _collect(stream) -> bytes:
    body = bytearray()
    async for chunk in stream:
        body.extend(chunk)
    return bytes(body)


@pytest.fixture()
def wired_api(
    install_jobs_index,
    integration_executor,
    integration_cache_root,
    mocker,
):
    """Bind executor/cache/registry onto the api module for router calls.

    The routers read ``api.cache``, ``api.processor_registry``, and
    ``api.executor`` to dispatch workflows. Assigning these from the
    integration executor lets the stream_file / stream_index_file
    handlers operate against a real workflow backend.
    """
    mocker.patch.object(api, "cache", integration_executor._cache)
    mocker.patch.object(
        api, "processor_registry", integration_executor._registry
    )
    mocker.patch.object(api, "executor", integration_executor)
    return integration_executor


async def _seed_db_with_bam(mock_db, sample, sample_server) -> dict:
    """Insert a BAM file_meta into the file collection and return it."""
    meta = make_file_meta(sample, base_url=sample_server)
    mock_db.dcc.docs = [
        {
            "dcc_abbreviation": "ENCODE",
            "project_id_namespace": "tag:encode.org,2020:",
        }
    ]
    doc = {
        **meta,
        "id_namespace": "tag:encode.org,2020:",
    }
    mock_db.files.docs = [doc]
    mock_db.file.docs = [doc]
    return doc


async def _seed_db_with_sample(
    mock_db,
    sample,
    sample_server,
    *,
    dcc: str = "ENCODE",
    submission: str | None = None,
    extra_files: list[dict] | None = None,
    local_id: str | None = None,
) -> dict:
    """Insert any sample's file_meta into the file collection and return it."""
    meta = make_file_meta(
        sample,
        base_url=sample_server,
        dcc=dcc,
        local_id=local_id,
        extra_files=extra_files,
    )
    sub = submission if submission is not None else dcc.lower()
    meta["submission"] = sub
    mock_db.dcc.docs = [
        {
            "dcc_abbreviation": dcc.upper(),
            "project_id_namespace": f"tag:{dcc.lower()}.example,2020:",
        }
    ]
    doc = {
        **meta,
        "id_namespace": f"tag:{dcc.lower()}.example,2020:",
    }
    mock_db.files.docs = [doc]
    mock_db.file.docs = [doc]
    return doc


async def _run_workflow_via_router(
    mock_db, sample, sample_server, wired_api, mocker
):
    """Hit /index once to trigger the workflow; return the job_id.

    /index is used as the trigger because ``BamIndexProcessor``
    advertises only the INDEX artifact for BAMs (DCC-published BAMs
    are pre-sorted, so /data falls through to upstream streaming and
    never dispatches a workflow). /index produces the BAI, which is
    what the cache is for, and the dispatch path is identical apart
    from which artifact_kind ends up in the cache.
    """
    doc = await _seed_db_with_bam(mock_db, sample, sample_server)
    mocker.patch.object(locks, "wait_for_cutover", return_value=None)

    resp = await stream_index_file(
        "encode", doc["local_id"], _Request(), range=None
    )
    assert isinstance(resp, JSONResponse)
    assert resp.status_code == 202
    payload = json.loads(bytes(resp.body).decode())
    job_id = payload["job_id"]
    await _wait_for_terminal(mock_db, job_id)
    return doc, job_id


def _range_header_for_shape(shape: RangeShape, size: int) -> tuple[str, int, int]:
    """Build a ``Range:`` header value for ``shape`` plus its expected slice."""
    if shape is RangeShape.EXPLICIT:
        return ("bytes=0-99", 0, min(99, size - 1))
    if shape is RangeShape.OPEN_ENDED:
        return (f"bytes={size // 2}-", size // 2, size - 1)
    if shape is RangeShape.SUFFIX:
        return ("bytes=-128", max(0, size - 128), size - 1)
    if shape is RangeShape.CLAMPED:
        return (f"bytes=10-{size + 1_000_000}", 10, size - 1)
    raise ValueError(f"Unknown RangeShape: {shape!r}")


# A flat list of (Format, RangeShape, Method) tuples so the
# range-axis parametrize call exposes a stable readable id for each
# pairwise row.
_RANGE_ROWS: list[tuple[Format, RangeShape, Method]] = []
for _row in AllPairs(
    [
        [Format.BAM, Format.VCF, Format.BED],
        list(RangeShape),
        [Method.GET],
    ]
):
    _fmt = next(v for v in _row if isinstance(v, Format))
    _shape = next(v for v in _row if isinstance(v, RangeShape))
    _method = next(v for v in _row if isinstance(v, Method))
    _RANGE_ROWS.append((_fmt, _shape, _method))


_RANGE_IDS = [f"{fmt.name}-{shape.name}-{method.name}" for fmt, shape, method in _RANGE_ROWS]


class TestRouterE2E:
    @pytest.mark.asyncio
    async def test_stream_index_file_should_return_202_then_200_across_workflow_run(
        self, samples, sample_server, wired_api, mock_db, mocker, xfail_known_bugs
    ):
        """Test the full /index miss → dispatch → cache-hit-stream cycle.

        Given:
            A pre-sorted BAM file in the DB and a cold cache.
        When:
            stream_index_file is called on cache miss, the workflow
            completes, and stream_index_file is called again.
        Then:
            First response is 202 with a Location header; second
            response is a 200 StreamingResponse whose body is the
            cached BAI bytes.
        """
        scenario = Scenario(format=Format.BAM, endpoint=Endpoint.INDEX)

        async def _body():
            # Arrange & Act
            doc, job_id = await _run_workflow_via_router(
                mock_db, samples["BAM"], sample_server, wired_api, mocker
            )

            # Act again — cache hit now.
            second = await stream_index_file(
                "encode", doc["local_id"], _Request(), range=None
            )

            # Assert
            assert isinstance(second, StreamingResponse)
            assert second.status_code == 200
            body = await _collect(second.body_iterator)
            assert len(body) > 0

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.asyncio
    async def test_stream_index_file_should_honor_range_request_on_cached_artifact(
        self, samples, sample_server, wired_api, mock_db, mocker, xfail_known_bugs
    ):
        """Test that /index range variants return the exact requested bytes.

        Given:
            A completed BAM workflow whose BAI is in cache; the artifact
            bytes are read once for ground-truth comparison.
        When:
            stream_index_file is called with each ``RangeShape`` variant
            (explicit / open-ended / suffix / clamped).
        Then:
            Each response is 206, ``Content-Range`` is set, and the body
            equals ``payload[start : end + 1]``.
        """
        scenario = Scenario(format=Format.BAM, endpoint=Endpoint.INDEX)

        async def _body():
            # Arrange
            doc, job_id = await _run_workflow_via_router(
                mock_db, samples["BAM"], sample_server, wired_api, mocker
            )
            final = await get_job(mock_db, job_id)
            assert final is not None
            cache_root = wired_api._cache.root
            cached_path = cache_root / final.artifact_cache_keys["index"]
            payload = cached_path.read_bytes()
            size = len(payload)
            # The fixture's BAI must be large enough to exercise mid-file
            # ranges meaningfully.
            assert size > 256

            for shape in RangeShape:
                header, expected_start, expected_end = _range_header_for_shape(
                    shape, size
                )

                # Act
                resp = await stream_index_file(
                    "encode", doc["local_id"], _Request(), range=header
                )

                # Assert
                assert isinstance(resp, StreamingResponse), shape
                assert resp.status_code == 206, shape
                assert (
                    resp.headers["content-range"]
                    == f"bytes {expected_start}-{expected_end}/{size}"
                ), shape
                body = await _collect(resp.body_iterator)
                assert body == payload[expected_start : expected_end + 1], shape

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.asyncio
    async def test_stream_index_file_should_return_416_when_range_starts_past_eof(
        self, samples, sample_server, wired_api, mock_db, mocker, xfail_known_bugs
    ):
        """Test that an unsatisfiable /index range yields 416 with Content-Range.

        Given:
            A completed BAM workflow whose BAI is in cache.
        When:
            stream_index_file is called with a Range whose start byte
            exceeds the cached artifact's size.
        Then:
            HTTPException(416) is raised and the response carries a
            ``Content-Range: bytes */<size>`` header so the client can
            recover with a fresh range request.
        """
        scenario = Scenario(format=Format.BAM, endpoint=Endpoint.INDEX)

        async def _body():
            # Arrange
            doc, job_id = await _run_workflow_via_router(
                mock_db, samples["BAM"], sample_server, wired_api, mocker
            )
            final = await get_job(mock_db, job_id)
            assert final is not None
            cache_root = wired_api._cache.root
            cached_path = cache_root / final.artifact_cache_keys["index"]
            size = cached_path.stat().st_size

            # Act & assert
            with pytest.raises(HTTPException) as exc_info:
                await stream_index_file(
                    "encode",
                    doc["local_id"],
                    _Request(),
                    range=f"bytes={size + 1}-{size + 100}",
                )
            assert exc_info.value.status_code == 416
            assert exc_info.value.headers["Content-Range"] == f"bytes */{size}"

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.asyncio
    async def test_stream_index_file_should_return_404_on_head_cache_miss_end_to_end(
        self, samples, sample_server, wired_api, mock_db, mocker
    ):
        """Test that HEAD on /index cache miss never dispatches a workflow.

        Given:
            A BAM file in the DB with an empty cache.
        When:
            stream_index_file is called with method HEAD.
        Then:
            An HTTPException(404) is raised; no JobRecord is persisted
            for the file's workflow_key (no workflow was dispatched).
        """
        # Arrange
        doc = await _seed_db_with_bam(mock_db, samples["BAM"], sample_server)
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file(
                "encode", doc["local_id"], _Request("HEAD"), range=None
            )
        assert exc_info.value.status_code == 404
        records_for_file = [
            d for d in mock_db.jobs.docs if d.get("local_id") == doc["local_id"]
        ]
        assert records_for_file == []

    @pytest.mark.asyncio
    async def test_get_job_status_should_return_completed_after_workflow_run(
        self, samples, sample_server, wired_api, mock_db, mocker, xfail_known_bugs
    ):
        """Test that /jobs/{id} reports the terminal artifact keys.

        Given:
            A completed BAM workflow job. Pre-sorted BAMs only produce
            an INDEX artifact; ``/data`` falls through to upstream.
        When:
            get_job_status is awaited with the job's id.
        Then:
            Response shows status=completed, stages_done=["index"],
            and only the INDEX artifact present (no data).
        """
        scenario = Scenario(format=Format.BAM, endpoint=Endpoint.JOBS)

        async def _body():
            # Arrange
            _, job_id = await _run_workflow_via_router(
                mock_db, samples["BAM"], sample_server, wired_api, mocker
            )

            # Act
            payload = await get_job_status(job_id)

            # Assert
            assert payload.status == JobStatus.COMPLETED.value
            assert payload.stages_done == ["index"]
            assert payload.artifacts == {"index": payload.artifacts["index"]}
            assert "data" not in payload.artifacts
            assert payload.error is None

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.asyncio
    async def test_stream_file_should_fall_through_for_pre_sorted_bam(
        self, samples, sample_server, wired_api, mock_db, mocker
    ):
        """Test that /data for a BAM falls through to direct streaming.

        Given:
            A pre-sorted BAM file in the DB whose access_url points at
            the local sample HTTP server.
        When:
            stream_file is called.
        Then:
            No workflow is dispatched (no JobRecord persisted) and the
            response either streams from upstream or surfaces an
            upstream-shaped error — anything except a 202 from the
            workflow path.
        """
        # Arrange
        doc = await _seed_db_with_bam(mock_db, samples["BAM"], sample_server)
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        # Act
        try:
            resp = await stream_file(
                "encode", doc["local_id"], _Request(), range=None
            )
        except HTTPException as exc:
            assert exc.status_code != 202
        else:
            assert not isinstance(resp, JSONResponse) or resp.status_code != 202

        # Assert — workflow was not dispatched.
        records_for_file = [
            d for d in mock_db.jobs.docs if d.get("local_id") == doc["local_id"]
        ]
        assert records_for_file == []

    @pytest.mark.asyncio
    async def test_stream_file_should_serve_passthrough_format_without_dispatching(
        self, samples, sample_server, wired_api, mock_db, mocker, tmp_path
    ):
        """Test that /data on a CSV file passes through without dispatch.

        Given:
            A CSV file_meta seeded into the file collection, the
            workflow subsystem wired up, and ``drs.stream_from_url``
            patched to return fixed bytes.
        When:
            ``stream_file("encode", local_id, GET, range=None)`` is
            awaited.
        Then:
            The router returns a streaming-shaped response (not a 202
            JSON envelope); no JobRecord is persisted; the registry
            resolves the file to the ``PassthroughProcessor``.
        """
        # Arrange — register a minimal SampleFile-shaped CSV inline.
        csv_path = tmp_path / "small.csv"
        csv_path.write_text("a,b,c\n1,2,3\n")
        from tests.integration.fixtures.make_samples import SampleFile, _md5

        csv_sample = SampleFile(
            path=csv_path,
            md5=_md5(csv_path),
            format="CSV",
        )
        doc = await _seed_db_with_sample(
            mock_db,
            csv_sample,
            sample_server,
            local_id="ENCFF-csv",
        )
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        async def fake_stream(*_args, **_kwargs):
            yield b"a,b,c\n1,2,3\n"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        # Act
        resp = await stream_file(
            "encode", doc["local_id"], _Request(), range=None
        )

        # Assert
        # PassthroughProcessor is wired but advertises no artifacts,
        # so the workflow branch returns None and the router falls
        # through to the existing /data streaming path. The exact
        # response shape depends on the DCC routing (ENCODE uses the
        # direct-streaming path, so we get a StreamingResponse).
        assert not isinstance(resp, JSONResponse) or resp.status_code != 202
        records_for_file = [
            d for d in mock_db.jobs.docs if d.get("local_id") == doc["local_id"]
        ]
        assert records_for_file == []
        # Registry resolves to PassthroughProcessor for CSV.
        from cfdb.workflows.processors.passthrough import PassthroughProcessor
        from cfdb.workflows.processors.registry import ProcessorRegistry

        # The wired registry only has BAM + tabix processors, not
        # passthrough. Build a fresh probe registry to confirm the
        # passthrough lookup behavior.
        probe = ProcessorRegistry()
        probe.register(PassthroughProcessor())
        assert isinstance(probe.lookup_for(doc), PassthroughProcessor)

    @pytest.mark.asyncio
    async def test_stream_index_file_should_return_404_for_passthrough_format_without_sidecar(
        self, samples, sample_server, wired_api, mock_db, mocker, tmp_path
    ):
        """Test that /index on bigWig with no sidecar returns 404.

        Given:
            A bigWig file_meta with no ``extra.extra_files`` sidecar
            and the workflow subsystem wired up.
        When:
            ``stream_index_file`` is awaited.
        Then:
            An ``HTTPException(404, "No index file available for this
            file format")`` is raised; no JobRecord is persisted.
        """
        # Arrange
        bw_path = tmp_path / "small.bw"
        bw_path.write_bytes(b"\x00" * 64)
        from tests.integration.fixtures.make_samples import SampleFile, _md5

        bw_sample = SampleFile(
            path=bw_path,
            md5=_md5(bw_path),
            format="bigWig",
        )
        doc = await _seed_db_with_sample(
            mock_db,
            bw_sample,
            sample_server,
            local_id="ENCFF-bigwig",
        )
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        # Wire a PassthroughProcessor into the registry so the format
        # is "known" — bigWig is handled by PassthroughProcessor.
        from cfdb.workflows.processors.passthrough import PassthroughProcessor

        wired_api._registry.register(PassthroughProcessor())

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file(
                "encode", doc["local_id"], _Request(), range=None
            )
        assert exc_info.value.status_code == 404
        assert "No index file available" in str(exc_info.value.detail)
        records_for_file = [
            d for d in mock_db.jobs.docs if d.get("local_id") == doc["local_id"]
        ]
        assert records_for_file == []

    @pytest.mark.asyncio
    async def test_stream_file_should_skip_workflow_when_raw_query_param_is_true(
        self, samples, sample_server, wired_api, mock_db, mocker
    ):
        """Test that ?raw=true bypasses the workflow dispatch branch.

        Given:
            A BAM file in the DB and a cold cache.
        When:
            ``stream_file(..., raw=True)`` is awaited.
        Then:
            The router falls through to the upstream direct-stream path
            (not a 202); no JobRecord is persisted.
        """
        # Arrange
        doc = await _seed_db_with_bam(mock_db, samples["BAM"], sample_server)
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        # Act
        try:
            resp = await stream_file(
                "encode", doc["local_id"], _Request(), range=None, raw=True
            )
        except HTTPException as exc:
            assert exc.status_code != 202
        else:
            assert not isinstance(resp, JSONResponse) or resp.status_code != 202

        # Assert
        records_for_file = [
            d for d in mock_db.jobs.docs if d.get("local_id") == doc["local_id"]
        ]
        assert records_for_file == []

    @pytest.mark.asyncio
    async def test_stream_index_file_should_prefer_fourdn_sidecar_over_workflow_dispatch(
        self, samples, sample_server, wired_api, mock_db, mocker, tmp_path
    ):
        """Test that a 4DN sidecar short-circuits the workflow path.

        Given:
            A 4DN BED file_meta carrying ``extra.extra_files`` with a
            ``tbi`` sidecar entry; the workflow subsystem wired up;
            ``drs.stream_from_url`` patched to return fixed sidecar
            bytes; ``validate_outbound_url`` patched to permit the
            sample-server URL.
        When:
            ``stream_index_file("4dn", local_id, GET, None)`` is awaited.
        Then:
            A ``StreamingResponse`` carrying the sidecar bytes is
            returned; no JobRecord is persisted; the workflow path is
            short-circuited.
        """
        # Arrange
        sample = samples["BED"]
        sidecar = [
            {
                "file_format": "tbi",
                "href": f"/output/files/{sample.path.name}.tbi",
                "file_size": 256,
            }
        ]
        doc = await _seed_db_with_sample(
            mock_db,
            sample,
            sample_server,
            dcc="4DN",
            submission="4dn",
            extra_files=sidecar,
            local_id="4DNFI-bed",
        )
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        # Patch the urlsafe validation so the sidecar URL passes the
        # outbound allowlist (the test uses a 127.0.0.1 sample server,
        # which the env var permits anyway, but we patch the function
        # used by the router for robustness).
        from cfdb.workflows import urlsafe

        mocker.patch.object(urlsafe, "validate_outbound_url", return_value=None)

        # Patch stream_from_url so the test doesn't depend on a real
        # 4DN-shaped URL — return fixed sidecar bytes.
        sidecar_bytes = b"FAKE-SIDECAR-BYTES-256-" + b"x" * 232

        async def fake_stream(*_args, **_kwargs):
            yield sidecar_bytes

        # Patch on the module where the index router imported it from.
        from cfdb.api.routers import index as index_module

        mocker.patch.object(index_module.drs, "stream_from_url", fake_stream)

        # Act
        resp = await stream_index_file(
            "4dn", doc["local_id"], _Request(), range=None
        )

        # Assert
        assert isinstance(resp, StreamingResponse)
        body = await _collect(resp.body_iterator)
        assert body == sidecar_bytes
        records_for_file = [
            d for d in mock_db.jobs.docs if d.get("local_id") == doc["local_id"]
        ]
        assert records_for_file == []

    @pytest.mark.asyncio
    async def test_get_job_status_should_return_404_when_job_id_unknown(
        self, wired_api, mock_db, mocker
    ):
        """Test that /jobs/{id} on a missing id returns 404.

        Given:
            A wired API and an empty ``jobs`` collection.
        When:
            ``get_job_status("does-not-exist")`` is awaited.
        Then:
            ``HTTPException(404, "Job not found")`` is raised.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await get_job_status("does-not-exist")
        assert exc_info.value.status_code == 404
        assert "Job not found" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_get_job_status_should_report_failed_status_with_scrubbed_error_after_processor_failure(
        self, wired_api, mock_db, mocker
    ):
        """Test that /jobs/{id} returns a FAILED record with a scrubbed error.

        Given:
            A wired API and a JobRecord whose status is FAILED and
            whose ``error`` field has already been scrubbed (via the
            ``release_workflow`` write-through path) so the persisted
            text carries no absolute filesystem paths.
        When:
            ``get_job_status(job_id)`` is awaited.
        Then:
            ``status == "failed"``; ``error`` carries the canonical
            scrubbed form (``<path>`` token replacing the original
            path); ``artifacts`` is empty.
        """
        # Arrange — persist a synthetic FAILED record. The lock module
        # scrubs paths at release_workflow time; here we install the
        # already-scrubbed form so the test verifies the get_job_status
        # contract over the persisted record.
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        now = datetime.now(timezone.utc)
        failed_record = JobRecord(
            job_id=str(uuid.uuid4()),
            workflow_key="encode/x/d41d8cd98f00b204e9800998ecf8427e/v1",
            status=JobStatus.FAILED,
            dcc="encode",
            local_id="x",
            md5="d41d8cd98f00b204e9800998ecf8427e",
            pipeline_version=1,
            submitted_at=now,
            updated_at=now,
            error="boom <path>",
        )
        mock_db[JOBS_COLLECTION].docs.append(failed_record.to_mongo())

        # Act
        payload = await get_job_status(failed_record.job_id)

        # Assert
        assert payload.status == JobStatus.FAILED.value
        assert payload.error == "boom <path>"
        # The persisted error MUST NOT carry a multi-segment path.
        assert re.search(r"/[A-Za-z][A-Za-z0-9_.-]*/[A-Za-z]", payload.error) is None
        assert payload.artifacts == {}

    @pytest.mark.parametrize(
        "fmt,shape,method", _RANGE_ROWS, ids=_RANGE_IDS
    )
    @pytest.mark.asyncio
    async def test_stream_index_file_range_request_should_match_pairwise_axis(
        self,
        samples,
        sample_server,
        wired_api,
        mock_db,
        mocker,
        fmt: Format,
        shape: RangeShape,
        method: Method,
        xfail_known_bugs,
    ):
        """Test that range requests honor the pairwise axis combinations.

        Given:
            A pairwise sweep over ``(Format ∈ {BAM, VCF, BED},
            RangeShape ∈ {EXPLICIT, OPEN_ENDED, SUFFIX, CLAMPED},
            Method ∈ {GET})`` — each scenario warms the cache then
            issues a range request.
        When:
            ``stream_index_file(..., range=header)`` is awaited.
        Then:
            Each response is 206; ``Content-Range`` carries the
            expected ``bytes {start}-{end}/{size}`` tuple; the body is
            ``payload[start : end + 1]``.
        """
        scenario = Scenario(
            format=fmt, endpoint=Endpoint.INDEX, method=method, cache_state=CacheState.WARM
        )

        async def _body():
            # Arrange
            sample_key = {Format.BAM: "BAM", Format.VCF: "VCF", Format.BED: "BED"}[fmt]
            sample = samples[sample_key]
            doc = await _seed_db_with_sample(
                mock_db,
                sample,
                sample_server,
                local_id=f"ENCFF-range-{fmt.name}",
            )
            mocker.patch.object(locks, "wait_for_cutover", return_value=None)
            # Drive the workflow so the index artifact is in cache.
            resp = await stream_index_file(
                "encode", doc["local_id"], _Request(), range=None
            )
            assert isinstance(resp, JSONResponse)
            payload_json = json.loads(bytes(resp.body).decode())
            job_id = payload_json["job_id"]
            await _wait_for_terminal(mock_db, job_id)

            final = await get_job(mock_db, job_id)
            assert final is not None
            cache_root = wired_api._cache.root
            cached_path = cache_root / final.artifact_cache_keys["index"]
            cached_bytes = cached_path.read_bytes()
            size = len(cached_bytes)
            assert size > 256, (
                f"Cached index for {fmt.name} must be >256 bytes to exercise "
                f"range shape {shape.name}"
            )

            header, expected_start, expected_end = _range_header_for_shape(shape, size)

            # Act
            range_resp = await stream_index_file(
                "encode",
                doc["local_id"],
                _Request(method.value),
                range=header,
            )

            # Assert
            assert isinstance(range_resp, StreamingResponse)
            assert range_resp.status_code == 206
            assert (
                range_resp.headers["content-range"]
                == f"bytes {expected_start}-{expected_end}/{size}"
            )
            body = await _collect(range_resp.body_iterator)
            assert body == cached_bytes[expected_start : expected_end + 1]

        await xfail_known_bugs(scenario, _body)
