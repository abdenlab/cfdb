"""End-to-end tileset preparation over a real wool worker.

Exercises the ``POST /tilesets/{dcc}/{local_id}`` preparation channel all
the way through: the router claims a workflow, the ``WoolExecutor``
dispatches it across the cloudpickle boundary into a real
``wool.WorkerPool`` worker, ``MatrixTilesetProcessor`` downloads the
contact map from the sample HTTP server and commits (or refuses) the
TILESET artifact, and the readiness probe flips. Every other tile test
runs the processor in-process with ``download_source`` patched — this
module is where the worker subprocess actually builds the artifact.
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone

import pytest

pytest.importorskip("cooler")

import numpy as np
from allpairspy import AllPairs
from fastapi import HTTPException
from fastapi.responses import JSONResponse

from cfdb import api
from cfdb.api.routers.tilesets import prepare_tileset, tileset_status
from cfdb.services import locks
from cfdb.workflows import TILESET_MAX_SOURCE_BYTES, WORKFLOW_MAX_ACTIVE
from cfdb.workflows.lock import JOBS_COLLECTION, get_job
from cfdb.workflows.models import ArtifactKind, JobStatus
from cfdb.workflows.processors.matrix import MatrixTilesetProcessor

from tests.integration.conftest import (
    CacheState,
    Concurrency,
    Endpoint,
    Format,
    PickleBoundary,
    Scenario,
    _wait_for_terminal,
    filter_func,
    make_file_meta,
)
from tests.integration.fixtures.make_samples import SampleFile, _md5

pytestmark = pytest.mark.integration

#: Multi-segment absolute paths must never survive error scrubbing into a
#: persisted JobRecord (mirrors the /jobs router e2e pin).
_PATH_LEAK = re.compile(r"/[A-Za-z][A-Za-z0-9_.-]*/[A-Za-z]")


@pytest.fixture()
def jobs_mutex_index(mock_db):
    """Install the partial-unique mutex index for real.

    The shared ``install_jobs_index`` fixture calls the FakeCollection's
    *async* ``create_index`` without awaiting it, so the coroutine never
    runs and the mutex index is never installed — concurrent claims then
    insert duplicate active rows instead of funneling. Registered here
    through the synchronous ``register_index`` seam so the funnel tests
    in this module exercise the real DuplicateKeyError attach path.
    """
    mock_db.jobs.register_index(
        {"workflow_key": 1},
        unique=True,
        partialFilterExpression={"active": True},
    )
    return mock_db


@pytest.fixture()
def wired_api(jobs_mutex_index, integration_executor, mocker):
    """Bind integration executor / cache / registry onto the api module."""
    mocker.patch.object(api, "cache", integration_executor._cache)
    mocker.patch.object(
        api, "processor_registry", integration_executor._registry
    )
    mocker.patch.object(api, "executor", integration_executor)
    return integration_executor


def _job_id_from_202(resp) -> str:
    assert isinstance(resp, JSONResponse)
    assert resp.status_code == 202
    payload = json.loads(bytes(resp.body).decode())
    return payload["job_id"]


def _seed_contact_map(
    mock_db,
    sample: SampleFile,
    sample_server: str,
    *,
    local_id: str,
    filename: str,
) -> dict:
    """Insert a 4DN contact-map file_meta into the file collections."""
    meta = make_file_meta(
        sample,
        base_url=sample_server,
        dcc="4DN",
        local_id=local_id,
        filename=filename,
        file_format_name="HDF5",
    )
    doc = {**meta, "id_namespace": "tag:4dn.example,2020:"}
    mock_db.files.docs = [doc]
    mock_db.file.docs = [doc]
    return doc


def _records_for(mock_db, local_id: str) -> list[dict]:
    return [
        d
        for d in mock_db[JOBS_COLLECTION].docs
        if d.get("local_id") == local_id
    ]


def _decode(payload: dict) -> np.ndarray:
    """Decode a dense tile payload the way a HiGlass client would."""
    import base64

    flat = np.frombuffer(
        base64.b64decode(payload["dense"]), dtype=payload["dtype"]
    )
    side = int(round(len(flat) ** 0.5))
    return flat.reshape(side, side)


class TestTilesetPrepareE2E:
    @pytest.mark.asyncio
    async def test_prepare_tileset_should_build_a_servable_mcool_artifact_end_to_end(
        self,
        samples,
        sample_server,
        wired_api,
        mock_db,
        mocker,
        tmp_path,
        xfail_known_bugs,
    ):
        """Test the full POST → worker build → probe → idempotent-POST cycle.

        Given:
            A real multi-resolution mcool served over the sample HTTP
            server, a 4DN contact-map document in the DB, and a cold
            cache — the first scenario to carry MatrixTilesetProcessor
            across the wool cloudpickle boundary.
        When:
            POST /tilesets claims a workflow, the job is polled to a
            terminal state, the readiness probe is called, and a second
            POST is issued.
        Then:
            It should answer 202 with a /jobs Location; complete with
            stages_done == ["tileset"] and the tileset cache key; commit
            bytes that open as a cooler whose decoded tile matches a
            direct read; flip the probe to ready:true; and answer the
            second POST with 200 while exactly one JobRecord exists.
        """
        pytest.importorskip("clodius.tiles_v2.cooler")
        import cooler

        from cfdb.api.routers.tiles import tiles
        from cfdb.tilesets.service import build_service

        scenario = Scenario(
            format=Format.MCOOL,
            endpoint=Endpoint.TILESETS,
            cache_state=CacheState.COLD,
            pickle_boundary=PickleBoundary.WOOL_WORKER,
        )

        async def _body():
            # Arrange
            sample = samples["mcool"]
            assert sample is not None
            doc = _seed_contact_map(
                mock_db,
                sample,
                sample_server,
                local_id="4DNFI-prep-mcool",
                filename="sample.mcool",
            )
            mocker.patch.object(locks, "wait_for_cutover", return_value=None)
            assert await tileset_status("4dn", doc["local_id"]) == {
                "ready": False
            }

            # Act — POST dispatches; poll the job to terminal.
            first = await prepare_tileset("4dn", doc["local_id"])
            job_id = _job_id_from_202(first)
            assert first.headers["location"] == f"/jobs/{job_id}"
            await _wait_for_terminal(mock_db, job_id)

            # Assert — the record carries the tileset stage and key.
            final = await get_job(mock_db, job_id)
            assert final is not None
            assert final.status is JobStatus.COMPLETED
            assert final.stages_done == ["tileset"]
            key = final.artifact_cache_keys["tileset"]

            # Assert — the committed bytes open as a cooler and a decoded
            # tile matches a direct read of the same corner.
            cached_path = api.cache.path_for(key)
            clr = cooler.Cooler(f"{cached_path}::/resolutions/4")
            service = build_service(
                root=tmp_path / "tilesets",
                cache_provider=lambda: api.cache,
                disk_cache_bytes=10**9,
                open_max=4,
                tile_cache_bytes=10**7,
                threads=2,
                hydrate_timeout_s=60,
            )
            mocker.patch.object(api, "tileset_service", service)
            try:
                uid = f"4dn/{doc['local_id']}"
                payload = (await tiles(d=[f"{uid}.0.0.0"]))[f"{uid}.0.0.0"]
                block = _decode(payload)
                assert block.shape == (256, 256)
                expected = clr.matrix(balance=False).fetch("c1")[:8, :8]
                np.testing.assert_allclose(
                    block[:8, :8], expected.astype(np.float32)
                )
            finally:
                await service.aclose()

            # Assert — the probe flips and a second POST is idempotent.
            assert await tileset_status("4dn", doc["local_id"]) == {
                "ready": True
            }
            second = await prepare_tileset("4dn", doc["local_id"])
            assert isinstance(second, JSONResponse)
            assert second.status_code == 200
            assert json.loads(bytes(second.body).decode()) == {"ready": True}
            assert len(_records_for(mock_db, doc["local_id"])) == 1

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.asyncio
    async def test_prepare_tileset_should_coarsen_a_flat_cool_inside_the_worker(
        self,
        samples,
        sample_server,
        wired_api,
        mock_db,
        mocker,
        xfail_known_bugs,
    ):
        """Test that a flat .cool is zoomified by the worker subprocess.

        Given:
            A real single-resolution .cool served over the sample HTTP
            server — the shape clodius refuses without a resolutions
            group — and the same live wool stack.
        When:
            POST /tilesets runs the same lifecycle to completion.
        Then:
            It should complete, and the committed artifact should carry
            a resolutions ladder starting at the source binsize —
            proving ``cooler.zoomify_cooler`` ran inside the worker.
        """
        import h5py

        scenario = Scenario(
            format=Format.COOL,
            endpoint=Endpoint.TILESETS,
            cache_state=CacheState.COLD,
            pickle_boundary=PickleBoundary.WOOL_WORKER,
        )

        async def _body():
            # Arrange
            sample = samples["cool"]
            assert sample is not None
            doc = _seed_contact_map(
                mock_db,
                sample,
                sample_server,
                local_id="4DNFI-prep-cool",
                filename="sample.cool",
            )
            mocker.patch.object(locks, "wait_for_cutover", return_value=None)

            # Act
            resp = await prepare_tileset("4dn", doc["local_id"])
            job_id = _job_id_from_202(resp)
            await _wait_for_terminal(mock_db, job_id)

            # Assert
            final = await get_job(mock_db, job_id)
            assert final is not None
            assert final.status is JobStatus.COMPLETED
            assert final.stages_done == ["tileset"]
            key = final.artifact_cache_keys["tileset"]
            cached_path = api.cache.path_for(key)
            with h5py.File(cached_path, "r") as handle:
                assert "resolutions" in handle
                ladder = sorted(int(r) for r in handle["resolutions"])
            # The fixture cooler is built at binsize 4, so the worker's
            # ladder must start there.
            assert ladder[0] == 4
            assert await tileset_status("4dn", doc["local_id"]) == {
                "ready": True
            }

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.asyncio
    async def test_prepare_tileset_should_fail_cleanly_when_upstream_bytes_are_not_hdf5(
        self,
        sample_data_root,
        sample_server,
        wired_api,
        mock_db,
        mocker,
        xfail_known_bugs,
    ):
        """Test that upstream garbage fails the job without poisoning the cache.

        Given:
            Non-HDF5 bytes named sample.mcool served upstream, so the
            worker's h5py open fails after a real download.
        When:
            POST /tilesets runs to a terminal state, the probe is
            called, and a second POST is issued.
        Then:
            It should record FAILED with a path-scrubbed error, leave
            ``cache.head`` None for the tileset key (garbage never lands
            in cache), report ready:false, and let the second POST claim
            a fresh job because the mutex was released.
        """
        scenario = Scenario(
            format=Format.MCOOL,
            endpoint=Endpoint.TILESETS,
            cache_state=CacheState.COLD,
            pickle_boundary=PickleBoundary.WOOL_WORKER,
        )

        async def _body():
            # Arrange — write garbage into the served sample directory.
            garbage_path = sample_data_root / "garbage-not-hdf5.mcool"
            garbage_path.write_bytes(
                b"cfdb-integration: definitely not HDF5 bytes\n" * 8
            )
            garbage = SampleFile(
                path=garbage_path, md5=_md5(garbage_path), format="mcool"
            )
            doc = _seed_contact_map(
                mock_db,
                garbage,
                sample_server,
                local_id="4DNFI-prep-garbage",
                filename="sample.mcool",
            )
            mocker.patch.object(locks, "wait_for_cutover", return_value=None)

            # Act
            first = await prepare_tileset("4dn", doc["local_id"])
            first_job_id = _job_id_from_202(first)
            await _wait_for_terminal(mock_db, first_job_id)

            # Assert — FAILED, scrubbed, nothing committed.
            final = await get_job(mock_db, first_job_id)
            assert final is not None
            assert final.status is JobStatus.FAILED
            assert final.error
            assert _PATH_LEAK.search(final.error) is None
            key = MatrixTilesetProcessor().cache_key_for(
                doc, ArtifactKind.TILESET
            )
            assert await api.cache.head(key) is None
            assert await tileset_status("4dn", doc["local_id"]) == {
                "ready": False
            }

            # Act & assert — the mutex is released, so a retry claims a
            # NEW job rather than attaching to the failed one.
            second = await prepare_tileset("4dn", doc["local_id"])
            second_job_id = _job_id_from_202(second)
            assert second_job_id != first_job_id
            await _wait_for_terminal(mock_db, second_job_id)

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.asyncio
    async def test_prepare_tileset_should_refuse_an_oversized_source_named_by_the_cap(
        self,
        samples,
        sample_server,
        wired_api,
        mock_db,
        mocker,
        xfail_known_bugs,
    ):
        """Test that the source-size cap fails the job before downloading.

        Given:
            A contact-map document whose integer ``size_in_bytes``
            exceeds ``CFDB_TILESET_MAX_SOURCE_BYTES``, dispatched through
            the real wool stack.
        When:
            POST /tilesets runs to a terminal state and is then retried.
        Then:
            It should record FAILED with an error naming the source cap
            and leaking no path, keep the probe at ready:false, and let
            the retry claim a fresh job.
        """
        if not TILESET_MAX_SOURCE_BYTES:
            pytest.skip(
                "CFDB_TILESET_MAX_SOURCE_BYTES=0 disables the cap in this "
                "environment"
            )

        scenario = Scenario(
            format=Format.MCOOL,
            endpoint=Endpoint.TILESETS,
            cache_state=CacheState.COLD,
            pickle_boundary=PickleBoundary.WOOL_WORKER,
        )

        async def _body():
            # Arrange
            sample = samples["mcool"]
            assert sample is not None
            doc = _seed_contact_map(
                mock_db,
                sample,
                sample_server,
                local_id="4DNFI-prep-oversize",
                filename="sample.mcool",
            )
            doc["size_in_bytes"] = TILESET_MAX_SOURCE_BYTES + 1
            mocker.patch.object(locks, "wait_for_cutover", return_value=None)

            # Act
            first = await prepare_tileset("4dn", doc["local_id"])
            first_job_id = _job_id_from_202(first)
            await _wait_for_terminal(mock_db, first_job_id)

            # Assert
            final = await get_job(mock_db, first_job_id)
            assert final is not None
            assert final.status is JobStatus.FAILED
            assert final.error is not None
            assert "CFDB_TILESET_MAX_SOURCE_BYTES" in final.error
            assert _PATH_LEAK.search(final.error) is None
            assert await tileset_status("4dn", doc["local_id"]) == {
                "ready": False
            }

            # Act & assert — retry claims a fresh job.
            second = await prepare_tileset("4dn", doc["local_id"])
            second_job_id = _job_id_from_202(second)
            assert second_job_id != first_job_id
            await _wait_for_terminal(mock_db, second_job_id)

        await xfail_known_bugs(scenario, _body)


# Pairwise (Format × Concurrency) rows for the preparation-channel funnel
# test. filter_func drops the cooler rows when the tiles extra is absent,
# mirroring the tool gates on the tabix sweeps.
def _prep_concurrency_scenarios() -> list[Scenario]:
    rows = AllPairs(
        [[Format.MCOOL, Format.COOL], list(Concurrency)],
        filter_func=filter_func,
    )
    scenarios: list[Scenario] = []
    for row in rows:
        fmt = next(v for v in row if isinstance(v, Format))
        concurrency = next(v for v in row if isinstance(v, Concurrency))
        scenarios.append(
            Scenario(
                format=fmt,
                endpoint=Endpoint.TILESETS,
                concurrency=concurrency,
            )
        )
    return scenarios


_PREP_CONCURRENCY_SCENARIOS = _prep_concurrency_scenarios()

_SAMPLE_KEY_BY_FORMAT: dict[Format, str] = {
    Format.MCOOL: "mcool",
    Format.COOL: "cool",
}

_FILENAME_BY_FORMAT: dict[Format, str] = {
    Format.MCOOL: "sample.mcool",
    Format.COOL: "sample.cool",
}


class TestTilesetPrepareConcurrency:
    @pytest.mark.parametrize(
        "scenario", _PREP_CONCURRENCY_SCENARIOS, ids=str
    )
    @pytest.mark.asyncio
    async def test_prepare_tileset_should_funnel_concurrent_posts_to_single_workflow_pairwise(
        self,
        samples,
        sample_server,
        wired_api,
        mock_db,
        mocker,
        scenario: Scenario,
        xfail_known_bugs,
    ):
        """Test that N concurrent POSTs funnel onto one workflow per source.

        Given:
            A pairwise sweep over ``(Format ∈ {MCOOL, COOL},
            Concurrency ∈ {N2, N10})`` filtered through ``filter_func``;
            one contact-map document per scenario over a cold cache.
        When:
            N concurrent ``prepare_tileset`` coroutines are awaited via
            ``asyncio.gather`` and the workflow runs to terminal.
        Then:
            It should answer every caller 202 with one identical
            job_id, persist exactly one JobRecord, and complete —
            the mutex funnel extended to the preparation channel.
        """

        async def _body():
            # Arrange
            sample = samples[_SAMPLE_KEY_BY_FORMAT[scenario.format]]
            if sample is None:
                pytest.skip(
                    f"Sample for {scenario.format.value} unavailable on "
                    "this host"
                )
            local_id = (
                f"4DNFI-conc-{scenario.format.name}-{scenario.concurrency.name}"
            )
            doc = _seed_contact_map(
                mock_db,
                sample,
                sample_server,
                local_id=local_id,
                filename=_FILENAME_BY_FORMAT[scenario.format],
            )
            mocker.patch.object(locks, "wait_for_cutover", return_value=None)
            n = scenario.concurrency.value

            # Act
            responses = await asyncio.gather(
                *[prepare_tileset("4dn", doc["local_id"]) for _ in range(n)]
            )

            # Assert
            job_ids = {_job_id_from_202(r) for r in responses}
            assert len(job_ids) == 1, (
                f"Expected one job_id across {n} concurrent POSTs; "
                f"got {job_ids!r}"
            )
            assert len(_records_for(mock_db, doc["local_id"])) == 1

            job_id = next(iter(job_ids))
            await _wait_for_terminal(mock_db, job_id, timeout=120.0)
            final = await get_job(mock_db, job_id)
            assert final is not None
            assert final.status is JobStatus.COMPLETED

        await xfail_known_bugs(scenario, _body)


class TestTilesetPrepareAdmission:
    @pytest.mark.asyncio
    async def test_prepare_tileset_should_return_429_when_admission_ceiling_reached(
        self, samples, sample_server, wired_api, mock_db, mocker
    ):
        """Test that the admission ceiling sheds a POST with 429 Retry-After.

        Given:
            The jobs collection seeded with ``WORKFLOW_MAX_ACTIVE``
            active job documents — the data the admission count reads —
            and a contact-map document over a cold cache.
        When:
            ``prepare_tileset`` is awaited.
        Then:
            It should raise HTTPException 429 carrying a numeric
            Retry-After header, and persist no JobRecord for the file.
        """
        # Arrange — fill the active backlog to the public ceiling.
        sample = samples["mcool"]
        assert sample is not None
        doc = _seed_contact_map(
            mock_db,
            sample,
            sample_server,
            local_id="4DNFI-prep-admission",
            filename="sample.mcool",
        )
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        now = datetime.now(timezone.utc)
        mock_db[JOBS_COLLECTION].docs.extend(
            {
                "job_id": f"seed-{i}",
                "workflow_key": f"seed/{i}/{'0' * 32}/v1",
                "status": JobStatus.PENDING.value,
                "active": True,
                "dcc": "seed",
                "local_id": f"seed-{i}",
                "md5": "0" * 32,
                "pipeline_version": 1,
                "submitted_at": now,
                "updated_at": now,
            }
            for i in range(WORKFLOW_MAX_ACTIVE)
        )

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await prepare_tileset("4dn", doc["local_id"])
        assert exc_info.value.status_code == 429
        assert exc_info.value.headers["Retry-After"].isdigit()
        assert _records_for(mock_db, doc["local_id"]) == []

    @pytest.mark.asyncio
    async def test_prepare_tileset_should_return_503_when_executor_drained(
        self, samples, sample_server, wired_api, mock_db, mocker
    ):
        """Test that a drained executor answers 503, not 409.

        Given:
            A contact-map document over a cold cache and a really-drained
            ``WoolExecutor`` (``drain`` awaited, not patched).
        When:
            ``prepare_tileset`` is awaited.
        Then:
            It should raise HTTPException 503 with a Retry-After header
            — pinning the catch order against the real hierarchy, where
            ``ExecutorDraining`` subclasses ``WorkflowNotApplicable`` and
            a mis-ordered ladder would report 409 — and persist no
            JobRecord.
        """
        # Arrange
        sample = samples["mcool"]
        assert sample is not None
        doc = _seed_contact_map(
            mock_db,
            sample,
            sample_server,
            local_id="4DNFI-prep-draining",
            filename="sample.mcool",
        )
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        await wired_api.drain(timeout=5.0)

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await prepare_tileset("4dn", doc["local_id"])
        assert exc_info.value.status_code == 503
        assert exc_info.value.status_code != 409
        assert exc_info.value.headers["Retry-After"].isdigit()
        assert _records_for(mock_db, doc["local_id"]) == []
