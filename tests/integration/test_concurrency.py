"""Concurrency integration tests — mutex dedup under contention.

These tests dispatch multiple overlapping workflow requests against
the same source file and assert that the Mongo partial-unique index
funnels them onto a single workflow job. They run end-to-end through
a real ``WoolExecutor`` so the full claim/insert/attach path is
exercised.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from allpairspy import AllPairs
from fastapi.responses import JSONResponse

from cfdb import api
from cfdb.api.routers.data import stream_file
from cfdb.api.routers.index import stream_index_file
from cfdb.services import locks
from cfdb.workflows.lock import JOBS_COLLECTION, get_job
from cfdb.workflows.models import JobStatus

from tests.integration.conftest import (
    Concurrency,
    Format,
    Scenario,
    _Request,
    _wait_for_terminal,
    filter_func,
    make_file_meta,
)

pytestmark = pytest.mark.integration


def _job_id_from_202(resp) -> str:
    assert isinstance(resp, JSONResponse)
    assert resp.status_code == 202
    payload = json.loads(bytes(resp.body).decode())
    return payload["job_id"]


@pytest.fixture()
def wired_api(
    install_jobs_index,
    integration_executor,
    mocker,
):
    """Bind integration executor / cache / registry onto the api module."""
    mocker.patch.object(api, "cache", integration_executor._cache)
    mocker.patch.object(
        api, "processor_registry", integration_executor._registry
    )
    mocker.patch.object(api, "executor", integration_executor)
    return integration_executor


def _seed_bam(mock_db, sample, sample_server) -> dict:
    meta = make_file_meta(sample, base_url=sample_server)
    mock_db.dcc.docs = [
        {
            "dcc_abbreviation": "ENCODE",
            "project_id_namespace": "tag:encode.org,2020:",
        }
    ]
    doc = {**meta, "id_namespace": "tag:encode.org,2020:"}
    mock_db.files.docs = [doc]
    mock_db.file.docs = [doc]
    return doc


def _seed_sample(mock_db, sample, sample_server, *, local_id: str | None = None) -> dict:
    """Insert any sample into the file collection, returning the doc."""
    meta = make_file_meta(sample, base_url=sample_server, local_id=local_id)
    mock_db.dcc.docs = [
        {
            "dcc_abbreviation": "ENCODE",
            "project_id_namespace": "tag:encode.org,2020:",
        }
    ]
    doc = {**meta, "id_namespace": "tag:encode.org,2020:"}
    mock_db.files.docs = [doc]
    mock_db.file.docs = [doc]
    return doc


_CONCURRENCY_FORMATS = (Format.BAM, Format.SAM, Format.BED, Format.VCF)


def _concurrency_scenarios() -> list[Scenario]:
    """Pairwise (Format, Concurrency) scenarios for the funnel test."""
    rows = AllPairs(
        [list(_CONCURRENCY_FORMATS), list(Concurrency)],
        filter_func=filter_func,
    )
    scenarios: list[Scenario] = []
    for row in rows:
        fmt = next(v for v in row if isinstance(v, Format))
        concurrency = next(v for v in row if isinstance(v, Concurrency))
        scenarios.append(Scenario(format=fmt, concurrency=concurrency))
    return scenarios


_CONCURRENCY_SCENARIOS = _concurrency_scenarios()


_SAMPLE_KEY_BY_FORMAT: dict[Format, str] = {
    Format.BAM: "BAM",
    Format.SAM: "SAM",
    Format.BED: "BED",
    Format.VCF: "VCF",
}


class TestConcurrency:
    @pytest.mark.asyncio
    async def test_stream_index_file_should_dedupe_concurrent_requests_via_mutex(
        self, samples, sample_server, wired_api, mock_db, mocker, xfail_known_bugs
    ):
        """Test that two concurrent /index calls attach to one workflow.

        Given:
            A pre-sorted BAM file in the DB with a cold cache, and the
            workflow subsystem wired up. /index is used as the dispatch
            trigger because BamIndexProcessor advertises only the INDEX
            artifact for BAMs (DCC-published BAMs are pre-sorted, so
            /data falls through to upstream rather than dispatching).
        When:
            Two stream_index_file coroutines are awaited concurrently
            via asyncio.gather.
        Then:
            Both return 202 with the same job_id; the jobs collection
            holds exactly one record for that workflow_key.
        """
        scenario = Scenario(format=Format.BAM)

        async def _body():
            # Arrange
            doc = _seed_bam(mock_db, samples["BAM"], sample_server)
            mocker.patch.object(locks, "wait_for_cutover", return_value=None)

            # Act — two concurrent GET /index
            a, b = await asyncio.gather(
                stream_index_file(
                    "encode", doc["local_id"], _Request(), range=None
                ),
                stream_index_file(
                    "encode", doc["local_id"], _Request(), range=None
                ),
            )

            # Assert — same job id from both responses
            assert _job_id_from_202(a) == _job_id_from_202(b)
            workflow_records = [
                d for d in mock_db[JOBS_COLLECTION].docs
                if d["local_id"] == doc["local_id"]
            ]
            assert len(workflow_records) == 1

            # Let the workflow complete so the background task doesn't race
            # test teardown, and verify it actually succeeded.
            job_id = _job_id_from_202(a)
            await _wait_for_terminal(mock_db, job_id)
            final = await get_job(mock_db, job_id)
            assert final is not None and final.status == JobStatus.COMPLETED

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.asyncio
    async def test_stream_file_and_stream_index_file_should_share_workflow_when_called_concurrently(
        self, samples, sample_server, wired_api, mock_db, mocker, xfail_known_bugs
    ):
        """Test that /data and /index converge on a single workflow for SAM.

        Given:
            A SAM file in the DB with a cold cache. SAM is used here
            because BamIndexProcessor produces both DATA and INDEX
            artifacts for SAMs (convert+sort+index), so both endpoints
            actually dispatch a workflow. For BAMs only /index
            dispatches; /data falls through to upstream.
        When:
            stream_file and stream_index_file coroutines are awaited
            concurrently against the same source.
        Then:
            Both return 202 with the same job_id; only one workflow
            record exists.
        """
        scenario = Scenario(format=Format.SAM)

        async def _body():
            # Arrange
            doc = _seed_bam(mock_db, samples["SAM"], sample_server)
            mocker.patch.object(locks, "wait_for_cutover", return_value=None)

            # Act
            data_resp, index_resp = await asyncio.gather(
                stream_file("encode", doc["local_id"], _Request(), range=None),
                stream_index_file(
                    "encode", doc["local_id"], _Request(), range=None
                ),
            )

            # Assert
            data_job_id = _job_id_from_202(data_resp)
            index_job_id = _job_id_from_202(index_resp)
            assert data_job_id == index_job_id

            workflow_records = [
                d for d in mock_db[JOBS_COLLECTION].docs
                if d["local_id"] == doc["local_id"]
            ]
            assert len(workflow_records) == 1

            await _wait_for_terminal(mock_db, data_job_id)
            final = await get_job(mock_db, data_job_id)
            assert final is not None and final.status == JobStatus.COMPLETED

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.parametrize(
        "scenario", _CONCURRENCY_SCENARIOS, ids=str
    )
    @pytest.mark.asyncio
    async def test_concurrent_requests_should_funnel_to_single_workflow_pairwise(
        self,
        samples,
        sample_server,
        wired_api,
        mock_db,
        mocker,
        scenario: Scenario,
        xfail_known_bugs,
    ):
        """Test that N concurrent requests funnel onto one workflow per source.

        Given:
            A pairwise sweep over ``(Format ∈ {BAM, SAM, BED, VCF},
            Concurrency ∈ {N2, N10})`` filtered through ``filter_func``;
            the wired API plus a cold cache per scenario.
        When:
            N concurrent ``stream_index_file`` coroutines are awaited
            via ``asyncio.gather`` and the workflow runs to terminal.
        Then:
            All N callers share a single ``job_id``; exactly one
            JobRecord per source is persisted; the final status is
            COMPLETED.
        """

        async def _body():
            # Arrange
            sample = samples[_SAMPLE_KEY_BY_FORMAT[scenario.format]]
            if sample is None:
                pytest.skip(
                    f"Sample for {scenario.format.value} unavailable on this host"
                )
            local_id = (
                f"ENCFF-conc-{scenario.format.name}-{scenario.concurrency.name}"
            )
            doc = _seed_sample(mock_db, sample, sample_server, local_id=local_id)
            mocker.patch.object(locks, "wait_for_cutover", return_value=None)
            n = scenario.concurrency.value

            # Act
            responses = await asyncio.gather(
                *[
                    stream_index_file(
                        "encode", doc["local_id"], _Request(), range=None
                    )
                    for _ in range(n)
                ]
            )

            # Assert
            job_ids = {_job_id_from_202(r) for r in responses}
            assert len(job_ids) == 1, (
                f"Expected one job_id across {n} concurrent calls; got {job_ids!r}"
            )
            records_for_file = [
                d for d in mock_db[JOBS_COLLECTION].docs
                if d["local_id"] == doc["local_id"]
            ]
            assert len(records_for_file) == 1

            job_id = next(iter(job_ids))
            await _wait_for_terminal(mock_db, job_id, timeout=120.0)
            final = await get_job(mock_db, job_id)
            assert final is not None
            assert final.status == JobStatus.COMPLETED

        await xfail_known_bugs(scenario, _body)
