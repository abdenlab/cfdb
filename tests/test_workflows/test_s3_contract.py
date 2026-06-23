"""End-to-end contract test: workflows must persist artifacts to S3.

This is the test that would have caught the bug where processors hardcoded
``LocalFsCache`` and ignored the injected backend, so the S3/ECS profile
never actually persisted artifacts to the store the API reads from.

It runs the executor with in-process dispatch (``no_wool_dispatch``) so
moto's in-process S3 mock stays active — moto cannot patch boto3 inside a
spawned worker subprocess. The cloudpickle boundary itself is exercised
separately by ``tests/integration/test_executor_boundary.py``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cfdb import api
from cfdb.api.routers.cache_stream import serve_workflow_artifact_or_dispatch
from cfdb.workflows.cache import S3Cache
from cfdb.workflows.events import Complete, StageComplete, WorkflowEvent
from cfdb.workflows.executor import WoolExecutor
from cfdb.workflows.lock import get_job
from cfdb.workflows.models import ArtifactKind, JobStatus
from cfdb.workflows.processors.base import Processor
from cfdb.workflows.processors.registry import ProcessorRegistry
from tests.test_workflows import FIXTURE_MD5
from tests.test_workflows.test_executor import _wait_for_terminal

_BUCKET = "cfdb-contract-cache"


@pytest.fixture()
def s3_client():
    """Yield a moto-backed boto3 S3 client with the cache bucket created."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=_BUCKET)
        yield client


class _S3StubProcessor(Processor):
    """Materialise two artifacts to ``workdir`` and ``put`` them via the cache.

    Tool-free so the test stays a unit test, but exercises the real
    producer path: derive keys via :meth:`cache_key_for`, write local
    files, and persist through the injected backend.
    """

    processor_version = 0
    supported_formats = frozenset({"BED"})
    artifact_kinds = (ArtifactKind.DATA, ArtifactKind.INDEX)

    _PAYLOADS = {
        ArtifactKind.DATA: b"data-bytes",
        ArtifactKind.INDEX: b"index-bytes",
    }

    async def run(self, file_meta, workdir, cache) -> AsyncIterator[WorkflowEvent]:
        workdir.mkdir(parents=True, exist_ok=True)
        artifacts: dict[str, str] = {}
        for kind, payload in self._PAYLOADS.items():
            key = self.cache_key_for(file_meta, kind)
            src = workdir / kind.value
            src.write_bytes(payload)
            await cache.put(key, src)
            artifacts[kind.value] = key
            yield StageComplete(kind=kind, key=key)
        yield Complete(artifacts=artifacts)


def _install_jobs_index(mock_db) -> None:
    mock_db.jobs.create_index(
        {"workflow_key": 1},
        unique=True,
        partialFilterExpression={"active": True},
    )


def _file_meta() -> dict[str, Any]:
    return {
        "dcc": {"dcc_abbreviation": "ENCODE"},
        "local_id": "ENCFF1",
        "md5": FIXTURE_MD5,
        "file_format": {"name": "BED"},
        "access_url": "https://example.org/x.bed",
    }


class _Request:
    def __init__(self, method: str = "GET") -> None:
        self.method = method


@pytest.mark.asyncio
async def test_workflow_should_persist_artifacts_to_s3_and_serve_cache_hit(
    mock_db, s3_client, tmp_path: Path, no_wool_dispatch, mocker
):
    """Test that a workflow's artifacts reach S3 and the API then serves them.

    Given:
        An executor wired to an S3Cache (moto-backed) and a processor that
        commits its artifacts through the injected backend.
    When:
        A workflow is dispatched in-process and runs to completion.
    Then:
        Each artifact exists as an object in the S3 bucket, cache.get
        streams the bytes back, and serve_workflow_artifact_or_dispatch
        returns a cache hit (not a 202) on a subsequent GET — proving the
        producer -> S3 -> consumer round-trip the S3 profile depends on.
    """
    # Arrange
    cache = S3Cache(bucket=_BUCKET, client=s3_client)
    processor = _S3StubProcessor()
    registry = ProcessorRegistry()
    registry.register(processor)
    executor = WoolExecutor(
        mock_db, cache, registry, workdir_root=tmp_path / "jobs"
    )
    _install_jobs_index(mock_db)
    file_meta = _file_meta()

    # Act
    record, _ = await executor.ensure_workflow(file_meta)
    await _wait_for_terminal(mock_db, record.job_id)

    # Assert — the workflow completed...
    final = await get_job(mock_db, record.job_id)
    assert final is not None
    assert final.status == JobStatus.COMPLETED

    data_key = processor.cache_key_for(file_meta, ArtifactKind.DATA)
    index_key = processor.cache_key_for(file_meta, ArtifactKind.INDEX)

    # ...the artifacts are real objects in the bucket (the bug: empty)...
    s3_client.head_object(Bucket=_BUCKET, Key=data_key)
    s3_client.head_object(Bucket=_BUCKET, Key=index_key)

    # ...the cache reads them back...
    assert await cache.head(data_key) is not None
    chunks = [chunk async for chunk in cache.get(data_key)]
    assert b"".join(chunks) == b"data-bytes"

    # ...and the router serves a cache hit rather than re-dispatching.
    mocker.patch.object(api, "cache", cache)
    mocker.patch.object(api, "executor", executor)
    mocker.patch.object(api, "processor_registry", registry)
    resp = await serve_workflow_artifact_or_dispatch(
        file_meta,
        ArtifactKind.DATA,
        _Request("GET"),
        None,
        head_404_detail="n/a",
    )
    assert resp is not None
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_serve_should_dispatch_when_s3_artifact_absent(
    mock_db, s3_client, tmp_path: Path, mocker
):
    """Test that an empty S3 cache yields a 202 dispatch, not a false hit.

    Given:
        An S3Cache with no objects and the workflow subsystem wired.
    When:
        serve_workflow_artifact_or_dispatch is awaited for a GET.
    Then:
        It returns a 202 (cache miss -> dispatch), confirming the cache
        probe genuinely consults S3.
    """
    # Arrange
    cache = S3Cache(bucket=_BUCKET, client=s3_client)
    registry = ProcessorRegistry()
    registry.register(_S3StubProcessor())
    executor = WoolExecutor(
        mock_db, cache, registry, workdir_root=tmp_path / "jobs"
    )
    _install_jobs_index(mock_db)
    mocker.patch.object(api, "cache", cache)
    mocker.patch.object(api, "executor", executor)
    mocker.patch.object(api, "processor_registry", registry)

    # Assert the bucket really is empty for this identity.
    miss_key = _S3StubProcessor().cache_key_for(_file_meta(), ArtifactKind.DATA)
    with pytest.raises(ClientError):
        s3_client.head_object(Bucket=_BUCKET, Key=miss_key)

    # Act
    resp = await serve_workflow_artifact_or_dispatch(
        _file_meta(),
        ArtifactKind.DATA,
        _Request("GET"),
        None,
        head_404_detail="n/a",
    )

    # Assert — dispatched (202), proving the probe consulted S3.
    assert resp is not None
    assert resp.status_code == 202
