"""Integration test for dispatch over discovery-supplied worker metadata.

This is the composition that shipped broken as issue #90, and the one
no other test reaches. ``tests/integration/test_identity_mtls.py``
proves the identity dial, but over ``WorkerPool(spawn=…)`` — a
different construction, where wool builds the worker itself and the
metadata is authored in-process. The unit suite proves
``EcsDiscovery.poll_once`` reads the right values out of ECS task tags,
but stops at the dict it produces.

What neither covers is the join: a real ``WorkerProxy`` **holding
credentials** consuming metadata that arrived through a **discovery
stream**, and dispatching over a real mTLS channel to a real worker.
wool decides admission on that metadata before attempting a connection
(``_create_security_filter`` / ``_create_version_filter``), so a wrong
``secure`` flag or version empties the pool with no connection error to
diagnose — which is exactly how the original defect hid.

The only fake here is the boto3 wire. Everything else is real:
``_task_to_metadata`` parses the tags, ``_EcsSubscriber`` carries the
events, the proxy is cloudpickled into the worker on every dispatch,
and the handshake is verified against a certificate whose only SAN is
the logical identity. That makes this the closest reachable
approximation of the ECS profile without AWS — the awsvpc IP a Fargate
worker answers on is, for TLS purposes, just another address absent
from the certificate, exactly like the loopback address used here.
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from contextlib import asynccontextmanager
from typing import Any

import pytest
import wool

from cfdb.workflows.constants import (
    WORKER_TAG_SECURE,
    WORKER_TAG_TRUE,
    WORKER_TAG_VERSION,
)
from cfdb.workflows.credentials import DEFAULT_TLS_IDENTITY, build_worker_credentials
from cfdb.workflows.discovery import EcsDiscovery
from cfdb.workflows.loadbalancer import PriorityLoadBalancer

from tests.integration.routines import echo


pytestmark = pytest.mark.integration

#: Bounds every dispatch so a stalled handshake cannot hang the suite.
_DISPATCH_TIMEOUT_S = 30.0

#: Admission budget for the passing case. The worker is already
#: listening before the pool is built, so admission is an in-memory
#: queue drain after discovery's first poll — measured at ~0.1 s. This
#: is pure headroom for a loaded CI box.
_QUORUM_TIMEOUT_S = 15.0

#: Admission budget for the rejecting case, where no worker will ever
#: be admitted and the wait is therefore spent in full. Kept tight
#: because it is the test's entire runtime.
_REJECTED_QUORUM_TIMEOUT_S = 3.0

#: Teardown grace. A clean drain takes about two seconds.
_SHUTDOWN_TIMEOUT_S = 15.0


class _FakeEcsClient:
    """ECS client double serving one hand-seeded task description.

    Only ``list_tasks`` and ``describe_tasks`` are implemented — the
    two calls ``EcsDiscovery`` makes. Their kwarg shape against real
    boto3 is pinned separately by the moto-backed tests in
    ``tests/test_workflows/test_discovery.py``; here the wire is
    deliberately stubbed so the *metadata* is the only variable.

    This never crosses the cloudpickle boundary into the worker:
    ``EcsDiscovery.__getstate__`` nulls ``_client``.
    """

    def __init__(self, tasks: list[dict[str, Any]]) -> None:
        self._tasks = tasks

    def list_tasks(self, **_kwargs: Any) -> dict[str, Any]:
        return {"taskArns": [task["taskArn"] for task in self._tasks]}

    def describe_tasks(
        self, *, cluster: str, tasks: list[str], include: list[str] | None = None
    ) -> dict[str, Any]:
        wanted = set(tasks)
        return {"tasks": [t for t in self._tasks if t["taskArn"] in wanted]}


def _task_advertising(metadata, *, secure: bool) -> dict[str, Any]:
    """Build the ECS task description a worker with ``metadata`` produces.

    The tag payload mirrors ``worker_main._publish_worker_metadata``
    exactly, down to the shared key constants — so a rename on either
    side fails this test rather than silently emptying a real fleet.
    The version is whatever the running worker actually authored, so it
    travels the same path ``worker_main`` publishes it on.
    """
    ip, _port = metadata.address.rsplit(":", 1)
    return {
        "taskArn": f"arn:aws:ecs:us-east-1:123:task/cfdb/{uuid.uuid4().hex}",
        "lastStatus": "RUNNING",
        "healthStatus": "HEALTHY",
        "attachments": [
            {
                "type": "ElasticNetworkInterface",
                "details": [{"name": "privateIPv4Address", "value": ip}],
            }
        ],
        "tags": [
            {"key": WORKER_TAG_VERSION, "value": metadata.version},
            {
                "key": WORKER_TAG_SECURE,
                "value": WORKER_TAG_TRUE if secure else "false",
            },
        ],
    }


@asynccontextmanager
async def _mtls_worker(worker_certs):
    """Run a real worker holding the worker leaf, on an ephemeral port."""
    ca, worker_cert, worker_key, _, _ = worker_certs
    worker = wool.LocalWorker(
        host="127.0.0.1",
        port=0,
        credentials=build_worker_credentials(
            ca, worker_cert, worker_key, identity=DEFAULT_TLS_IDENTITY
        ),
    )
    await worker.start()
    try:
        yield worker
    finally:
        await worker.stop()


def _discovery_resolving(worker, *, secure: bool) -> EcsDiscovery:
    """Build an EcsDiscovery that resolves to ``worker``'s real address.

    ``LocalWorker(port=0)`` binds an ephemeral port and reports the
    resolved address, so the IP seeds the fake task's ENI detail and
    the port becomes ``worker_port`` — which is how
    ``_task_to_metadata`` reassembles it. No fixed ports, no collisions.
    """
    _ip, port = worker.metadata.address.rsplit(":", 1)
    return EcsDiscovery(
        cluster="cfdb-test",
        task_definition_family="cfdb-test-worker",
        client=_FakeEcsClient([_task_advertising(worker.metadata, secure=secure)]),
        worker_port=int(port),
    )


def _pool(discovery, worker_certs, *, quorum_timeout: float):
    """Build the pool the API lifespan builds, bar the quorum.

    Production runs ``quorum=0`` because a Fargate cold start outlasts
    any readiness gate. Here the worker is already listening, so a
    quorum of 1 removes the race between the first dispatch and
    discovery's first poll.

    ``lazy`` is deliberately left at wool's default of True. Dispatch
    cloudpickles this proxy into the worker, and a non-lazy copy enters
    eagerly there — blocking on its own quorum against an
    ``_EcsSubscriber`` that ``__setstate__`` deliberately restores
    inert. Passing ``lazy=False`` turns every dispatch into a timeout.
    """
    ca, _, _, api_cert, api_key = worker_certs
    return wool.WorkerPool(
        discovery=discovery,
        credentials=build_worker_credentials(
            ca, api_cert, api_key, identity=DEFAULT_TLS_IDENTITY
        ),
        loadbalancer=PriorityLoadBalancer(),
        quorum=1,
        quorum_timeout=quorum_timeout,
        shutdown_timeout=_SHUTDOWN_TIMEOUT_S,
    )


class TestEcsDiscoveryDispatch:
    @pytest.mark.asyncio
    async def test_dispatch_should_succeed_when_tags_advertise_a_secure_worker(
        self, worker_certs, offline_aws_env
    ):
        """Test that tag-published metadata carries a real mTLS dispatch.

        Given:
            A real mTLS worker whose certificate's only SAN is the
            logical identity, and an EcsDiscovery resolving it from ECS
            task tags carrying the version and TLS flag that worker
            itself authored.
        When:
            A credentialed pool dispatches a routine through that
            discovery.
        Then:
            It should return the routine's result, because both halves
            of wool's admission gate passed on worker-published metadata
            and the handshake was verified against the identity rather
            than the address discovery composed.
        """
        # Arrange
        async with _mtls_worker(worker_certs) as worker:
            discovery = _discovery_resolving(worker, secure=True)

            # Act
            async with _pool(
                discovery, worker_certs, quorum_timeout=_QUORUM_TIMEOUT_S
            ):
                result = await asyncio.wait_for(
                    echo("hello"), timeout=_DISPATCH_TIMEOUT_S
                )

        # Assert
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_dispatch_should_fail_when_tags_advertise_an_insecure_worker(
        self, worker_certs, offline_aws_env, caplog
    ):
        """Test that a credentialed proxy refuses an insecure worker.

        Given:
            The same reachable mTLS worker, differing in one tag: its
            published ``wool.secure`` reads false — which is what
            discovery reported for every ECS worker before it read the
            tags at all.
        When:
            The same credentialed pool dispatches a routine.
        Then:
            It should never admit the worker, timing out on the
            readiness gate instead, because wool admits only secure
            workers to a proxy holding credentials — so a wrong flag
            empties the pool however healthy the fleet is.
        """
        # Arrange
        async with _mtls_worker(worker_certs) as worker:
            discovery = _discovery_resolving(worker, secure=False)

            # Act & assert
            with caplog.at_level(
                logging.DEBUG, logger="wool.runtime.worker.proxy"
            ):
                async with _pool(
                    discovery,
                    worker_certs,
                    quorum_timeout=_REJECTED_QUORUM_TIMEOUT_S,
                ):
                    with pytest.raises(asyncio.TimeoutError):
                        await asyncio.wait_for(
                            echo("hello"), timeout=_DISPATCH_TIMEOUT_S
                        )

        # A timeout alone would also follow from a worker that never
        # started; the gate's own diagnostic is what ties the refusal to
        # the security half. Matched loosely because wool builds the
        # message from a parameterized format string and emits it at
        # DEBUG — the exception carries the contract, this corroborates.
        assert re.search(
            r"(?i)admission gate rejected.*incompatible", caplog.text
        )
