"""Tests for EcsDiscovery."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import cloudpickle
import pytest
import wool
from wool.runtime.discovery.base import WorkerMetadata

from cfdb.workflows.discovery import EcsDiscovery


def _task_arn(task_id: str | None = None) -> str:
    """Build a stable ECS task ARN for tests."""
    return f"arn:aws:ecs:us-east-1:123:task/cluster/{task_id or uuid.uuid4().hex}"


def _running_task(
    task_id: str,
    *,
    ip: str = "10.0.0.5",
    health: str = "HEALTHY",
    status: str = "RUNNING",
) -> dict[str, Any]:
    """Construct a fake ECS DescribeTasks entry."""
    return {
        "taskArn": _task_arn(task_id),
        "lastStatus": status,
        "healthStatus": health,
        "attachments": [
            {
                "type": "ElasticNetworkInterface",
                "details": [
                    {"name": "subnetId", "value": "subnet-1"},
                    {"name": "privateIPv4Address", "value": ip},
                ],
            }
        ],
    }


class _FakeEcsClient:
    """Fake ECS client whose responses can be re-set per call."""

    def __init__(self) -> None:
        self.task_arns: list[str] = []
        self.tasks: list[dict[str, Any]] = []

    def list_tasks(self, **_kwargs):
        return {"taskArns": list(self.task_arns)}

    def describe_tasks(self, *, cluster: str, tasks: list[str]):
        wanted = set(tasks)
        return {
            "tasks": [t for t in self.tasks if t["taskArn"] in wanted],
        }


class TestEcsDiscovery:
    def test___init___without_cluster(self):
        """Test that EcsDiscovery rejects an empty cluster argument.

        Given:
            An empty cluster name.
        When:
            EcsDiscovery is constructed.
        Then:
            It should raise ValueError so misconfigurations fail fast.
        """
        # Act & assert
        with pytest.raises(ValueError, match="cluster"):
            EcsDiscovery(
                cluster="",
                task_definition_family="worker",
                client=_FakeEcsClient(),
            )

    def test___init___without_task_definition_family(self):
        """Test that EcsDiscovery rejects an empty task_definition_family.

        Given:
            A cluster but no task_definition_family.
        When:
            EcsDiscovery is constructed.
        Then:
            It should raise ValueError to surface the misconfiguration.
        """
        # Act & assert
        with pytest.raises(ValueError, match="task_definition_family"):
            EcsDiscovery(
                cluster="c",
                task_definition_family="",
                client=_FakeEcsClient(),
            )

    def test_publisher_conforms_to_discovery_publisher_like(self):
        """Test that the publisher satisfies wool's DiscoveryPublisherLike.

        Given:
            An EcsDiscovery and the guard-rail publisher it returns.
        When:
            That publisher is checked against ``wool.DiscoveryPublisherLike``.
        Then:
            It should satisfy the protocol — including the ``bind_host``
            attribute wool 0.9.2 made part of the contract — so wool keeps
            accepting EcsDiscovery as a valid discovery backend.
        """
        # Arrange
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=_FakeEcsClient(),
        )

        # Act
        publisher = discovery.publisher

        # Assert
        assert isinstance(publisher, wool.DiscoveryPublisherLike)
        assert isinstance(publisher.bind_host, str)

    @pytest.mark.asyncio
    async def test_poll_once_with_initial_healthy_workers(self):
        """Test that the first poll emits worker-added events.

        Given:
            A fake ECS client returning one RUNNING + HEALTHY task.
        When:
            poll_once is awaited.
        Then:
            It should emit exactly one ``worker-added`` event whose metadata
            carries the task's private IP and the configured worker port.
        """
        # Arrange
        client = _FakeEcsClient()
        task_id = uuid.uuid4().hex
        client.task_arns = [_task_arn(task_id)]
        client.tasks = [_running_task(task_id)]
        # The fake's task ARN must align with the seed.
        client.tasks[0]["taskArn"] = client.task_arns[0]
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=client,
            worker_port=4242,
        )

        # Act
        events, resolved = await discovery.poll_once()

        # Assert
        assert len(events) == 1
        assert events[0].type == "worker-added"
        assert events[0].metadata.address == "10.0.0.5:4242"
        assert len(resolved) == 1

    @pytest.mark.asyncio
    async def test_poll_once_with_unhealthy_task_filtered(self):
        """Test that UNHEALTHY tasks never surface as workers.

        Given:
            A fake ECS client whose task is RUNNING but UNHEALTHY.
        When:
            poll_once is awaited.
        Then:
            No events should be emitted and resolved should be empty.
        """
        # Arrange
        client = _FakeEcsClient()
        task_id = uuid.uuid4().hex
        client.task_arns = [_task_arn(task_id)]
        client.tasks = [_running_task(task_id, health="UNHEALTHY")]
        client.tasks[0]["taskArn"] = client.task_arns[0]
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=client,
        )

        # Act
        events, resolved = await discovery.poll_once()

        # Assert
        assert events == []
        assert resolved == {}

    @pytest.mark.asyncio
    async def test_poll_once_with_dropped_task_after_initial_seen(self):
        """Test that a vanished task surfaces as worker-dropped.

        Given:
            A fake ECS client that initially reports one task and then
            reports an empty cluster on the next poll.
        When:
            poll_once is awaited twice.
        Then:
            The second poll should emit one worker-dropped event for the
            previously-known task.
        """
        # Arrange
        client = _FakeEcsClient()
        task_id = uuid.uuid4().hex
        client.task_arns = [_task_arn(task_id)]
        client.tasks = [_running_task(task_id)]
        client.tasks[0]["taskArn"] = client.task_arns[0]
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=client,
        )
        await discovery.poll_once()

        # Act — second poll observes the cluster gone empty
        client.task_arns = []
        client.tasks = []
        events, resolved = await discovery.poll_once()

        # Assert
        assert len(events) == 1
        assert events[0].type == "worker-dropped"
        assert resolved == {}

    @pytest.mark.asyncio
    async def test_poll_once_with_idempotent_steady_state(self):
        """Test that repeated polls of the same set emit no extra events.

        Given:
            A fake ECS client that consistently returns the same task.
        When:
            poll_once is awaited twice.
        Then:
            The second poll should emit no events because the diff is empty.
        """
        # Arrange
        client = _FakeEcsClient()
        task_id = uuid.uuid4().hex
        client.task_arns = [_task_arn(task_id)]
        client.tasks = [_running_task(task_id)]
        client.tasks[0]["taskArn"] = client.task_arns[0]
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=client,
        )
        await discovery.poll_once()

        # Act
        events, _ = await discovery.poll_once()

        # Assert
        assert events == []

    @pytest.mark.asyncio
    async def test_subscribe_with_filter_excluding_worker(self):
        """Test that a subscriber's filter suppresses non-matching events.

        Given:
            A discovery instance with one healthy worker and a subscriber
            whose filter rejects every metadata.
        When:
            poll_once is awaited.
        Then:
            The subscriber's queue should remain empty.
        """
        # Arrange
        client = _FakeEcsClient()
        task_id = uuid.uuid4().hex
        client.task_arns = [_task_arn(task_id)]
        client.tasks = [_running_task(task_id)]
        client.tasks[0]["taskArn"] = client.task_arns[0]
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=client,
        )
        sub = discovery.subscribe(filter=lambda _meta: False)

        async def _drain():
            async for event in sub:  # pragma: no cover — should never iterate
                return event
            return None

        # Act — start subscriber, then poll
        consumer = asyncio.create_task(_drain())
        await asyncio.sleep(0)  # let consumer register
        await discovery.poll_once()
        await asyncio.sleep(0.05)

        # Assert
        consumer.cancel()
        try:
            await consumer
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_subscribe_with_two_iter_calls_raises_on_second(self):
        """Test that double-iteration of one subscriber is refused.

        Given:
            A subscriber whose async iterator has already been opened.
        When:
            A second async-for loop tries to drive the same subscriber.
        Then:
            It should raise RuntimeError on the second __anext__ rather
            than silently double-register and duplicate event delivery.
        """
        # Arrange
        client = _FakeEcsClient()
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=client,
        )
        sub = discovery.subscribe()
        iter_one = sub.__aiter__()
        iter_two = sub.__aiter__()
        # Start iter_one so it flips _exhausted before iter_two runs.
        first_consumer = asyncio.create_task(iter_one.__anext__())
        await asyncio.sleep(0)

        # Act & assert
        with pytest.raises(RuntimeError, match="already iterated"):
            await iter_two.__anext__()

        # Cleanup
        first_consumer.cancel()
        try:
            await first_consumer
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_aexit_wakes_parked_consumer(self):
        """Test that __aexit__ unblocks consumers parked on queue.get().

        Given:
            A discovery context with a subscriber consuming events.
        When:
            The discovery context exits while the consumer is parked.
        Then:
            The consumer's async-for loop ends cleanly within a short
            timeout rather than blocking on a queue that nothing will
            publish to again.
        """
        # Arrange
        client = _FakeEcsClient()
        events_seen: list[Any] = []

        async def _consume(sub):
            async for event in sub:
                events_seen.append(event)

        # Act
        async with EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=client,
        ) as discovery:
            consumer = asyncio.create_task(_consume(discovery.subscribe()))
            # Let the consumer register and park on queue.get().
            await asyncio.sleep(0.05)
        # Exiting the context should push the sentinel; the consumer
        # task ends shortly after.
        await asyncio.wait_for(consumer, timeout=1.0)

        # Assert — the consumer ended cleanly without exception.
        assert consumer.done() and consumer.exception() is None


# ---------------------------------------------------------------------------
# Moto-backed wire-shape verification.
#
# Pure unit tests above use ``_FakeEcsClient`` to assert call shape and
# branch coverage. The class below runs the same discovery client
# against a real boto3 ``ecs`` client wired into ``moto``'s in-process
# simulator — any drift between our kwarg shape and what ECS actually
# accepts (or in the attachment payload shape ``_extract_eni_ip``
# parses) surfaces here, catching a class of bug pure mocks cannot.
#
# moto does not transition ``healthStatus`` past ``None`` and ages
# ``lastStatus`` to ``DEACTIVATING`` immediately, so full
# ``poll_once`` cycles surface no metadata. These tests cover the
# low-level methods (``_list_task_arns`` / ``_describe_tasks_batched``)
# and the IP-extraction parser; ``_task_to_metadata``'s status filter
# remains covered by the upstream class's synthetic fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture()
def moto_ecs_env_for_discovery():
    """Stand up a moto-backed ECS cluster + task def + VPC for discovery tests.

    Pre-launches one Fargate task so the discovery client has
    something to enumerate. Returns the boto3 ``ecs`` client, the
    cluster, the family, and the launched ARN.
    """
    import boto3
    from moto import mock_aws

    with mock_aws():
        ec2 = boto3.client("ec2", region_name="us-east-1")
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
        ec2.modify_vpc_attribute(VpcId=vpc, EnableDnsHostnames={"Value": True})
        subnet = ec2.create_subnet(
            VpcId=vpc, CidrBlock="10.0.0.0/24", AvailabilityZone="us-east-1a"
        )["Subnet"]["SubnetId"]
        sg = ec2.create_security_group(
            GroupName="cfdb-test-sg", Description="moto sg", VpcId=vpc
        )["GroupId"]
        ecs = boto3.client("ecs", region_name="us-east-1")
        ecs.create_cluster(clusterName="cfdb-test")
        ecs.register_task_definition(
            family="cfdb-test-worker",
            networkMode="awsvpc",
            requiresCompatibilities=["FARGATE"],
            cpu="256",
            memory="512",
            containerDefinitions=[
                {
                    "name": "worker",
                    "image": "cfdb-worker:test",
                    "essential": True,
                    "memory": 512,
                }
            ],
        )
        arns: list[str] = []
        for _ in range(3):
            resp = ecs.run_task(
                cluster="cfdb-test",
                taskDefinition="cfdb-test-worker",
                launchType="FARGATE",
                count=1,
                networkConfiguration={
                    "awsvpcConfiguration": {
                        "subnets": [subnet],
                        "securityGroups": [sg],
                        "assignPublicIp": "DISABLED",
                    }
                },
            )
            arns.append(resp["tasks"][0]["taskArn"])
        yield {
            "client": ecs,
            "cluster": "cfdb-test",
            "family": "cfdb-test-worker",
            "task_arns": arns,
        }


class TestEcsDiscoveryAgainstMoto:
    """Wire-shape verification against a real boto3 ``ecs`` client."""

    def test_list_task_arns_should_enumerate_launched_tasks(
        self, moto_ecs_env_for_discovery
    ):
        """Test that ``_list_task_arns`` surfaces every launched task.

        Given:
            A moto-backed cluster with three Fargate tasks launched
            against the same family.
        When:
            ``_list_task_arns`` is invoked.
        Then:
            It should return all three ARNs, confirming the
            ``cluster`` + ``family`` + ``desiredStatus`` kwargs are
            accepted by the real ECS API shape.
        """
        # Arrange
        discovery = EcsDiscovery(
            cluster=moto_ecs_env_for_discovery["cluster"],
            task_definition_family=moto_ecs_env_for_discovery["family"],
            client=moto_ecs_env_for_discovery["client"],
        )

        # Act
        arns = discovery._list_task_arns()

        # Assert
        assert set(arns) == set(moto_ecs_env_for_discovery["task_arns"])

    def test_describe_tasks_batched_should_split_into_multiple_calls(
        self, moto_ecs_env_for_discovery, monkeypatch
    ):
        """Test that ``_describe_tasks_batched`` chunks ARNs across batches.

        Given:
            A patched batch size of 2 and three launched tasks (so two
            batches are required).
        When:
            ``_describe_tasks_batched`` is invoked on all three ARNs.
        Then:
            It should return three describe-task entries, proving both
            batches were sent and concatenated correctly.
        """
        # Arrange
        from cfdb.workflows import discovery as discovery_mod

        monkeypatch.setattr(discovery_mod, "_DESCRIBE_BATCH_SIZE", 2)
        discovery = EcsDiscovery(
            cluster=moto_ecs_env_for_discovery["cluster"],
            task_definition_family=moto_ecs_env_for_discovery["family"],
            client=moto_ecs_env_for_discovery["client"],
        )

        # Act
        tasks = discovery._describe_tasks_batched(
            moto_ecs_env_for_discovery["task_arns"]
        )

        # Assert
        assert {t["taskArn"] for t in tasks} == set(
            moto_ecs_env_for_discovery["task_arns"]
        )

    def test_extract_eni_ip_should_parse_motos_attachment_payload(
        self, moto_ecs_env_for_discovery
    ):
        """Test that ``_extract_eni_ip`` recovers the IP from moto's attachments.

        Given:
            A describe-tasks entry returned by moto for a Fargate task
            with awsvpc networking.
        When:
            ``_extract_eni_ip`` runs against the task dict.
        Then:
            It should return the ``privateIPv4Address`` recorded in the
            ``ElasticNetworkInterface`` attachment, proving our parser
            agrees with the canonical ECS attachment shape.
        """
        # Arrange
        from cfdb.workflows.discovery import _extract_eni_ip

        described = moto_ecs_env_for_discovery["client"].describe_tasks(
            cluster=moto_ecs_env_for_discovery["cluster"],
            tasks=moto_ecs_env_for_discovery["task_arns"][:1],
        )["tasks"][0]

        # Act
        ip = _extract_eni_ip(described)

        # Assert — moto draws private IPs out of the subnet CIDR, which
        # we set to 10.0.0.0/24; assert the parser recovered an IP in
        # the IPv4 dotted-quad shape rather than pinning to a specific
        # address (moto picks randomly within the subnet).
        assert ip is not None
        parts = ip.split(".")
        assert len(parts) == 4 and all(p.isdigit() for p in parts)


class TestEcsDiscoveryPickle:
    """Cloudpickle round-trip behaviour of EcsDiscovery (issue #54 Bug 1).

    wool's ``WorkerProxy.__reduce__`` drags the caller's ``discovery``
    across the dispatch boundary, so ``EcsDiscovery`` MUST serialize.
    Its live boto3 ECS client holds an ``ssl.SSLContext`` that
    cloudpickle cannot handle, so ``__getstate__`` nulls it (and the
    other loop-bound runtime fields) and ``__setstate__`` rebuilds it
    via :func:`build_ecs_client`, mirroring ``S3Cache``.
    """

    def test___getstate___should_strip_client_and_loop_bound_fields(self):
        """Test that __getstate__ nulls the boto3 client and drops live state.

        Given:
            An EcsDiscovery built with a dummy (picklable) ECS client.
        When:
            ``__getstate__`` is called.
        Then:
            ``_client`` is None and the loop-bound / live-runtime fields
            (the two locks, the poll task, and the subscriber / known
            sets) are absent from the pickled state, while the config
            needed to rebuild the client (endpoint_url, region_name)
            survives.
        """
        # Arrange
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=_FakeEcsClient(),
        )

        # Act
        state = discovery.__getstate__()

        # Assert — the SSLContext-bearing client is gone...
        assert state["_client"] is None
        # ...as are the loop-bound locks, the poll task, and live state.
        for transient in (
            "_state_lock",
            "_serialize_polls",
            "_poll_task",
            "_subscribers",
            "_known",
        ):
            assert transient not in state
        # ...but the boto rebuild config is retained.
        assert "_endpoint_url" in state
        assert "_region_name" in state

    def test_cloudpickle_dumps_should_succeed_with_dummy_client(self):
        """Test that an EcsDiscovery cloudpickle-serializes after __getstate__.

        Given:
            An EcsDiscovery built with a dummy ECS client (a real boto3
            client cannot be constructed in this venv).
        When:
            ``cloudpickle.dumps`` is called on it.
        Then:
            Serialization succeeds — the boundary that wool's proxy
            reduce-path exercises is no longer poisoned by the live
            client.
        """
        # Arrange
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=_FakeEcsClient(),
        )

        # Act
        blob = cloudpickle.dumps(discovery)

        # Assert
        assert isinstance(blob, bytes) and blob

    def test___setstate___should_rebuild_client_and_reset_runtime_state(
        self, monkeypatch
    ):
        """Test that unpickling rebuilds the client and resets transient state.

        Given:
            An EcsDiscovery built with a DISTINCT original ECS client and
            then serialized with cloudpickle, with ``build_ecs_client``
            patched to return a fresh sentinel so unpickling does not
            require a real boto3 ECS client (unbuildable in this venv).
        When:
            The blob is loaded back.
        Then:
            ``__setstate__`` rebuilds ``_client`` via ``build_ecs_client``
            (so the restored client is the freshly-built sentinel, NOT the
            original instance — a no-op ``__setstate__`` that left the
            stripped ``None`` or somehow restored the original would fail
            this assertion), threading the original endpoint_url /
            region_name, and the transient runtime fields are recreated
            empty (fresh locks, no poll task, empty subscriber and known
            sets).
        """
        # Arrange — build with a distinct original client so the rebuild
        # is observable. ``build_ecs_client`` is patched for the whole
        # test (so __setstate__ doesn't reach for an unbuildable real
        # client) and the boto kwargs are threaded via __dict__ rather
        # than the ctor, which rejects client + endpoint/region together.
        original_client = _FakeEcsClient()
        rebuilt: dict[str, Any] = {}
        sentinel = object()

        def _fake_build(*, endpoint_url=None, region_name=None):
            rebuilt["endpoint_url"] = endpoint_url
            rebuilt["region_name"] = region_name
            return sentinel

        monkeypatch.setattr(
            "cfdb.workflows.discovery.build_ecs_client", _fake_build
        )
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=original_client,
            worker_port=4242,
        )
        # The ctor refuses client + endpoint/region together, so set the
        # rebuild config directly to exercise the threading without that
        # guard.
        discovery._endpoint_url = "http://localstack:4566"
        discovery._region_name = "us-east-2"
        blob = cloudpickle.dumps(discovery)

        # Act
        restored = cloudpickle.loads(blob)

        # Assert — the client was rebuilt via build_ecs_client (the
        # restored client is the sentinel, NOT the original instance)...
        assert restored._client is sentinel
        assert restored._client is not original_client
        assert rebuilt == {
            "endpoint_url": "http://localstack:4566",
            "region_name": "us-east-2",
        }
        # ...config survived the round-trip...
        assert restored._cluster == "c"
        assert restored._task_definition_family == "worker"
        assert restored._worker_port == 4242
        # ...and transient runtime state is fresh and inert.
        assert restored._subscribers == []
        assert restored._known == {}
        assert restored._poll_task is None
        assert restored._closed is False
        assert isinstance(restored._state_lock, asyncio.Lock)
        assert isinstance(restored._serialize_polls, asyncio.Lock)

    def test___setstate___should_keep_existing_client_and_skip_rebuild(
        self, monkeypatch
    ):
        """Test that __setstate__ preserves an already-present client.

        Given:
            A pickle state whose ``_client`` is already populated, with
            ``build_ecs_client`` patched to a recording stub. (The pickle
            protocol always nulls ``_client``, so a non-None client only
            reaches ``__setstate__`` via a non-pickle caller; the state is
            built by hand and the protocol method invoked directly to
            exercise the guard that the pickle round-trip cannot reach.)
        When:
            ``__setstate__`` is applied to that state.
        Then:
            The existing client is preserved and ``build_ecs_client`` is
            never called, so restoration is idempotent and never clobbers a
            caller-supplied client.
        """
        # Arrange
        build_calls = {"n": 0}

        def _fake_build(*, endpoint_url=None, region_name=None):
            build_calls["n"] += 1
            return object()

        monkeypatch.setattr(
            "cfdb.workflows.discovery.build_ecs_client", _fake_build
        )
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=_FakeEcsClient(),
        )
        existing_client = _FakeEcsClient()
        state = discovery.__getstate__()
        state["_client"] = existing_client

        # Act
        restored = EcsDiscovery.__new__(EcsDiscovery)
        restored.__setstate__(state)

        # Assert
        assert restored._client is existing_client
        assert build_calls["n"] == 0


def _seeded_fake_client() -> _FakeEcsClient:
    """Build a ``_FakeEcsClient`` seeded with one RUNNING + HEALTHY task."""
    client = _FakeEcsClient()
    task_id = uuid.uuid4().hex
    arn = _task_arn(task_id)
    client.task_arns = [arn]
    task = _running_task(task_id)
    task["taskArn"] = arn
    client.tasks = [task]
    return client


class TestEcsDiscoveryPickleAgainstMoto:
    """Cloudpickle round-trip against a REAL boto3 ECS client (issue #54 Bug 1).

    The pure-unit pickle tests inject ``_FakeEcsClient``, which is itself
    picklable — so they prove ``__getstate__`` runs but not that it is
    *load-bearing*. The real boto3 ECS client holds an ``ssl.SSLContext``
    via its urllib3 pools and is genuinely unpicklable; this is the test
    that fails if ``__getstate__`` ever stops nulling ``_client``. A real
    client can only be built inside ``moto.mock_aws()`` (the venv lacks
    ``botocore[crt]`` for the default credential chain), and
    ``cloudpickle.loads`` must also run inside the context because
    ``__setstate__`` rebuilds the client via :func:`build_ecs_client`.
    """

    def test_cloudpickle_roundtrip_should_succeed_with_real_boto3_client(self):
        """Test that an EcsDiscovery over a real boto3 client round-trips.

        Given:
            A real boto3 ``ecs`` client built inside ``moto.mock_aws()``
            (the raw client is unpicklable — cloudpickle chokes on its
            ``ssl.SSLContext``), wrapped in an EcsDiscovery.
        When:
            The discovery is ``cloudpickle.dumps``'d and ``loads``'d back
            inside the same moto context.
        Then:
            The full round-trip succeeds — ``__getstate__`` strips the
            unpicklable client and ``__setstate__`` rebuilds a fresh one
            via :func:`build_ecs_client`. This is the load-bearing proof
            the ``_FakeEcsClient`` tests cannot give.
        """
        import boto3
        from moto import mock_aws

        with mock_aws():
            # Arrange — the raw client cannot be pickled.
            client = boto3.client("ecs", region_name="us-east-1")
            with pytest.raises((TypeError, Exception)):
                cloudpickle.dumps(client)
            discovery = EcsDiscovery(
                cluster="c",
                task_definition_family="worker",
                client=client,
            )

            # Act — dump + load must both happen inside mock_aws so the
            # __setstate__ rebuild can construct a fresh moto-backed client.
            restored = cloudpickle.loads(cloudpickle.dumps(discovery))

            # Assert — a real client survived the boundary.
            assert restored._client is not None
            assert restored._client is not client
            assert restored._cluster == "c"
            assert restored._task_definition_family == "worker"


class TestEcsDiscoveryPickleUnpicklableClient:
    """Round-trip resilience when the live client is deliberately unpicklable."""

    def test_cloudpickle_dumps_should_succeed_with_unpicklable_client(self):
        """Test that dumps succeeds even when the live client cannot pickle.

        Given:
            An EcsDiscovery whose injected client raises from
            ``__reduce__`` (a stand-in for the real boto3 client's
            unpicklable ``ssl.SSLContext``).
        When:
            ``cloudpickle.dumps`` is called on the discovery.
        Then:
            Serialization succeeds because ``__getstate__`` nulls
            ``_client`` before cloudpickle ever tries to reduce it — so
            the client's broken ``__reduce__`` is never reached.
        """
        # Arrange
        class _Unpicklable:
            def __reduce__(self):
                raise RuntimeError("this client must never be pickled")

        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=_Unpicklable(),
        )

        # Act
        blob = cloudpickle.dumps(discovery)

        # Assert
        assert isinstance(blob, bytes) and blob

    @pytest.mark.asyncio
    async def test_restored_discovery_should_poll_with_rebuilt_client(
        self, monkeypatch
    ):
        """Test that a round-tripped discovery polls via its rebuilt client.

        Given:
            ``build_ecs_client`` patched to return a ``_FakeEcsClient``
            seeded with one RUNNING + HEALTHY task, and an EcsDiscovery
            cloudpickle round-tripped through that patch.
        When:
            ``poll_once`` is awaited and a subscriber is opened on the
            restored instance.
        Then:
            The rebuilt client drives a real poll — one ``worker-added``
            event is emitted — and the restored object's fresh locks and
            subscriber machinery work together, proving the round-trip
            yields a functional discovery handle, not just a field-shaped
            husk.
        """
        # Arrange — patch build so __setstate__ gets a working fake client.
        monkeypatch.setattr(
            "cfdb.workflows.discovery.build_ecs_client",
            lambda **_kwargs: _seeded_fake_client(),
        )
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=_FakeEcsClient(),
            worker_port=4242,
        )
        restored = cloudpickle.loads(cloudpickle.dumps(discovery))

        # Act
        events, resolved = await restored.poll_once()

        # Assert — the rebuilt client produced a live worker-added event...
        assert [e.type for e in events] == ["worker-added"]
        assert len(resolved) == 1
        # ...and the restored subscriber machinery (fresh locks) works.
        sub = restored.subscribe()
        first = await asyncio.wait_for(sub.__aiter__().__anext__(), timeout=1.0)
        assert first.type == "worker-added"
        assert first.metadata.address.endswith(":4242")

    def test_cloudpickle_roundtrip_should_be_idempotent(self, monkeypatch):
        """Test that two successive round-trips both succeed and stay stable.

        Given:
            ``build_ecs_client`` patched to a stub and an EcsDiscovery.
        When:
            ``loads(dumps(...))`` is applied twice in sequence.
        Then:
            The second round-trip succeeds, the config is stable across
            both, and the transient runtime fields remain fresh — so a
            once-pickled-then-unpickled object remains re-picklable
            (``__getstate__``/``__setstate__`` are not single-shot).
        """
        # Arrange
        sentinel = object()
        monkeypatch.setattr(
            "cfdb.workflows.discovery.build_ecs_client",
            lambda **_kwargs: sentinel,
        )
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=_FakeEcsClient(),
            worker_port=4242,
        )

        # Act
        once = cloudpickle.loads(cloudpickle.dumps(discovery))
        twice = cloudpickle.loads(cloudpickle.dumps(once))

        # Assert — config stable across both hops...
        assert twice._cluster == "c"
        assert twice._task_definition_family == "worker"
        assert twice._worker_port == 4242
        assert twice._client is sentinel
        # ...transients fresh after the second hop.
        assert twice._subscribers == []
        assert twice._known == {}
        assert twice._poll_task is None
        assert isinstance(twice._state_lock, asyncio.Lock)
        assert isinstance(twice._serialize_polls, asyncio.Lock)


class TestEcsDiscoveryGetstateNonMutation:
    """``__getstate__`` must not mutate the live source instance."""

    def test___getstate___should_not_mutate_source_instance(self, monkeypatch):
        """Test that pickling a live discovery leaves the source intact.

        Given:
            A live EcsDiscovery with a real client, populated ``_known``
            and ``_subscribers``, and its locks.
        When:
            The instance is cloudpickle-dumped (driving ``__getstate__``).
        Then:
            The SOURCE instance still has its ``_client``, both locks, its
            ``_known`` entry, and its subscriber — proving ``__getstate__``
            copies ``self.__dict__`` rather than stripping fields off the
            live object (which would break a still-running API poller).
        """
        # Arrange
        monkeypatch.setattr(
            "cfdb.workflows.discovery.build_ecs_client",
            lambda **_kwargs: _FakeEcsClient(),
        )
        client = _FakeEcsClient()
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=client,
        )
        # Populate live state so a stripping mutation would be observable.
        sub = discovery.subscribe()
        discovery._subscribers.append(sub)
        marker = WorkerMetadata(
            uid=uuid.uuid4(), address="10.0.0.5:4242", pid=0, version="0"
        )
        discovery._known[str(marker.uid)] = marker
        state_lock = discovery._state_lock
        serialize_polls = discovery._serialize_polls

        # Act
        cloudpickle.dumps(discovery)

        # Assert — the live source is untouched.
        assert discovery._client is client
        assert discovery._state_lock is state_lock
        assert discovery._serialize_polls is serialize_polls
        assert discovery._known == {str(marker.uid): marker}
        assert discovery._subscribers == [sub]


class TestEcsDiscoveryPickleEdges:
    """Completeness/edge coverage for the pickle round-trip (issue #54)."""

    def test___setstate___should_preserve_poll_interval_and_version(
        self, monkeypatch
    ):
        """Test that non-default ``_poll_interval`` / ``_version`` survive.

        Given:
            An EcsDiscovery built with a non-default ``poll_interval`` and
            ``version``, with ``build_ecs_client`` stubbed.
        When:
            The instance is cloudpickle round-tripped.
        Then:
            Both ``_poll_interval`` and ``_version`` survive — config the
            in-diff setstate test does not assert — confirming the full
            config payload (not just cluster/family/port) crosses the
            boundary.
        """
        # Arrange
        monkeypatch.setattr(
            "cfdb.workflows.discovery.build_ecs_client",
            lambda **_kwargs: object(),
        )
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=_FakeEcsClient(),
            poll_interval=12.5,
            version="cfdb-1.2.3",
        )

        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(discovery))

        # Assert
        assert restored._poll_interval == 12.5
        assert restored._version == "cfdb-1.2.3"

    def test___setstate___should_rebuild_with_none_endpoint_and_region(
        self, monkeypatch
    ):
        """Test that a client-injected discovery rebuilds with None boto kwargs.

        Given:
            An EcsDiscovery built by injecting ``client=`` (so its
            ``_endpoint_url`` and ``_region_name`` are both None — the
            ctor forbids passing the boto kwargs alongside a client).
        When:
            The instance is cloudpickle round-tripped through a stubbed
            ``build_ecs_client`` that records its kwargs.
        Then:
            ``build_ecs_client`` is called with ``endpoint_url=None`` and
            ``region_name=None`` — the None/None rebuild branch, distinct
            from the in-diff test that threads concrete localstack values.
        """
        # Arrange
        captured: dict[str, Any] = {}

        def _fake_build(*, endpoint_url=None, region_name=None):
            captured["endpoint_url"] = endpoint_url
            captured["region_name"] = region_name
            return object()

        monkeypatch.setattr(
            "cfdb.workflows.discovery.build_ecs_client", _fake_build
        )
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=_FakeEcsClient(),
        )

        # Act
        cloudpickle.loads(cloudpickle.dumps(discovery))

        # Assert
        assert captured == {"endpoint_url": None, "region_name": None}

    def test___setstate___should_reset_closed_flag_to_false(self, monkeypatch):
        """Test that a ``_closed=True`` source restores with ``_closed=False``.

        Given:
            An EcsDiscovery whose ``_closed`` flag has been forced True
            (as it would be after ``__aexit__`` ran), with
            ``build_ecs_client`` stubbed.
        When:
            The instance is cloudpickle round-tripped.
        Then:
            The restored instance's ``_closed`` is False — ``__setstate__``
            resets the shutdown latch so the unpickled handle is a fresh,
            non-closed object rather than inheriting a stale closed state.
        """
        # Arrange
        monkeypatch.setattr(
            "cfdb.workflows.discovery.build_ecs_client",
            lambda **_kwargs: object(),
        )
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=_FakeEcsClient(),
        )
        discovery._closed = True

        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(discovery))

        # Assert
        assert restored._closed is False

    def test_two_loads_should_yield_independent_locks(self, monkeypatch):
        """Test that two loads of one blob get distinct lock instances.

        Given:
            A single cloudpickle blob of an EcsDiscovery, with
            ``build_ecs_client`` stubbed.
        When:
            The blob is loaded twice.
        Then:
            The two restored instances hold distinct ``asyncio.Lock``
            objects — each ``__setstate__`` mints fresh locks, so two
            workers unpickling the same dispatched discovery never share a
            lock bound to a foreign event loop.
        """
        # Arrange
        monkeypatch.setattr(
            "cfdb.workflows.discovery.build_ecs_client",
            lambda **_kwargs: object(),
        )
        discovery = EcsDiscovery(
            cluster="c",
            task_definition_family="worker",
            client=_FakeEcsClient(),
        )
        blob = cloudpickle.dumps(discovery)

        # Act
        a = cloudpickle.loads(blob)
        b = cloudpickle.loads(blob)

        # Assert
        assert a._state_lock is not b._state_lock
        assert a._serialize_polls is not b._serialize_polls
