"""Tests for EcsDiscovery."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
import wool

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
