"""Tests for EcsProvisioner."""

from __future__ import annotations

import asyncio

import pytest

from cfdb.workflows.provisioner import RetryableProvisionerError, EcsProvisioner


class _FakeEcsClient:
    """In-memory ECS client recording RunTask calls for assertions."""

    def __init__(
        self,
        *,
        response: dict | None = None,
        raise_on_call: Exception | None = None,
        list_tasks_arns: list[str] | None = None,
        list_tasks_raise: Exception | None = None,
    ) -> None:
        self.calls: list[dict] = []
        self.list_tasks_calls: list[dict] = []
        self._response = response or {
            "tasks": [{"taskArn": "arn:aws:ecs:::task/cluster/abc"}],
            "failures": [],
        }
        self._raise = raise_on_call
        self._list_tasks_arns = list(list_tasks_arns or [])
        self._list_tasks_raise = list_tasks_raise

    def run_task(self, **kwargs):
        self.calls.append(kwargs)
        if self._raise is not None:
            raise self._raise
        return self._response

    def list_tasks(self, **kwargs):
        self.list_tasks_calls.append(kwargs)
        if self._list_tasks_raise is not None:
            raise self._list_tasks_raise
        return {"taskArns": list(self._list_tasks_arns)}


class _SimpleGatedClient(_FakeEcsClient):
    """run_task blocks on a threading.Event until released by the test."""

    def __init__(self) -> None:
        super().__init__()
        import threading

        self._gate = threading.Event()
        self._call_started = threading.Event()

    def run_task(self, **kwargs):
        self.calls.append(kwargs)
        self._call_started.set()
        # Block the worker thread until the test releases the gate.
        self._gate.wait()
        return self._response

    def release(self) -> None:
        self._gate.set()


def _client_error(code: str) -> Exception:
    """Construct a botocore ClientError with a structured response dict."""
    from botocore.exceptions import ClientError

    return ClientError(
        {"Error": {"Code": code, "Message": "simulated"}, "ResponseMetadata": {"HTTPStatusCode": 500}},
        "RunTask",
    )


class TestEcsProvisioner:
    def test___init___without_cluster(self):
        """Test that constructing EcsProvisioner without a cluster fails fast.

        Given:
            No cluster name.
        When:
            EcsProvisioner is constructed.
        Then:
            It should raise ValueError so misconfiguration is caught at boot.
        """
        # Act & assert
        with pytest.raises(ValueError, match="cluster"):
            EcsProvisioner(
                cluster="",
                task_definition="worker",
                subnets=["subnet-1"],
                client=_FakeEcsClient(),
            )

    def test___init___without_task_definition(self):
        """Test that omitting task_definition raises ValueError.

        Given:
            A cluster but no task definition.
        When:
            EcsProvisioner is constructed.
        Then:
            It should raise ValueError to surface the misconfiguration.
        """
        # Act & assert
        with pytest.raises(ValueError, match="task_definition"):
            EcsProvisioner(
                cluster="c",
                task_definition="",
                subnets=["subnet-1"],
                client=_FakeEcsClient(),
            )

    def test___init___without_subnets(self):
        """Test that constructing without subnets raises ValueError.

        Given:
            A cluster and task definition but no subnets.
        When:
            EcsProvisioner is constructed.
        Then:
            It should raise ValueError because awsvpc requires at least one.
        """
        # Act & assert
        with pytest.raises(ValueError, match="subnet"):
            EcsProvisioner(
                cluster="c",
                task_definition="worker",
                subnets=[],
                client=_FakeEcsClient(),
            )

    @pytest.mark.asyncio
    async def test_request_with_single_caller(self):
        """Test that request returns the launched task ARNs.

        Given:
            A provisioner backed by a fake client returning one task ARN.
        When:
            request is awaited once.
        Then:
            It should return the list of ARNs and have invoked RunTask once
            with the configured cluster, family, and awsvpc network config.
        """
        # Arrange
        client = _FakeEcsClient()
        provisioner = EcsProvisioner(
            cluster="c",
            task_definition="worker",
            subnets=["subnet-1"],
            security_groups=["sg-1"],
            client=client,
        )

        # Act
        arns = await provisioner.request(dedup_key="wf-1")

        # Assert
        assert arns == ["arn:aws:ecs:::task/cluster/abc"]
        assert len(client.calls) == 1
        call = client.calls[0]
        assert call["cluster"] == "c"
        assert call["taskDefinition"] == "worker"
        assert call["launchType"] == "FARGATE"
        awsvpc = call["networkConfiguration"]["awsvpcConfiguration"]
        assert awsvpc["subnets"] == ["subnet-1"]
        assert awsvpc["securityGroups"] == ["sg-1"]

    @pytest.mark.asyncio
    async def test_request_with_concurrent_dedup_key_collisions(self):
        """Test that concurrent calls with the same dedup_key share one RunTask.

        Given:
            A provisioner whose underlying RunTask call is gated on a
            threading.Event so the first caller stays in-flight.
        When:
            Two coroutines call request concurrently with the same dedup_key.
        Then:
            Only one RunTask invocation should be issued, and both callers
            should observe identical ARNs.
        """
        # Arrange
        client = _SimpleGatedClient()
        provisioner = EcsProvisioner(
            cluster="c",
            task_definition="worker",
            subnets=["subnet-1"],
            client=client,
        )

        # Act
        first = asyncio.create_task(provisioner.request(dedup_key="wf-1"))
        # Wait until the first call is mid-flight before starting the
        # second so the dedup map is populated.
        await asyncio.to_thread(client._call_started.wait)
        second = asyncio.create_task(provisioner.request(dedup_key="wf-1"))
        # Release the gated client so both callers can return.
        client.release()
        first_arns, second_arns = await asyncio.gather(first, second)

        # Assert
        assert first_arns == second_arns == ["arn:aws:ecs:::task/cluster/abc"]
        assert len(client.calls) == 1

    @pytest.mark.asyncio
    async def test_request_with_distinct_dedup_keys(self):
        """Test that distinct dedup keys produce independent RunTask calls.

        Given:
            A provisioner backed by a fake client.
        When:
            request is awaited twice with different dedup_keys.
        Then:
            Two RunTask invocations should be issued.
        """
        # Arrange
        client = _FakeEcsClient()
        provisioner = EcsProvisioner(
            cluster="c",
            task_definition="worker",
            subnets=["subnet-1"],
            client=client,
        )

        # Act
        await provisioner.request(dedup_key="wf-a")
        await provisioner.request(dedup_key="wf-b")

        # Assert
        assert len(client.calls) == 2

    @pytest.mark.asyncio
    async def test_request_with_capacity_client_error(self):
        """Test that capacity ClientErrors map to RetryableProvisionerError.

        Given:
            A provisioner whose underlying client raises a Capacity error.
        When:
            request is awaited.
        Then:
            It should raise RetryableProvisionerError so callers can surface it as a
            retryable terminal failure.
        """
        # Arrange
        client = _FakeEcsClient(raise_on_call=_client_error("CapacityProviderException"))
        provisioner = EcsProvisioner(
            cluster="c",
            task_definition="worker",
            subnets=["subnet-1"],
            client=client,
        )

        # Act & assert
        with pytest.raises(RetryableProvisionerError):
            await provisioner.request(dedup_key="wf-1")

    @pytest.mark.asyncio
    async def test_request_with_capacity_failure_in_response(self):
        """Test that capacity failures inside the RunTask response also raise.

        Given:
            A provisioner whose RunTask response carries a failures entry
            whose reason includes RESOURCE: tokens.
        When:
            request is awaited.
        Then:
            It should raise RetryableProvisionerError rather than treating the call
            as a success with zero ARNs.
        """
        # Arrange
        client = _FakeEcsClient(
            response={
                "tasks": [],
                "failures": [{"reason": "RESOURCE:CPU"}],
            }
        )
        provisioner = EcsProvisioner(
            cluster="c",
            task_definition="worker",
            subnets=["subnet-1"],
            client=client,
        )

        # Act & assert
        with pytest.raises(RetryableProvisionerError):
            await provisioner.request(dedup_key="wf-1")

    @pytest.mark.asyncio
    async def test_request_with_partial_failure_preserves_arn(self):
        """Test that a launched ARN is preserved even when failures[] is non-empty.

        Given:
            A provisioner whose RunTask response carries both a launched
            taskArn and a non-retryable failures entry.
        When:
            request is awaited.
        Then:
            It should return the launched ARN and log the failure rather
            than discarding the worker that is already running.
        """
        # Arrange
        client = _FakeEcsClient(
            response={
                "tasks": [{"taskArn": "arn:aws:ecs:::task/cluster/abc"}],
                "failures": [{"reason": "secondary placement warning"}],
            }
        )
        provisioner = EcsProvisioner(
            cluster="c",
            task_definition="worker",
            subnets=["subnet-1"],
            client=client,
        )

        # Act
        arns = await provisioner.request(dedup_key="wf-1")

        # Assert
        assert arns == ["arn:aws:ecs:::task/cluster/abc"]

    @pytest.mark.asyncio
    async def test_request_with_done_cached_task_launches_fresh_run(self):
        """Test that a stale completed in-flight entry is replaced on next request.

        Given:
            A provisioner where the first request completed and the dedup
            slot still holds the done task (simulating a release-slot race
            where the post-completion pop was skipped).
        When:
            A second request with the same dedup_key is awaited.
        Then:
            A fresh RunTask is issued rather than the second caller
            attaching to the already-finished task.
        """
        # Arrange
        client = _FakeEcsClient()
        provisioner = EcsProvisioner(
            cluster="c",
            task_definition="worker",
            subnets=["subnet-1"],
            client=client,
        )
        first = await provisioner.request(dedup_key="wf-shared")
        # Simulate the post-completion pop having been skipped (e.g. by a
        # CancelledError that re-raised out of the shielded release): put
        # the done task back into the dedup map.
        done_task = asyncio.create_task(_resolved(first))
        await done_task
        provisioner._in_flight["wf-shared"] = done_task

        # Act
        second = await provisioner.request(dedup_key="wf-shared")

        # Assert
        assert second == first
        assert len(client.calls) == 2

    @pytest.mark.asyncio
    async def test_aclose_cancels_in_flight_and_clears_map(self):
        """Test that aclose cancels in-flight tasks and clears the dedup map.

        Given:
            A provisioner with one in-flight RunTask whose underlying
            boto3 call is mid-flight on the ``asyncio.to_thread`` worker.
        When:
            aclose is awaited.
        Then:
            The asyncio task is cancelled, the dedup map is empty, and
            the original caller observes CancelledError rather than a hang.
        """
        # Arrange — use a client that simulates a slow round-trip via a
        # short ``time.sleep`` in the worker thread. ``asyncio.to_thread``
        # cannot cancel the running thread, but the wrapping task can be
        # cancelled — which is what ``aclose`` does. The short sleep
        # makes the thread exit promptly so pytest's executor join at
        # session teardown does not hit a 300 s timeout.
        import time

        class _SlowClient(_FakeEcsClient):
            def run_task(self, **kwargs):
                self.calls.append(kwargs)
                time.sleep(0.5)
                return self._response

        client = _SlowClient()
        provisioner = EcsProvisioner(
            cluster="c",
            task_definition="worker",
            subnets=["subnet-1"],
            client=client,
        )
        caller = asyncio.create_task(provisioner.request(dedup_key="wf-1"))
        # Yield so the dedup-registration task is scheduled and the
        # boto thread call has begun.
        await asyncio.sleep(0.05)

        # Act
        await provisioner.aclose()

        # Assert
        assert provisioner._in_flight == {}
        with pytest.raises(asyncio.CancelledError):
            await caller


async def _resolved(value):
    """Helper coroutine that immediately returns ``value``.

    Used by the dedup-self-heal test to construct a real done task that
    holds the same return value as the first request.
    """
    return value


class TestEcsProvisionerWorkerCap:
    def test___init___rejects_negative_max_workers(self):
        """Test that a negative max_workers is rejected at construction.

        Given:
            A negative ``max_workers`` value.
        When:
            EcsProvisioner is constructed.
        Then:
            It should raise ValueError so the misconfiguration is caught at
            boot rather than silently disabling the cap.
        """
        # Act & assert
        with pytest.raises(ValueError, match="max_workers"):
            EcsProvisioner(
                cluster="c",
                task_definition="worker",
                subnets=["subnet-1"],
                client=_FakeEcsClient(),
                max_workers=-1,
            )

    @pytest.mark.asyncio
    async def test_request_should_skip_spawn_when_fleet_at_capacity(self):
        """Test that request does not launch a task when the fleet is at the cap.

        Given:
            A provisioner with ``max_workers=2`` whose ``list_tasks`` reports
            two running worker tasks.
        When:
            request is awaited.
        Then:
            It should count the fleet, skip RunTask entirely, and return an
            empty ARN list so the caller queues the job instead of spawning a
            third worker.
        """
        # Arrange
        client = _FakeEcsClient(
            list_tasks_arns=["arn:task/w1", "arn:task/w2"],
        )
        provisioner = EcsProvisioner(
            cluster="c",
            task_definition="worker:7",
            subnets=["subnet-1"],
            client=client,
            max_workers=2,
        )

        # Act
        arns = await provisioner.request(dedup_key="wf-1")

        # Assert
        assert arns == []
        assert client.calls == []  # RunTask never invoked
        assert len(client.list_tasks_calls) == 1
        # The :revision suffix is stripped for the family filter.
        assert client.list_tasks_calls[0]["family"] == "worker"
        assert client.list_tasks_calls[0]["desiredStatus"] == "RUNNING"

    @pytest.mark.asyncio
    async def test_request_should_spawn_when_below_capacity(self):
        """Test that request launches a task when the fleet is below the cap.

        Given:
            A provisioner with ``max_workers=3`` whose ``list_tasks`` reports
            one running worker task.
        When:
            request is awaited.
        Then:
            It should launch one worker via RunTask and return its ARN.
        """
        # Arrange
        client = _FakeEcsClient(list_tasks_arns=["arn:task/w1"])
        provisioner = EcsProvisioner(
            cluster="c",
            task_definition="worker",
            subnets=["subnet-1"],
            client=client,
            max_workers=3,
        )

        # Act
        arns = await provisioner.request(dedup_key="wf-1")

        # Assert
        assert arns == ["arn:aws:ecs:::task/cluster/abc"]
        assert len(client.calls) == 1
        assert len(client.list_tasks_calls) == 1

    @pytest.mark.asyncio
    async def test_request_should_not_count_fleet_when_cap_disabled(self):
        """Test that max_workers=0 disables the fleet count entirely.

        Given:
            A provisioner with the default ``max_workers=0`` (cap disabled).
        When:
            request is awaited.
        Then:
            It should launch a worker without ever calling ``list_tasks``, so
            an unset cap incurs no extra ECS round-trip and never throttles.
        """
        # Arrange
        client = _FakeEcsClient(list_tasks_arns=["arn:task/w1", "arn:task/w2"])
        provisioner = EcsProvisioner(
            cluster="c",
            task_definition="worker",
            subnets=["subnet-1"],
            client=client,
        )

        # Act
        arns = await provisioner.request(dedup_key="wf-1")

        # Assert
        assert arns == ["arn:aws:ecs:::task/cluster/abc"]
        assert client.list_tasks_calls == []

    @pytest.mark.asyncio
    async def test_request_should_cap_a_concurrent_cold_fleet_burst(self):
        """Test that a simultaneous burst from a list_tasks-blind fleet is capped.

        Given:
            ``max_workers=2`` and a client whose ``list_tasks`` always reports
            zero running tasks — modeling the ECS eventual-consistency lag
            where freshly launched workers are not yet visible (the live
            failure mode where every decider saw a stale 0 and all spawned).
        When:
            Five distinct-key requests are awaited concurrently.
        Then:
            Only two RunTask calls happen: the in-flight-launch accounting
            bounds the burst even though ``list_tasks`` never reflects the new
            workers, and the excess three requests return [] (queue, not
            spawn).
        """
        # Arrange — list_tasks is permanently blind (always 0 visible).
        client = _FakeEcsClient(list_tasks_arns=[])
        provisioner = EcsProvisioner(
            cluster="c",
            task_definition="worker",
            subnets=["subnet-1"],
            client=client,
            max_workers=2,
        )

        # Act
        results = await asyncio.gather(
            *(provisioner.request(dedup_key=f"wf-{i}") for i in range(5))
        )

        # Assert — at most the cap launched, despite list_tasks reporting 0.
        spawned = [r for r in results if r]
        skipped = [r for r in results if r == []]
        assert len(client.calls) == 2, f"expected 2 RunTask, got {len(client.calls)}"
        assert len(spawned) == 2
        assert len(skipped) == 3

    @pytest.mark.asyncio
    async def test_request_should_raise_retryable_when_list_tasks_fails(self):
        """Test that a list_tasks failure surfaces as a retryable error.

        Given:
            A provisioner with a cap whose ``list_tasks`` raises a
            botocore ClientError.
        When:
            request is awaited.
        Then:
            It should raise RetryableProvisionerError (so the caller queues
            and retries) and never launch a task blind past the cap.
        """
        # Arrange
        client = _FakeEcsClient(list_tasks_raise=_client_error("ThrottlingException"))
        provisioner = EcsProvisioner(
            cluster="c",
            task_definition="worker",
            subnets=["subnet-1"],
            client=client,
            max_workers=2,
        )

        # Act & assert
        with pytest.raises(RetryableProvisionerError, match="list_tasks failed"):
            await provisioner.request(dedup_key="wf-1")
        assert client.calls == []


# ---------------------------------------------------------------------------
# Moto-backed wire-shape verification.
#
# Pure unit tests above use ``_FakeEcsClient`` to assert call shape and
# branch coverage. The class below runs the same provisioner against a
# real boto3 ``ecs`` client wired into ``moto``'s in-process AWS
# simulator — any drift between our kwarg shape and what ECS actually
# accepts surfaces as a moto rejection, catching the class of bug
# pure mocks cannot (e.g. a misspelled ``networkConfiguration`` key,
# an unsupported ``launchType`` value, an unregistered cluster).
# ---------------------------------------------------------------------------


@pytest.fixture()
def moto_ecs_env():
    """Stand up a moto-backed ECS cluster + task definition + VPC.

    Returns a ``(client, cluster, task_definition, subnet, security_group)``
    bundle for the provisioner to target. The VPC has
    ``EnableDnsHostnames`` enabled because moto's ECS ``RunTask`` model
    unconditionally reads ``eni.private_dns_name`` and crashes when the
    attribute is unset.
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
        yield {
            "client": ecs,
            "cluster": "cfdb-test",
            "task_definition": "cfdb-test-worker",
            "subnet": subnet,
            "security_group": sg,
        }


class TestEcsProvisionerAgainstMoto:
    """Wire-shape verification against a real boto3 ``ecs`` client.

    These tests are slower than the ``_FakeEcsClient`` ones (a few
    hundred milliseconds for VPC + cluster + task-def setup) so they
    live in a separate class. They guard against accidental drift in
    the ``RunTask`` kwarg names / enum values that pure mocks cannot
    detect.
    """

    @pytest.mark.asyncio
    async def test_request_should_launch_a_task_when_moto_accepts_the_kwargs(
        self, moto_ecs_env
    ):
        """Test that RunTask is accepted by moto and returns a task ARN.

        Given:
            A provisioner targeting a moto-backed cluster with a
            registered Fargate task definition and a valid awsvpc
            subnet + security group.
        When:
            request is awaited with a fresh dedup_key.
        Then:
            It should return one task ARN matching the ARN ECS recorded,
            with no failure entries, proving the kwarg shape is accepted.
        """
        # Arrange
        provisioner = EcsProvisioner(
            cluster=moto_ecs_env["cluster"],
            task_definition=moto_ecs_env["task_definition"],
            subnets=[moto_ecs_env["subnet"]],
            security_groups=[moto_ecs_env["security_group"]],
            client=moto_ecs_env["client"],
        )
        try:
            # Act
            arns = await provisioner.request(dedup_key="wf-moto-1")

            # Assert
            assert len(arns) == 1
            assert arns[0].startswith(
                "arn:aws:ecs:us-east-1:123456789012:task/cfdb-test/"
            )
            recorded = moto_ecs_env["client"].list_tasks(
                cluster=moto_ecs_env["cluster"],
                family=moto_ecs_env["task_definition"],
                desiredStatus="RUNNING",
            )["taskArns"]
            assert arns[0] in recorded
        finally:
            await provisioner.aclose()

    @pytest.mark.asyncio
    async def test_request_should_send_fargate_awsvpc_configuration(
        self, moto_ecs_env
    ):
        """Test that the awsvpc network configuration round-trips through ECS.

        Given:
            A provisioner with explicit subnet, security group, and
            assignPublicIp=DISABLED.
        When:
            request launches a task and DescribeTasks queries it back.
        Then:
            The launched task should carry a FARGATE launchType and an
            ElasticNetworkInterface attachment, confirming the awsvpc
            configuration reached the ECS backend intact.
        """
        # Arrange
        provisioner = EcsProvisioner(
            cluster=moto_ecs_env["cluster"],
            task_definition=moto_ecs_env["task_definition"],
            subnets=[moto_ecs_env["subnet"]],
            security_groups=[moto_ecs_env["security_group"]],
            assign_public_ip="DISABLED",
            client=moto_ecs_env["client"],
        )
        try:
            # Act
            arns = await provisioner.request(dedup_key="wf-moto-net")
            described = moto_ecs_env["client"].describe_tasks(
                cluster=moto_ecs_env["cluster"], tasks=arns
            )["tasks"]

            # Assert
            assert len(described) == 1
            task = described[0]
            assert task.get("launchType") == "FARGATE"
            eni_attachments = [
                a
                for a in task.get("attachments", [])
                if a.get("type") == "ElasticNetworkInterface"
            ]
            assert eni_attachments, (
                "moto did not produce an ENI attachment — awsvpc kwargs "
                "may not have reached the backend correctly"
            )
        finally:
            await provisioner.aclose()

    @pytest.mark.asyncio
    async def test_request_should_classify_throttling_as_retryable_against_moto(
        self, moto_ecs_env, mocker
    ):
        """Test that ThrottlingException from RunTask becomes RetryableProvisionerError.

        Given:
            A provisioner whose underlying boto3 RunTask is monkey-
            patched to raise the canonical ClientError moto would
            emit if ECS were throttling.
        When:
            request is awaited.
        Then:
            It should raise RetryableProvisionerError so the executor
            surfaces the workflow failure with a retryable contract.
        """
        # Arrange
        provisioner = EcsProvisioner(
            cluster=moto_ecs_env["cluster"],
            task_definition=moto_ecs_env["task_definition"],
            subnets=[moto_ecs_env["subnet"]],
            client=moto_ecs_env["client"],
        )
        mocker.patch.object(
            moto_ecs_env["client"], "run_task", side_effect=_client_error("ThrottlingException")
        )
        try:
            # Act & assert
            with pytest.raises(RetryableProvisionerError):
                await provisioner.request(dedup_key="wf-moto-throttle")
        finally:
            await provisioner.aclose()
