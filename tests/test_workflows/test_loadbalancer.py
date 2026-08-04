"""Tests for the priority (leaky-bucket) load balancer."""

from __future__ import annotations

import asyncio
import inspect
import uuid
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

import cloudpickle
import pytest
import wool
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from cfdb.workflows.loadbalancer import PriorityLoadBalancer


@dataclass(frozen=True)
class _FakeMeta:
    """Stand-in for WorkerMetadata; frozen so it is hashable."""

    uid: Any


class _FakeConn:
    """Opaque stand-in for WorkerConnection; the balancer never calls it."""


class _FakeContext:
    """Minimal LoadBalancerContextView keyed by uid, as wool 0.12+ requires."""

    def __init__(self, uids: list[Any]) -> None:
        self._workers = {
            uid: (_FakeMeta(uid), _FakeConn()) for uid in uids
        }

    @property
    def workers(self) -> MappingProxyType:
        return MappingProxyType(self._workers)

    def remove(self, uid: Any) -> None:
        """Evict a worker, standing in for what the proxy does mid-dispatch."""
        self._workers.pop(uid, None)


async def _candidates(balancer, context) -> list[Any]:
    """Drain the delegate generator, reporting every candidate as failed.

    Needs no iteration bound: the balancer's contract is that a pass
    terminates once its candidates are exhausted.
    """
    generator = balancer.delegate(object(), context=context)
    seen: list[Any] = []
    try:
        candidate = await anext(generator)
        while True:
            seen.append(candidate)
            candidate = await generator.athrow(wool.TransientRpcError(details="busy"))
    except StopAsyncIteration:
        pass
    finally:
        await generator.aclose()
    return seen


class TestPriorityLoadBalancer:
    def test_delegate_should_return_an_async_generator(self):
        """Test that delegate produces a driveable async generator.

        Given:
            A constructed balancer and a context.
        When:
            Delegate is called.
        Then:
            It should return an async generator rather than None — the
            failure mode of inheriting the protocol's `...` stub (#84).
        """
        # Arrange
        context = _FakeContext(["a"])

        # Act
        generator = PriorityLoadBalancer().delegate(object(), context=context)

        # Assert
        assert inspect.isasyncgen(generator)

    def test_delegate_should_conform_to_the_delegating_protocol_only(self):
        """Test that the balancer is classified as a delegating balancer.

        Given:
            A constructed balancer.
        When:
            It is checked against both of wool's runtime-checkable load
            balancer protocols.
        Then:
            It should satisfy LoadBalancerLike and NOT the deprecated
            DispatchingLoadBalancerLike, so WorkerProxy routes it down the
            delegate path it actually implements.
        """
        # Act
        balancer = PriorityLoadBalancer()

        # Assert
        assert isinstance(balancer, wool.LoadBalancerLike)
        assert not isinstance(balancer, wool.DispatchingLoadBalancerLike)

    @pytest.mark.asyncio
    async def test_delegate_should_yield_lowest_uid_first(self):
        """Test that the lowest-uid worker is offered the task first.

        Given:
            Three healthy workers inserted out of uid order.
        When:
            The first candidate is requested.
        Then:
            It should yield the lowest-uid worker (priority short-circuit).
        """
        # Arrange — insert c, a, b so insertion order != priority order.
        context = _FakeContext(["c", "a", "b"])
        generator = PriorityLoadBalancer().delegate(object(), context=context)

        # Act
        candidate = await anext(generator)

        # Assert
        assert candidate == "a"
        await generator.aclose()

    @pytest.mark.asyncio
    async def test_delegate_should_order_workers_by_string_form_of_uid(self):
        """Test that workers are ordered by the string form of their uid.

        Given:
            Two workers with non-string uids (10 and 2) whose lexicographic
            string order ("10" < "2") differs from numeric order.
        When:
            The first candidate is requested.
        Then:
            It should yield the worker whose str(uid) sorts first (10),
            pinning the str coercion that gives a total, orderable key
            regardless of the uid's concrete type.
        """
        # Arrange
        context = _FakeContext([10, 2])
        generator = PriorityLoadBalancer().delegate(object(), context=context)

        # Act
        candidate = await anext(generator)

        # Assert
        assert candidate == 10
        await generator.aclose()

    @pytest.mark.asyncio
    async def test_delegate_should_advance_when_dispatch_reports_failure(self):
        """Test that a reported dispatch failure advances to the next worker.

        Given:
            Three healthy workers.
        When:
            Each yielded candidate is reported failed via athrow.
        Then:
            It should offer every worker exactly once in priority order —
            the proxy owns eviction, so the balancer only advances.
        """
        # Arrange
        context = _FakeContext(["c", "a", "b"])

        # Act
        seen = await _candidates(PriorityLoadBalancer(), context)

        # Assert
        assert seen == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_delegate_should_advance_on_a_non_transient_error(self):
        """Test that a non-transient failure also just advances.

        Given:
            Two healthy workers.
        When:
            The first candidate is reported failed with a non-transient
            RpcError.
        Then:
            It should yield the next worker — classification and eviction
            moved to the proxy, so the balancer does not distinguish.
        """
        # Arrange
        context = _FakeContext(["a", "b"])
        generator = PriorityLoadBalancer().delegate(object(), context=context)
        await anext(generator)

        # Act
        candidate = await generator.athrow(wool.RpcError(details="broken"))

        # Assert
        assert candidate == "b"
        await generator.aclose()

    @pytest.mark.asyncio
    async def test_delegate_should_terminate_when_dispatch_succeeds(self):
        """Test that a success signal ends the generator.

        Given:
            A pool of three workers with the first candidate yielded.
        When:
            Success is reported by sending back the yielded uid.
        Then:
            It should terminate rather than offering another candidate —
            yielding after a success signal is a protocol violation.
        """
        # Arrange
        context = _FakeContext(["a", "b", "c"])
        generator = PriorityLoadBalancer().delegate(object(), context=context)
        candidate = await anext(generator)

        # Act & assert
        with pytest.raises(StopAsyncIteration):
            await generator.asend(candidate)

    @pytest.mark.asyncio
    async def test_delegate_should_terminate_when_successful_uid_is_falsy(self):
        """Test that success is detected by identity, not truthiness.

        Given:
            A pool whose highest-priority worker has a falsy but perfectly
            valid uid, and a second worker behind it.
        When:
            That first candidate is yielded and success is signalled by
            sending the same falsy uid back.
        Then:
            It should terminate rather than offer the second worker,
            because only a None resume means the candidate was skipped —
            weakening the check to `not resumed` would mistake a
            successful dispatch for a skip and re-dispatch the task.
        """
        # Arrange — 0 sorts before 1 by str, so the falsy uid goes first.
        context = _FakeContext([0, 1])
        generator = PriorityLoadBalancer().delegate(object(), context=context)
        candidate = await anext(generator)
        assert candidate == 0

        # Act & assert
        with pytest.raises(StopAsyncIteration):
            await generator.asend(candidate)

    @pytest.mark.asyncio
    async def test_delegate_should_advance_when_candidate_was_skipped(self):
        """Test that a None resume advances instead of ending the pass.

        Given:
            A pool of two workers with the first candidate yielded.
        When:
            The proxy resumes with None, meaning that candidate had left the
            pool and was skipped rather than dispatched to.
        Then:
            It should yield the next candidate — treating a skip as a
            success would silently drop the task.
        """
        # Arrange
        context = _FakeContext(["a", "b"])
        generator = PriorityLoadBalancer().delegate(object(), context=context)
        await anext(generator)

        # Act
        candidate = await generator.asend(None)

        # Assert
        assert candidate == "b"
        await generator.aclose()

    @pytest.mark.asyncio
    async def test_delegate_should_end_immediately_when_pool_is_empty(self):
        """Test that an empty pool yields nothing.

        Given:
            A context with no workers.
        When:
            The first candidate is requested.
        Then:
            It should raise StopAsyncIteration, which the proxy converts to
            NoWorkersAvailable (the executor's signal to add a worker and
            re-queue).
        """
        # Arrange
        generator = PriorityLoadBalancer().delegate(
            object(), context=_FakeContext([])
        )

        # Act & assert
        with pytest.raises(StopAsyncIteration):
            await anext(generator)

    @pytest.mark.asyncio
    async def test_delegate_should_end_after_every_worker_is_offered(self):
        """Test that the pass ends once the candidates are exhausted.

        Given:
            Two workers that both fail their dispatch.
        When:
            Both are reported failed and a third candidate is requested.
        Then:
            It should end the generator after one stable-order pass rather
            than cycling, so the proxy surfaces NoWorkersAvailable.
        """
        # Arrange
        context = _FakeContext(["a", "b"])
        generator = PriorityLoadBalancer().delegate(object(), context=context)
        await anext(generator)
        await generator.athrow(wool.TransientRpcError(details="busy"))

        # Act & assert
        with pytest.raises(StopAsyncIteration):
            await generator.athrow(wool.TransientRpcError(details="busy"))

    @pytest.mark.asyncio
    async def test_delegate_should_end_when_the_last_candidate_is_skipped(self):
        """Test that a pass exhausted by a skip ends like one exhausted by failure.

        Given:
            A pool of exactly one worker.
        When:
            That candidate's turn ends via the skip path — the proxy
            resumes with None because the worker left the pool — rather
            than via a reported failure.
        Then:
            It should end the generator, so the proxy still surfaces
            NoWorkersAvailable instead of hanging or re-offering a
            candidate that is already gone.
        """
        # Arrange
        context = _FakeContext(["a"])
        generator = PriorityLoadBalancer().delegate(object(), context=context)
        await anext(generator)

        # Act & assert
        with pytest.raises(StopAsyncIteration):
            await generator.asend(None)

    @pytest.mark.asyncio
    async def test_delegate_should_skip_a_worker_that_left_the_pool(self):
        """Test that a departed worker is not offered as a candidate.

        Given:
            Three workers, where the second leaves the pool after the first
            candidate is yielded (as the proxy's eviction would do).
        When:
            The first candidate is reported failed.
        Then:
            It should skip the departed worker and yield the third, so a
            candidate the proxy would only discard is never offered.
        """
        # Arrange
        context = _FakeContext(["a", "b", "c"])
        generator = PriorityLoadBalancer().delegate(object(), context=context)
        await anext(generator)
        context.remove("b")

        # Act
        candidate = await generator.athrow(wool.TransientRpcError(details="busy"))

        # Assert
        assert candidate == "c"
        await generator.aclose()

    @pytest.mark.asyncio
    async def test_delegate_should_pick_same_priority_worker_across_passes(self):
        """Test that the priority order is stable across repeated passes.

        Given:
            Three healthy workers and one balancer reused across two
            consecutive delegate passes.
        When:
            The first candidate of each pass is requested.
        Then:
            Both should be the lowest-uid worker — the leaky-bucket
            invariant, distinct from round-robin which advances each call.
        """
        # Arrange
        context = _FakeContext(["a", "b", "c"])
        balancer = PriorityLoadBalancer()

        # Act
        first_pass = balancer.delegate(object(), context=context)
        first = await anext(first_pass)
        await first_pass.aclose()
        second_pass = balancer.delegate(object(), context=context)
        second = await anext(second_pass)
        await second_pass.aclose()

        # Assert
        assert first == "a"
        assert second == "a"

    @pytest.mark.asyncio
    async def test_delegate_should_yield_worker_uids_not_metadata(self):
        """Test that candidates are the pool's uid keys.

        Given:
            A pool keyed by UUID uids, as wool's context view exposes it.
        When:
            The first candidate is requested.
        Then:
            It should be the uid itself, since the proxy resolves the name
            against the pool it owns at the moment it dispatches.
        """
        # Arrange
        uid = uuid.uuid4()
        context = _FakeContext([uid])
        generator = PriorityLoadBalancer().delegate(object(), context=context)

        # Act
        candidate = await anext(generator)

        # Assert
        assert candidate == uid
        await generator.aclose()

    @pytest.mark.asyncio
    async def test_delegate_should_propagate_cancellation(self):
        """Test that cancellation is not absorbed as a dispatch failure.

        Given:
            A delegate generator suspended on its first candidate.
        When:
            CancelledError is thrown in, as task cancellation does.
        Then:
            It should propagate rather than advance to the next worker —
            pinning that only Exception, not BaseException, is treated as a
            failed dispatch.
        """
        # Arrange
        context = _FakeContext(["a", "b"])
        generator = PriorityLoadBalancer().delegate(object(), context=context)
        await anext(generator)

        # Act & assert
        with pytest.raises(asyncio.CancelledError):
            await generator.athrow(asyncio.CancelledError())

    @pytest.mark.asyncio
    async def test_delegate_should_close_cleanly_while_suspended(self):
        """Test that closing a suspended generator does not error.

        Given:
            A delegate generator suspended on its first candidate, as the
            proxy leaves it when a dispatch resolves.
        When:
            The generator is closed.
        Then:
            It should close without raising — a balancer that swallowed
            GeneratorExit would instead fail with "async generator ignored
            GeneratorExit".
        """
        # Arrange
        context = _FakeContext(["a", "b"])
        generator = PriorityLoadBalancer().delegate(object(), context=context)
        await anext(generator)

        # Act
        await generator.aclose()

        # Assert
        with pytest.raises(StopAsyncIteration):
            await anext(generator)

    @pytest.mark.asyncio
    async def test_delegate_should_be_stateless_across_concurrent_passes(self):
        """Test that concurrent passes do not share rotation state.

        Given:
            One balancer and one context, with two delegate passes opened
            concurrently.
        When:
            The first candidate of each pass is requested.
        Then:
            Both should yield the same lowest-uid worker — the balancer
            keeps no per-context cursor, which is what makes its trivial
            __reduce__ correct.
        """
        # Arrange
        context = _FakeContext(["a", "b", "c"])
        balancer = PriorityLoadBalancer()
        first_pass = balancer.delegate(object(), context=context)
        second_pass = balancer.delegate(object(), context=context)

        # Act
        first, second = await asyncio.gather(
            anext(first_pass), anext(second_pass)
        )

        # Assert
        assert first == "a"
        assert second == "a"
        await first_pass.aclose()
        await second_pass.aclose()

    @given(
        uids=st.lists(
            st.one_of(st.integers(), st.text(), st.uuids()),
            min_size=1,
            max_size=8,
            unique_by=str,
        )
    )
    @settings(max_examples=100)
    def test_delegate_should_offer_every_worker_in_string_uid_order(self, uids):
        """Test the priority ordering invariant over arbitrary uids.

        Given:
            Any non-empty pool of uids drawn from a mixed domain of
            integers, strings, and UUIDs.
        When:
            Every candidate is drained, each reported as a failed dispatch.
        Then:
            The sequence should be exactly the uids sorted by their string
            form, covering the domain the str coercion exists for.
        """
        # Arrange
        context = _FakeContext(uids)

        # Act
        seen = asyncio.run(_candidates(PriorityLoadBalancer(), context))

        # Assert
        assert seen == sorted(uids, key=str)

    @pytest.mark.asyncio
    async def test_delegate_should_yield_lowest_uid_first_when_reconstructed(self):
        """Test that a cloudpickled balancer still delegates correctly.

        Given:
            A PriorityLoadBalancer round-tripped through cloudpickle, as it
            is when carried across the worker-spawn boundary.
        When:
            The restored instance is delegated through.
        Then:
            It should yield the lowest-uid worker — a type check alone
            would pass even on an instance whose delegate was broken.
        """
        # Arrange
        restored = cloudpickle.loads(cloudpickle.dumps(PriorityLoadBalancer()))
        context = _FakeContext(["c", "a", "b"])

        # Act
        generator = restored.delegate(object(), context=context)
        candidate = await anext(generator)

        # Assert
        assert isinstance(restored, PriorityLoadBalancer)
        assert candidate == "a"
        await generator.aclose()
