"""Tests for the priority (leaky-bucket) load balancer."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

import cloudpickle
import pytest
import wool

from cfdb.workflows.loadbalancer import PriorityLoadBalancer


@dataclass(frozen=True)
class _FakeMeta:
    """Stand-in for WorkerMetadata; frozen so it is hashable as a dict key."""

    uid: str


class _FakeConn:
    """Worker connection stub whose dispatch returns a stream or raises."""

    def __init__(self, behavior: Any) -> None:
        # behavior: a sentinel "stream" to return, or an Exception to raise.
        self.behavior = behavior
        self.calls = 0

    async def dispatch(self, task: Any, *, timeout: float | None = None) -> Any:
        self.calls += 1
        if isinstance(self.behavior, Exception):
            raise self.behavior
        return self.behavior


class _FakeContext:
    """Minimal LoadBalancerContextLike backed by an ordered dict."""

    def __init__(self, workers: dict[_FakeMeta, _FakeConn]) -> None:
        self._workers = dict(workers)
        self.removed: list[_FakeMeta] = []

    @property
    def workers(self) -> MappingProxyType:
        return MappingProxyType(self._workers)

    def add_worker(self, metadata: _FakeMeta, connection: _FakeConn) -> None:
        self._workers[metadata] = connection

    def update_worker(self, metadata, connection, *, upsert: bool = False) -> None:
        if upsert or metadata in self._workers:
            self._workers[metadata] = connection

    def remove_worker(self, metadata: _FakeMeta) -> None:
        self.removed.append(metadata)
        self._workers.pop(metadata, None)


class TestPriorityLoadBalancer:
    @pytest.mark.asyncio
    async def test_dispatch_should_return_first_worker_in_priority_order(self):
        """Test that the lowest-uid worker is offered the task first.

        Given:
            Three healthy workers inserted out of uid order.
        When:
            A task is dispatched.
        Then:
            It should return the lowest-uid worker's stream and not even
            contact the higher-uid workers (priority short-circuit).
        """
        # Arrange — insert c, a, b so insertion order != priority order.
        conns = {
            _FakeMeta("c"): _FakeConn("stream-c"),
            _FakeMeta("a"): _FakeConn("stream-a"),
            _FakeMeta("b"): _FakeConn("stream-b"),
        }
        context = _FakeContext(conns)

        # Act
        stream = await PriorityLoadBalancer().dispatch(object(), context=context)

        # Assert
        assert stream == "stream-a"
        assert conns[_FakeMeta("a")].calls == 1
        assert conns[_FakeMeta("b")].calls == 0
        assert conns[_FakeMeta("c")].calls == 0

    @pytest.mark.asyncio
    async def test_dispatch_should_rotate_past_busy_worker_without_evicting(self):
        """Test that a transiently-rejecting worker is skipped, not evicted.

        Given:
            The priority worker rejects with TransientRpcError (a
            backpressure RESOURCE_EXHAUSTED) and the next accepts.
        When:
            A task is dispatched.
        Then:
            It should return the next worker's stream and leave the busy
            worker in the pool (no eviction).
        """
        # Arrange
        busy = _FakeConn(wool.TransientRpcError(details="busy"))
        free = _FakeConn("stream-b")
        context = _FakeContext({_FakeMeta("a"): busy, _FakeMeta("b"): free})

        # Act
        stream = await PriorityLoadBalancer().dispatch(object(), context=context)

        # Assert
        assert stream == "stream-b"
        assert busy.calls == 1
        assert context.removed == []

    @pytest.mark.asyncio
    async def test_dispatch_should_evict_worker_on_non_transient_error(self):
        """Test that a non-transient RpcError evicts the failing worker.

        Given:
            The priority worker fails with a non-transient RpcError and the
            next accepts.
        When:
            A task is dispatched.
        Then:
            It should return the next worker's stream and evict the failing
            worker via remove_worker.
        """
        # Arrange
        broken_meta = _FakeMeta("a")
        broken = _FakeConn(wool.RpcError(details="broken"))
        free = _FakeConn("stream-b")
        context = _FakeContext({broken_meta: broken, _FakeMeta("b"): free})

        # Act
        stream = await PriorityLoadBalancer().dispatch(object(), context=context)

        # Assert
        assert stream == "stream-b"
        assert context.removed == [broken_meta]

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_when_pool_is_empty(self):
        """Test that an empty pool surfaces NoWorkersAvailable.

        Given:
            A context with no workers.
        When:
            A task is dispatched.
        Then:
            It should raise NoWorkersAvailable (the executor's signal to add
            a worker and re-queue).
        """
        # Arrange
        context = _FakeContext({})

        # Act & assert
        with pytest.raises(wool.NoWorkersAvailable):
            await PriorityLoadBalancer().dispatch(object(), context=context)

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_when_all_workers_reject(self):
        """Test that an all-busy pool surfaces NoWorkersAvailable.

        Given:
            Every worker rejects with TransientRpcError (all at capacity).
        When:
            A task is dispatched.
        Then:
            It should raise NoWorkersAvailable after one full pass, without
            evicting the still-healthy busy workers.
        """
        # Arrange
        a = _FakeConn(wool.TransientRpcError(details="busy"))
        b = _FakeConn(wool.TransientRpcError(details="busy"))
        context = _FakeContext({_FakeMeta("a"): a, _FakeMeta("b"): b})

        # Act & assert
        with pytest.raises(wool.NoWorkersAvailable):
            await PriorityLoadBalancer().dispatch(object(), context=context)

        # ...and busy workers stay in the pool for the next attempt.
        assert context.removed == []
        assert a.calls == 1 and b.calls == 1

    @pytest.mark.asyncio
    async def test_dispatch_should_let_unexpected_errors_propagate(self):
        """Test that a non-RpcError propagates without touching the pool.

        Given:
            The priority worker raises a plain ValueError (a caller-fault,
            not a worker-health signal).
        When:
            A task is dispatched.
        Then:
            It should propagate the ValueError unwrapped and not evict the
            worker, per wool's worker-health contract.
        """
        # Arrange
        meta = _FakeMeta("a")
        context = _FakeContext({meta: _FakeConn(ValueError("boom"))})

        # Act & assert
        with pytest.raises(ValueError, match="boom"):
            await PriorityLoadBalancer().dispatch(object(), context=context)
        assert context.removed == []

    def test_cloudpickle_roundtrip_should_succeed(self):
        """Test that the stateless balancer cloudpickle-serializes.

        Given:
            A PriorityLoadBalancer instance.
        When:
            It is cloudpickle dumped and loaded back.
        Then:
            The round-trip yields another PriorityLoadBalancer (no lock or
            index state to poison serialization).
        """
        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(PriorityLoadBalancer()))

        # Assert
        assert isinstance(restored, PriorityLoadBalancer)

    def test_conforms_to_loadbalancer_like_protocol(self):
        """Test that the balancer satisfies wool's LoadBalancerLike protocol.

        Given:
            A constructed balancer.
        When:
            It is checked against the runtime-checkable LoadBalancerLike
            protocol.
        Then:
            It should be recognized as a LoadBalancerLike so WorkerPool
            accepts it.
        """
        # Act & assert
        assert isinstance(PriorityLoadBalancer(), wool.LoadBalancerLike)
