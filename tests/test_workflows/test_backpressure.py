"""Tests for the per-worker backpressure hook."""

from __future__ import annotations

import cloudpickle
import pytest
import wool

from cfdb.workflows.backpressure import TaskCountBackpressure
from cfdb.workflows.backpressure import backpressure_for


def _ctx(active_task_count: int) -> wool.BackpressureContext:
    """Build a BackpressureContext with a sentinel task for hook tests."""
    return wool.BackpressureContext(active_task_count=active_task_count, task=object())


class TestTaskCountBackpressure:
    def test___init___should_raise_when_threshold_below_one(self):
        """Test that a non-positive threshold is rejected at construction.

        Given:
            A threshold of 0, the "disable backpressure" sentinel that the
            wiring handles by passing None instead of this hook.
        When:
            TaskCountBackpressure is constructed with it.
        Then:
            It should raise ValueError rather than build a hook that never
            rejects.
        """
        # Arrange, act, & assert
        with pytest.raises(ValueError, match="threshold must be >= 1"):
            TaskCountBackpressure(0)

    def test___call___should_accept_when_below_threshold(self):
        """Test that the hook accepts a task below the threshold.

        Given:
            A hook with threshold 1 and a worker with no tasks in flight.
        When:
            The hook is invoked with that context.
        Then:
            It should return False (accept).
        """
        # Arrange
        hook = TaskCountBackpressure(1)

        # Act
        decision = hook(_ctx(active_task_count=0))

        # Assert
        assert decision is False

    def test___call___should_reject_when_at_threshold(self):
        """Test that the hook rejects a task at the threshold.

        Given:
            A hook with threshold 1 and a worker already running one task.
        When:
            The hook is invoked with that context.
        Then:
            It should return True (reject), which wool surfaces as
            RESOURCE_EXHAUSTED.
        """
        # Arrange
        hook = TaskCountBackpressure(1)

        # Act
        decision = hook(_ctx(active_task_count=1))

        # Assert
        assert decision is True

    def test___call___should_reject_when_above_threshold(self):
        """Test that the hook rejects once in-flight exceeds the threshold.

        Given:
            A hook with threshold 2 and a worker running three tasks.
        When:
            The hook is invoked with that context.
        Then:
            It should return True (reject).
        """
        # Arrange
        hook = TaskCountBackpressure(2)

        # Act
        decision = hook(_ctx(active_task_count=3))

        # Assert
        assert decision is True

    def test_cloudpickle_roundtrip_should_preserve_threshold(self):
        """Test that the hook survives the worker-serialization boundary.

        Given:
            A hook that wool will serialize into a LocalWorker subprocess.
        When:
            It is cloudpickle dumped and loaded back.
        Then:
            The restored hook preserves its threshold and still rejects at
            it, proving it is safe to ship across the worker boundary.
        """
        # Arrange
        hook = TaskCountBackpressure(2)

        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(hook))

        # Assert
        assert isinstance(restored, TaskCountBackpressure)
        assert restored.threshold == 2
        assert restored(_ctx(active_task_count=2)) is True
        assert restored(_ctx(active_task_count=1)) is False

    def test_conforms_to_backpressure_like_protocol(self):
        """Test that the hook satisfies wool's BackpressureLike protocol.

        Given:
            A constructed hook.
        When:
            It is checked against the runtime-checkable BackpressureLike
            protocol.
        Then:
            It should be recognized as a BackpressureLike, so wool accepts
            it where a backpressure hook is expected.
        """
        # Arrange
        hook = TaskCountBackpressure(1)

        # Act & assert — structural conformance, plus the bool-return
        # contract wool actually depends on (BackpressureLike is satisfied
        # by any callable, so the return-type check gives the test teeth).
        assert isinstance(hook, wool.BackpressureLike)
        assert isinstance(hook(_ctx(active_task_count=0)), bool)


class TestBackpressureFor:
    def test_backpressure_for_should_return_none_when_threshold_is_zero(self):
        """Test that a threshold of 0 disables backpressure.

        Given:
            A threshold of 0 (the disable sentinel).
        When:
            backpressure_for is called.
        Then:
            It should return None so the wiring passes backpressure=None to
            wool (unbounded admission).
        """
        # Act & assert
        assert backpressure_for(0) is None

    def test_backpressure_for_should_build_hook_when_threshold_positive(self):
        """Test that a positive threshold yields a configured hook.

        Given:
            A threshold of 2.
        When:
            backpressure_for is called.
        Then:
            It should return a TaskCountBackpressure carrying that threshold.
        """
        # Act
        hook = backpressure_for(2)

        # Assert
        assert isinstance(hook, TaskCountBackpressure)
        assert hook.threshold == 2

    def test_backpressure_for_should_raise_on_negative_threshold(self):
        """Test that a negative threshold is rejected, not silently disabled.

        Given:
            A negative threshold.
        When:
            backpressure_for is called.
        Then:
            It should raise ValueError, so a negative value cannot
            accidentally disable backpressure (only 0 disables it).
        """
        # Act & assert
        with pytest.raises(ValueError, match=">= 0"):
            backpressure_for(-1)
