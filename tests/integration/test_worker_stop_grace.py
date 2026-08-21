"""Integration test for the stop-grace contract the idle exit relies on.

``worker_main.serve`` stops the worker with a grace on both of its
self-termination paths, and that grace is the whole reason those exits
are safe: an idle reading is a snapshot, so it cannot rule out a
dispatch accepted between the final poll and the teardown, and a
max-lifetime expiry can land mid-job outright. In either case the API
has already marked that task running, where a mid-stream cancel is
finalized as a terminal job failure rather than re-queued.

Every unit test on that path mocks ``wool.LocalWorker``, so the suite
asserts only that cfdb *passes* a grace — nothing verifies wool honors
it. A wool change that made ``stop`` cancel regardless would leave the
whole suite green while silently restoring the defect the grace exists
to prevent. These tests start a real worker, dispatch a real task, and
stop it mid-flight.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import socket

import pytest
import wool

from tests.integration.routines import sleep_then_return


pytestmark = pytest.mark.integration

#: How long the dispatched task occupies the worker. Long enough that
#: the stop below reliably lands while it is still running, short
#: enough not to drag the suite.
TASK_SECONDS = 4.0

#: Grace allowed for the drain — comfortably longer than the task, so a
#: completed task means the drain waited rather than that the drain
#: happened to outlast a tiny task.
DRAIN_GRACE_SECONDS = 30.0


def _free_port() -> int:
    """Reserve an ephemeral port and return it.

    The worker has to answer at an address the test can dial, so the
    port cannot be left to ``0``; binding and releasing avoids the
    collisions a hard-coded port would cause on a busy machine.
    """
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@contextlib.asynccontextmanager
async def _worker_on_known_port():
    """Yield ``(connection, port)`` for a pool-managed worker.

    Dispatch needs a pool session, but the stop has to target one known
    worker, so the pool is given a factory bound to a reserved port and
    the test dials that port directly.
    """
    port = _free_port()
    factory = functools.partial(wool.LocalWorker, host="127.0.0.1", port=port)
    async with wool.WorkerPool(
        spawn=1, worker=factory, discovery=wool.LocalDiscovery(), lazy=False
    ):
        connection = wool.WorkerConnection(f"127.0.0.1:{port}")
        try:
            yield connection, port
        finally:
            await connection.close()


class TestWorkerStopGrace:
    @pytest.mark.asyncio
    async def test_stop_should_drain_an_in_flight_task_when_given_a_grace(self):
        """Test that a graced stop lets a running task finish.

        Given:
            A real worker running a dispatched task that outlasts the
            moment the stop is issued.
        When:
            The worker is stopped with a grace longer than the task's
            remaining runtime.
        Then:
            It should let the task run to completion and return its
            result, pinning the contract that makes cfdb's idle and
            max-lifetime exits safe rather than merely narrow.
        """
        async with _worker_on_known_port() as (connection, _):
            # Arrange — put a task in flight, then let it get underway
            task = asyncio.create_task(
                sleep_then_return(TASK_SECONDS, "drained")
            )
            await asyncio.sleep(TASK_SECONDS / 4)
            assert not task.done()

            # Act — stop the worker out from under the running task
            await connection.stop(grace=DRAIN_GRACE_SECONDS)

            # Assert
            assert await task == "drained"

    @pytest.mark.asyncio
    async def test_stop_should_cancel_an_in_flight_task_when_given_no_grace(self):
        """Test that a graceless stop kills a running task.

        Given:
            A real worker running a dispatched task that outlasts the
            moment the stop is issued.
        When:
            The worker is stopped without a grace — wool's default.
        Then:
            It should cancel the task rather than return its value, so
            the drain asserted above is demonstrably the grace's doing
            and not an artifact of the task finishing on its own.
        """
        async with _worker_on_known_port() as (connection, _):
            # Arrange — put a task in flight, then let it get underway
            task = asyncio.create_task(
                sleep_then_return(TASK_SECONDS, "drained")
            )
            await asyncio.sleep(TASK_SECONDS / 4)
            assert not task.done()

            # Act — stop with wool's default: no grace
            await connection.stop()

            # Assert — a hung task raises TimeoutError here instead, so
            # the cancellation is pinned rather than merely "not done"
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=TASK_SECONDS)
