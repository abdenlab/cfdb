"""Local-dev worker pool entrypoint (LAN discovery).

This is the single-host development counterpart to :mod:`worker_main`
(the ECS entrypoint). The two differ in how workers are *discovered*:

* :mod:`worker_main` boots a bare ``wool.LocalWorker`` and relies on
  ``EcsDiscovery`` polling the ECS control plane — the worker never
  advertises itself.
* This module spawns a ``wool.WorkerPool`` wired to ``LanDiscovery`` so
  the pool publishes its workers over zeroconf/mDNS under a shared
  namespace. A co-located API process running the ``local`` or
  ``s3-cached`` profile leases them via the same namespace.

Run it in a separate process *before* starting the API, with the same
namespace the API uses (``WORKFLOW_POOL_NAMESPACE``)::

    python -m cfdb.workflows.worker_lan --namespace cfdb-workers --workers 2

SIGINT (Ctrl-C) or SIGTERM drains the pool and exits. With no pool
running, ``/data`` and ``/index`` requests for processable formats hang
on the dispatch retry budget before failing with ``NoWorkersAvailable``.

Environment variables (CLI flags mirror them):

* ``WORKFLOW_POOL_NAMESPACE`` — LAN discovery namespace the pool
  publishes under (default ``cfdb-workers``). MUST match the API's value.
* ``WORKFLOW_WORKER_COUNT`` — number of workers to spawn and publish
  (default ``2``). Size it at least as high as the API's lease count or
  the API blocks waiting for workers.
"""

from __future__ import annotations

import asyncio
import logging
import signal

import click
import wool
from wool.runtime.discovery.lan import LanDiscovery

logger = logging.getLogger(__name__)

__all__ = ["main", "serve"]

#: Default LAN discovery namespace. Matches the API's
#: ``WORKFLOW_POOL_NAMESPACE`` default so a bare ``python -m
#: cfdb.workflows.worker_lan`` pairs with a bare API out of the box.
DEFAULT_NAMESPACE = "cfdb-workers"

#: Default number of workers to spawn and publish. Matches the API's
#: ``WORKFLOW_WORKER_COUNT`` default so the publisher supplies at least
#: as many workers as the API leases.
DEFAULT_WORKER_COUNT = 2


async def serve(*, namespace: str, workers: int) -> int:
    """Spawn ``workers`` wool workers and publish them under ``namespace``.

    Blocks until SIGTERM/SIGINT, then drains the pool via the
    ``WorkerPool`` async-context exit and returns ``0``. Bind/spawn
    failures raise out so the cause surfaces in the process logs rather
    than as a silent non-zero exit.
    """
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _signal_handler() -> None:
        logger.info("Received termination signal — draining worker pool")
        stop_event.set()

    def _signal_handler_threaded(*_: object) -> None:
        # Fallback for platforms where ``add_signal_handler`` raises
        # ``NotImplementedError`` (notably Windows in CI).
        loop.call_soon_threadsafe(_signal_handler)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            signal.signal(sig, _signal_handler_threaded)

    pool = wool.WorkerPool(
        spawn=workers, discovery=LanDiscovery(namespace)
    )
    async with pool:
        logger.info(
            "Published %d wool worker(s) under LAN namespace %r — "
            "Ctrl-C to drain and exit",
            workers,
            namespace,
        )
        await stop_event.wait()
    return 0


@click.command()
@click.option(
    "--namespace",
    envvar="WORKFLOW_POOL_NAMESPACE",
    default=DEFAULT_NAMESPACE,
    show_default=True,
    help="LAN discovery namespace to publish under; must match the API.",
)
@click.option(
    "--workers",
    type=click.IntRange(min=1),
    envvar="WORKFLOW_WORKER_COUNT",
    default=DEFAULT_WORKER_COUNT,
    show_default=True,
    help="Number of wool workers to spawn and publish.",
)
def main(namespace: str, workers: int) -> None:
    """Local-dev worker pool — publishes workers over LAN discovery.

    Run this in a separate process before starting the API so the API's
    ``LanDiscovery`` subscriber can lease the published workers. Mirror
    the API's ``WORKFLOW_POOL_NAMESPACE``.
    """
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(
        asyncio.run(serve(namespace=namespace, workers=workers))
    )


if __name__ == "__main__":  # pragma: no cover
    main()
