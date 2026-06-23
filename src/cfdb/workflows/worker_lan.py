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
running, ``/data`` and ``/index`` requests for processable formats do not
hang or surface ``NoWorkersAvailable`` to the client: the job is claimed
and queued PENDING (the request returns ``202``), and the API's durable
retry scheduler re-attempts dispatch every ``CFDB_WORKFLOW_RETRY_INTERVAL_S``
until a worker appears or the ``CFDB_WORKFLOW_DISPATCH_DEADLINE_S`` deadline
elapses (then the job is failed ``capacity:``).

Environment variables (CLI flags mirror them):

* ``WORKFLOW_POOL_NAMESPACE`` — LAN discovery namespace the pool
  publishes under (default ``cfdb-workers``). MUST match the API's value.
* ``WORKFLOW_WORKER_COUNT`` — number of workers to spawn and publish
  (default ``2``). The API admits every worker discovery surfaces (there is
  no fixed lease count), so size this to the local concurrency you want.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import signal
from typing import Optional

import click
import wool
from wool.runtime.discovery.lan import LanDiscovery

from cfdb.workflows import WORKER_MAX_CONCURRENT_TASKS
from cfdb.workflows.backpressure import backpressure_for
from cfdb.workflows.credentials import build_worker_credentials

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


async def serve(
    *,
    namespace: str,
    workers: int,
    tls_ca: Optional[str] = None,
    tls_cert: Optional[str] = None,
    tls_key: Optional[str] = None,
) -> int:
    """Spawn ``workers`` wool workers and publish them under ``namespace``.

    Blocks until SIGTERM/SIGINT, then drains the pool via the
    ``WorkerPool`` async-context exit and returns ``0``. Bind/spawn
    failures raise out so the cause surfaces in the process logs rather
    than as a silent non-zero exit.

    When ``tls_ca`` / ``tls_cert`` / ``tls_key`` are all supplied the
    pool's workers require mutual TLS; unset leaves the gRPC channel
    plaintext. The API leasing these workers must hold a certificate
    signed by the same CA. Partial cert config raises before the pool
    starts (see :func:`build_worker_credentials`).
    """
    credentials = build_worker_credentials(tls_ca, tls_cert, tls_key)
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

    # Bind per-worker backpressure onto the spawn factory so each spawned
    # LocalWorker serializes its routines (mirrors the ECS worker_main
    # wiring). ``functools.partial(..., backpressure=hook)`` keeps wool's
    # ``declares_host`` True, so the pool still prescribes the bind host.
    backpressure = backpressure_for(WORKER_MAX_CONCURRENT_TASKS)
    worker_factory = (
        functools.partial(wool.LocalWorker, backpressure=backpressure)
        if backpressure is not None
        else wool.LocalWorker
    )

    pool = wool.WorkerPool(
        spawn=workers,
        worker=worker_factory,
        discovery=LanDiscovery(namespace),
        credentials=credentials,
    )
    async with pool:
        logger.info(
            "Published %d wool worker(s) under LAN namespace %r (mTLS %s, "
            "max concurrent tasks %s) — Ctrl-C to drain and exit",
            workers,
            namespace,
            "enabled" if credentials is not None else "disabled",
            WORKER_MAX_CONCURRENT_TASKS if backpressure is not None else "unbounded",
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
@click.option(
    "--tls-ca",
    envvar="CFDB_WORKER_TLS_CA",
    default=None,
    help=(
        "Path to the shared CA certificate. Set all three --tls-* "
        "options to require mutual TLS; leave them unset for plaintext. "
        "The API must use a cert signed by the same CA."
    ),
)
@click.option(
    "--tls-cert",
    envvar="CFDB_WORKER_TLS_CERT",
    default=None,
    help="Path to the workers' PEM certificate (CA-signed).",
)
@click.option(
    "--tls-key",
    envvar="CFDB_WORKER_TLS_KEY",
    default=None,
    help="Path to the workers' PEM private key.",
)
def main(
    namespace: str,
    workers: int,
    tls_ca: Optional[str],
    tls_cert: Optional[str],
    tls_key: Optional[str],
) -> None:
    """Local-dev worker pool — publishes workers over LAN discovery.

    Run this in a separate process before starting the API so the API's
    ``LanDiscovery`` subscriber can lease the published workers. Mirror
    the API's ``WORKFLOW_POOL_NAMESPACE``.
    """
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(
        asyncio.run(
            serve(
                namespace=namespace,
                workers=workers,
                tls_ca=tls_ca,
                tls_cert=tls_cert,
                tls_key=tls_key,
            )
        )
    )


if __name__ == "__main__":  # pragma: no cover
    main()
