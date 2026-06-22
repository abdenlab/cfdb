"""ECS Fargate worker entrypoint.

This module is the ``CMD`` for the worker container image. It boots a
``wool.LocalWorker`` on a known port, exposes a tiny HTTP health endpoint
the ECS health check probes, handles SIGTERM cleanly so ``ecs.stop_task``
cycles drain in-flight work, and self-terminates after a configurable
maximum lifetime so workers don't accumulate when the dispatch rate falls.

No discovery registration code lives here. ``EcsDiscovery`` polls ECS's
own task state to surface running workers; the worker only needs to
bind its gRPC port and respond ``200 OK`` to the health probe.

Environment variables (single source of truth; CLI flags mirror them):

* ``CFDB_WORKER_GRPC_PORT`` — gRPC port wool binds (default 50051).
* ``CFDB_WORKER_HEALTH_PORT`` — HTTP ``/health`` port the ECS
  ``healthCheck`` probes (default 8080).
* ``CFDB_WORKER_MAX_LIFETIME_SECONDS`` — hard ceiling on worker
  uptime; 0 disables. One hour above
  :data:`cfdb.workflows.WORKFLOW_DURATION_CAP_S` so a worker started
  shortly before a long sort can still outlive the job.
* ``CFDB_WORKER_DRAIN_GRACE_SECONDS`` — how long ``/health`` returns
  503 after SIGTERM before tearing down the gRPC port. A second
  SIGTERM short-circuits.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Callable
from typing import TYPE_CHECKING, Optional

import click
import wool

from cfdb.workflows import WORKER_MAX_CONCURRENT_TASKS
from cfdb.workflows.backpressure import backpressure_for
from cfdb.workflows.constants import DEFAULT_WORKER_PORT
from cfdb.workflows.credentials import build_worker_credentials

if TYPE_CHECKING:
    from aiohttp import web

logger = logging.getLogger(__name__)

__all__ = ["main", "serve"]

#: Default health probe HTTP port — distinct from the gRPC port so
#: ``healthCheck`` can ``curl`` it without speaking gRPC.
DEFAULT_HEALTH_PORT = 8080

#: Default maximum wall-clock lifetime of a worker process. Wool exposes
#: no per-job activity hook today, so the worker can't tell idle from
#: busy; this is a hard ceiling — ECS replaces the task after this long.
#: Sized one hour above :data:`cfdb.workflows.WORKFLOW_DURATION_CAP_S`
#: (default 4 h) so a worker started shortly before a long sort can
#: still outlive the job. Note: max-lifetime expiry exits without the
#: drain-grace window the SIGTERM path provides — operators that want
#: a cleaner handoff should rely on ECS rolling tasks via service
#: updates rather than waiting for max-lifetime to fire.
DEFAULT_MAX_LIFETIME_SECONDS = 5 * 60 * 60

#: How long to keep returning ``503`` on ``/health`` after SIGTERM,
#: giving ECS a chance to observe ``unhealthy`` and drain at the load
#: balancer before we tear the gRPC port down. ECS health checks
#: default to ~30 s interval × 3 unhealthy retries = ~90 s worst case
#: to mark the task unhealthy; we hold for 120 s by default to leave
#: real margin even when the task definition uses ECS-default health
#: check cadence. Operators tightening ``healthCheck.interval`` to
#: ~10-15 s can lower this further via ``CFDB_WORKER_DRAIN_GRACE_SECONDS``.
DEFAULT_DRAIN_GRACE_SECONDS = 120.0

#: Cadence of the main loop's wakeup when waiting on ``stop_event``.
#: One second balances precision (max-lifetime checks fire within ~1 s
#: of the target) against CPU churn over a multi-hour worker lifetime
#: (~3600 wakeups/h vs. ~3.6 M at 1 ms). Sub-second granularity is
#: unnecessary because the upstream coarse-grained signals
#: (SIGTERM-on-stop_task, ~hours-scale max-lifetime) don't need it.
_STOP_POLL_INTERVAL_SECONDS = 1.0


async def serve(
    *,
    worker_port: int = DEFAULT_WORKER_PORT,
    health_port: int = DEFAULT_HEALTH_PORT,
    max_lifetime_seconds: float = DEFAULT_MAX_LIFETIME_SECONDS,
    drain_grace_seconds: float = DEFAULT_DRAIN_GRACE_SECONDS,
    tls_ca: Optional[str] = None,
    tls_cert: Optional[str] = None,
    tls_key: Optional[str] = None,
) -> int:
    """Run the worker until SIGTERM or maximum lifetime elapses.

    Returns ``0`` on clean shutdown (SIGTERM, SIGINT, or max-lifetime).
    Bind failures and other early-startup errors raise out — ``main``
    propagates them and the process exits with a Python traceback,
    which surfaces the cause in container logs more clearly than a
    silent non-zero status.

    A second SIGTERM during drain short-circuits the grace window
    (operator impatience or ECS escalating before SIGKILL); the worker
    stops immediately.

    When ``tls_ca`` / ``tls_cert`` / ``tls_key`` are all supplied the
    worker requires mutual TLS (a CA-signed client certificate);
    unset leaves the gRPC channel plaintext. Partial cert config raises
    before the worker binds (see :func:`build_worker_credentials`).
    """
    credentials = build_worker_credentials(tls_ca, tls_cert, tls_key)
    stop_event = asyncio.Event()
    force_stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    started_at = loop.time()

    def _signal_handler() -> None:
        if stop_event.is_set():
            logger.info("Second termination signal — exiting drain immediately")
            force_stop_event.set()
        else:
            logger.info("Received termination signal — draining worker")
            stop_event.set()

    def _signal_handler_threaded(*_: object) -> None:
        # Fallback for platforms (notably Windows in CI) where
        # ``loop.add_signal_handler`` raises ``NotImplementedError``.
        # Defined once so the closure isn't rebuilt per signal.
        loop.call_soon_threadsafe(_signal_handler)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            signal.signal(sig, _signal_handler_threaded)

    health_runner = await _start_health_server(
        health_port, lambda: stop_event.is_set()
    )

    # Per-worker backpressure: reject a dispatch once this worker already
    # has CFDB_WORKER_MAX_CONCURRENT_TASKS routines in flight (default 1),
    # so its subprocess pipelines serialize instead of oversubscribing the
    # 1-vCPU task. ``None`` (threshold 0) keeps the unbounded prior behavior.
    backpressure = backpressure_for(WORKER_MAX_CONCURRENT_TASKS)

    worker = wool.LocalWorker(
        host="0.0.0.0",
        port=worker_port,
        credentials=credentials,
        backpressure=backpressure,
    )
    await worker.start()
    try:
        logger.info(
            "Wool worker listening on port %d (health on %d, mTLS %s, "
            "max concurrent tasks %s)",
            worker_port,
            health_port,
            "enabled" if credentials is not None else "disabled",
            WORKER_MAX_CONCURRENT_TASKS if backpressure is not None else "unbounded",
        )
        while True:
            # Check the self-termination path first. Max-lifetime is a
            # local hard ceiling: the gap between ``stop_event.set()``
            # and ``worker.stop()`` is microseconds — no health probe
            # will actually fire during it, so this is a defense-in-
            # depth flip rather than a real drain window. Operators
            # that need a true drain handoff on max-lifetime should
            # roll tasks via ECS service updates instead of relying
            # on the self-timeout. Flipping /health to 503 still costs
            # nothing and keeps the in-process ordering consistent
            # with the signal path's drain semantics.
            if (
                max_lifetime_seconds > 0
                and (loop.time() - started_at) >= max_lifetime_seconds
            ):
                logger.info(
                    "Max lifetime (%.0fs) reached — exiting",
                    max_lifetime_seconds,
                )
                stop_event.set()
                break
            if stop_event.is_set():
                # Signal-initiated shutdown. Hold the gRPC port open
                # while ECS observes 503 on /health and stops routing
                # new dispatches. A second signal flips
                # force_stop_event and we exit the wait early.
                if drain_grace_seconds > 0:
                    logger.info(
                        "Draining for up to %.0fs before exiting",
                        drain_grace_seconds,
                    )
                    try:
                        await asyncio.wait_for(
                            force_stop_event.wait(),
                            timeout=drain_grace_seconds,
                        )
                    except asyncio.TimeoutError:
                        pass
                break
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=_STOP_POLL_INTERVAL_SECONDS
                )
            except asyncio.TimeoutError:
                continue
        return 0
    finally:
        try:
            await worker.stop()
        except Exception:
            logger.exception("worker.stop() failed during shutdown")
        await _shutdown_health_server(health_runner)


async def _start_health_server(
    port: int, draining: Callable[[], bool]
) -> "web.AppRunner":
    """Start a tiny HTTP server returning ``200 OK`` on ``/health``.

    The container's ECS ``healthCheck`` runs ``curl`` against this
    endpoint, so the response shape doesn't matter — only the status
    code does. While ``draining`` returns True (i.e. we're shutting
    down), the endpoint returns ``503`` so ECS can mark the task
    unhealthy and the load balancer / discovery can drain it before
    ``ecs.stop_task`` actually kills the gRPC port.

    Cleans up the partial runner on bind failure so the caller's
    finally doesn't have a half-initialized object to deal with.
    """
    from aiohttp import web

    async def _health(_: web.Request) -> web.Response:
        if draining():
            return web.Response(status=503, text="draining")
        return web.Response(status=200, text="ok")

    app = web.Application()
    app.router.add_get("/health", _health)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, host="0.0.0.0", port=port)
        await site.start()
    except Exception:
        await runner.cleanup()
        raise
    return runner


async def _shutdown_health_server(runner: Optional["web.AppRunner"]) -> None:
    """Tear down the aiohttp ``AppRunner`` started by ``_start_health_server``.

    Tolerates ``runner is None`` so the caller's ``finally`` can run
    even when the health server failed to start in the first place.
    Cleanup exceptions are logged but swallowed: the worker is already
    on its way out, and surfacing a teardown error would mask the
    upstream cause (whatever triggered the shutdown).
    """
    if runner is None:
        return
    try:
        await runner.cleanup()
    except Exception:
        logger.exception("health server cleanup failed")


@click.command()
@click.option(
    "--worker-port",
    type=click.IntRange(1, 65535),
    envvar="CFDB_WORKER_GRPC_PORT",
    default=DEFAULT_WORKER_PORT,
    show_default=True,
    help="gRPC port the wool worker binds.",
)
@click.option(
    "--health-port",
    type=click.IntRange(1, 65535),
    envvar="CFDB_WORKER_HEALTH_PORT",
    default=DEFAULT_HEALTH_PORT,
    show_default=True,
    help="HTTP port the ECS health-check endpoint binds.",
)
@click.option(
    "--max-lifetime-seconds",
    type=click.FloatRange(min=0),
    envvar="CFDB_WORKER_MAX_LIFETIME_SECONDS",
    default=DEFAULT_MAX_LIFETIME_SECONDS,
    show_default=True,
    help="Hard ceiling on worker uptime in seconds; 0 disables.",
)
@click.option(
    "--drain-grace-seconds",
    type=click.FloatRange(min=0),
    envvar="CFDB_WORKER_DRAIN_GRACE_SECONDS",
    default=DEFAULT_DRAIN_GRACE_SECONDS,
    show_default=True,
    help=(
        "Seconds to keep returning 503 on /health after SIGTERM before "
        "tearing down the gRPC port. A second SIGTERM short-circuits."
    ),
)
@click.option(
    "--tls-ca",
    envvar="CFDB_WORKER_TLS_CA",
    default=None,
    help=(
        "Path to the shared CA certificate. Set all three --tls-* "
        "options to require mutual TLS; leave them unset for plaintext."
    ),
)
@click.option(
    "--tls-cert",
    envvar="CFDB_WORKER_TLS_CERT",
    default=None,
    help="Path to this worker's PEM certificate (CA-signed).",
)
@click.option(
    "--tls-key",
    envvar="CFDB_WORKER_TLS_KEY",
    default=None,
    help="Path to this worker's PEM private key.",
)
def main(
    worker_port: int,
    health_port: int,
    max_lifetime_seconds: float,
    drain_grace_seconds: float,
    tls_ca: Optional[str],
    tls_cert: Optional[str],
    tls_key: Optional[str],
) -> None:
    """ECS Fargate worker entrypoint — invoked by the container CMD.

    Boots a wool gRPC worker, exposes /health for ECS to probe, and
    self-terminates after the max-lifetime ceiling. SIGTERM begins a
    drain grace window during which /health returns 503 so the load
    balancer can drop the worker before the gRPC port closes.
    """
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(
        asyncio.run(
            serve(
                worker_port=worker_port,
                health_port=health_port,
                max_lifetime_seconds=max_lifetime_seconds,
                drain_grace_seconds=drain_grace_seconds,
                tls_ca=tls_ca,
                tls_cert=tls_cert,
                tls_key=tls_key,
            )
        )
    )


if __name__ == "__main__":  # pragma: no cover
    main()
