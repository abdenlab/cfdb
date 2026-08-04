"""ECS Fargate worker entrypoint.

This module is the ``CMD`` for the worker container image. It boots a
``wool.LocalWorker`` on a known port, exposes a tiny HTTP health endpoint
the ECS health check probes, handles SIGTERM cleanly so ``ecs.stop_task``
cycles drain in-flight work, and self-terminates after a configurable
maximum lifetime so workers don't accumulate when the dispatch rate falls.

ECS owns the worker *lifecycle* — registration, IP, status, health — and
``EcsDiscovery`` polls it for all of that. But two fields of the wool
``WorkerMetadata`` the API needs are knowable only in here: the wool
protocol version this process runs, and whether it configured TLS. wool
gates worker admission on both, so a value the API invented for them is
a value that silently rejects the whole fleet (issue #90). After the
worker starts, this module therefore publishes what wool authored for it
(``LocalWorker.metadata``) onto its own ECS task as tags, which
``EcsDiscovery`` reads back. ECS supplies liveness; the worker supplies
identity.

Publishing is skipped outside ECS — ``ECS_CONTAINER_METADATA_URI_V4``
is injected only into Fargate tasks — so running this module locally is
unaffected, and the LAN path (:mod:`cfdb.workflows.worker_lan`) uses
wool's own publisher instead.

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
* ``CFDB_WORKER_PUBLISH_ATTEMPTS`` — attempts to publish metadata
  before giving up and exiting (default 5).
* ``CFDB_WORKER_PUBLISH_BACKOFF_SECONDS`` — base of the exponential
  backoff between publish attempts (default 0.5).
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections.abc import Callable
from typing import TYPE_CHECKING, Optional

import click
import wool

from cfdb.workflows import WORKER_MAX_CONCURRENT_TASKS
from cfdb.workflows.backpressure import backpressure_for
from cfdb.workflows.constants import (
    DEFAULT_WORKER_PORT,
    WORKER_TAG_SECURE,
    WORKER_TAG_TRUE,
    WORKER_TAG_VERSION,
)
from cfdb.workflows.credentials import build_worker_credentials, identity_from_env
from cfdb.workflows.grpc_options import worker_grpc_options
from cfdb.workflows.provisioner import _is_retryable_error, build_ecs_client

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

#: Env var ECS injects into every Fargate task (platform 1.4.0+). Its
#: absence is how this module tells "running on ECS" from "running on a
#: laptop", and it is the only signal needed — no profile flag.
_ECS_METADATA_URI_ENV = "ECS_CONTAINER_METADATA_URI_V4"

#: Default attempts to publish metadata before giving up. ``TagResource``
#: shares the ECS API's account-wide rate limit, and the task metadata
#: endpoint is busiest during exactly the cold-start burst that spawns
#: workers — so a burst of simultaneous starts can throttle a few;
#: retrying costs a few seconds and saves a task that would otherwise be
#: discarded. Overridable via ``CFDB_WORKER_PUBLISH_ATTEMPTS``.
DEFAULT_PUBLISH_ATTEMPTS = 5

#: Default base of the exponential backoff between publish attempts, in
#: seconds. Overridable via ``CFDB_WORKER_PUBLISH_BACKOFF_SECONDS``.
DEFAULT_PUBLISH_BACKOFF_SECONDS = 0.5

#: How long to wait on the task metadata endpoint. It is a link-local
#: HTTP server in the same task, so a slow response means something is
#: wrong rather than merely busy.
_METADATA_TIMEOUT_SECONDS = 5.0


async def _task_arn() -> Optional[str]:
    """Return this task's ARN from the ECS metadata endpoint, or None.

    None means the worker is not running on ECS, which is the normal
    case for local development. A malformed or unreachable endpoint
    while the env var *is* set raises instead — that is a broken task,
    not a laptop.
    """
    base = os.getenv(_ECS_METADATA_URI_ENV)
    if not base:
        return None

    from aiohttp import ClientSession, ClientTimeout

    timeout = ClientTimeout(total=_METADATA_TIMEOUT_SECONDS)
    async with ClientSession(timeout=timeout) as session:
        async with session.get(f"{base.rstrip('/')}/task") as response:
            response.raise_for_status()
            # The endpoint serves application/json but has historically
            # been served with a text content type; don't let aiohttp's
            # content-type check reject a valid body.
            payload = await response.json(content_type=None)

    arn = payload.get("TaskARN")
    if not arn:
        raise RuntimeError(
            "ECS task metadata endpoint returned no TaskARN; cannot "
            "publish worker metadata"
        )
    return arn


def _region_from_arn(arn: str) -> Optional[str]:
    """Return the region segment of an ECS task ARN, or None.

    ``arn:aws:ecs:us-east-2:...:task/<cluster>/<id>`` — the fourth
    colon-delimited field. Used as the region fallback when
    ``AWS_REGION`` is unset in the environment: the ARN names the
    region the task actually runs in, so it cannot drift from the
    resource being tagged, and it makes publishing independent of the
    task definition's env block (see the readiness discussion in
    ``_publish_worker_metadata``).
    """
    parts = arn.split(":")
    if len(parts) > 4 and parts[3]:
        return parts[3]
    return None


def _is_access_denied(exc: BaseException) -> bool:
    """Return True when an ECS error is an authorization failure."""
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = (response.get("Error") or {}).get("Code")
        return code in ("AccessDeniedException", "AccessDenied")
    return False


def _is_retryable_publish_error(exc: BaseException) -> bool:
    """Return True when a publish failure is worth another attempt.

    The publish path crosses two boundaries with distinct failure
    vocabularies: the link-local task metadata endpoint (aiohttp
    errors, timeouts — throttled or busy during a cold-start burst)
    and the ECS control plane (botocore errors, classified by the
    provisioner's shared :func:`_is_retryable_error`). Authorization
    failures are permanent by definition and are excluded so a missing
    ``ecs:TagResource`` grant fails in one attempt with a message
    naming the fix, rather than after the full backoff budget.
    """
    if _is_access_denied(exc):
        return False
    from aiohttp import ClientError

    if isinstance(exc, (ClientError, asyncio.TimeoutError)):
        return True
    return _is_retryable_error(exc)


async def _publish_worker_metadata(
    worker: "wool.LocalWorker",
    *,
    stop_event: Optional[asyncio.Event] = None,
    attempts: int = DEFAULT_PUBLISH_ATTEMPTS,
    backoff_seconds: float = DEFAULT_PUBLISH_BACKOFF_SECONDS,
) -> None:
    """Publish this worker's wool metadata onto its own ECS task tags.

    ``EcsDiscovery`` reads these back to build the ``WorkerMetadata`` it
    advertises. Only the fields it cannot otherwise know are published:
    the wool protocol version and the TLS flag, both of which wool's
    admission gate tests (see the module docstring).

    Returns without doing anything when not running on ECS, and returns
    early (without publishing) when ``stop_event`` is set mid-retry —
    the worker is exiting anyway, so becoming discoverable would only
    invite a dispatch it cannot honor.

    Every step that can fail transiently — the task-metadata fetch, the
    client construction, and the ``TagResource`` call — sits inside the
    retry loop, because all three share the same burst profile: a fleet
    cold start is exactly when the metadata endpoint and the ECS API
    are busiest. The region falls back to the task ARN's own region
    segment when ``AWS_REGION`` is unset, so publishing works even if
    the task definition's env block loses the variable.

    Raises when the metadata cannot be published within ``attempts``
    tries, or immediately on a permanent error (an ``AccessDenied``
    from a missing ``ecs:TagResource`` grant). That is deliberate: a
    worker whose metadata never lands is invisible to the API forever,
    so it would hold a Fargate slot and count against
    ``ECS_MAX_WORKERS`` while being incapable of receiving work.
    Exiting frees both immediately and the provisioner launches a
    replacement on the next dispatch; standalone ``RunTask`` tasks are
    not restarted, so there is no crash-loop.
    """
    if not os.getenv(_ECS_METADATA_URI_ENV):
        logger.debug(
            "%s unset — not running on ECS, skipping metadata publish",
            _ECS_METADATA_URI_ENV,
        )
        return

    metadata = worker.metadata
    if metadata is None:  # pragma: no cover — start() precedes this call
        raise RuntimeError("Worker has no metadata; publish called before start")

    tags = [
        {"key": WORKER_TAG_VERSION, "value": metadata.version},
        {
            "key": WORKER_TAG_SECURE,
            "value": WORKER_TAG_TRUE if metadata.secure else "false",
        },
    ]

    arn: Optional[str] = None
    for attempt in range(1, attempts + 1):
        try:
            arn = await _task_arn()
            assert arn is not None  # env var is set, so _task_arn cannot skip
            client = build_ecs_client(
                endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
                region_name=os.getenv("AWS_REGION") or _region_from_arn(arn),
            )
            # boto3 is synchronous; keep it off the event loop so the
            # health server stays responsive while we retry.
            await asyncio.to_thread(client.tag_resource, resourceArn=arn, tags=tags)
        except Exception as exc:
            if _is_access_denied(exc):
                logger.error(
                    "ecs:TagResource denied while publishing worker metadata "
                    "to %s — the worker task role is missing the grant the "
                    "workers stack (cloudformation/workers.yml) provides. "
                    "Deploy the workers stack before shipping this image; "
                    "this worker can never be discovered, exiting: %s",
                    arn,
                    exc,
                )
                raise
            if attempt == attempts or not _is_retryable_publish_error(exc):
                logger.error(
                    "Failed to publish worker metadata to %s after %d attempt(s); "
                    "this worker can never be discovered, exiting: %s",
                    arn,
                    attempt,
                    exc,
                )
                raise
            delay = backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                "Publishing worker metadata to %s failed (attempt %d/%d), "
                "retrying in %.1fs: %s",
                arn,
                attempt,
                attempts,
                delay,
                exc,
            )
            if stop_event is not None:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                if stop_event.is_set():
                    logger.info(
                        "Stop requested during metadata publish — abandoning "
                        "publish and shutting down"
                    )
                    return
            else:
                await asyncio.sleep(delay)
        else:
            logger.info(
                "Published worker metadata to %s (version %s, secure %s)",
                arn,
                metadata.version,
                metadata.secure,
            )
            return


async def serve(
    *,
    worker_port: int = DEFAULT_WORKER_PORT,
    health_port: int = DEFAULT_HEALTH_PORT,
    max_lifetime_seconds: float = DEFAULT_MAX_LIFETIME_SECONDS,
    drain_grace_seconds: float = DEFAULT_DRAIN_GRACE_SECONDS,
    tls_ca: Optional[str] = None,
    tls_cert: Optional[str] = None,
    tls_key: Optional[str] = None,
    publish_attempts: int = DEFAULT_PUBLISH_ATTEMPTS,
    publish_backoff_seconds: float = DEFAULT_PUBLISH_BACKOFF_SECONDS,
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

    The identity comes from the environment rather than a flag because
    it is inert on the serving side — it applies only to the one channel
    wool opens back to this worker's subprocess to drain it.

    ``publish_attempts`` and ``publish_backoff_seconds`` bound the
    metadata-publish retry loop (see ``_publish_worker_metadata``);
    they are exposed here — like every other operational knob in this
    module — so tests and operators configure the budget through the
    public surface rather than by patching module state.
    """
    credentials = build_worker_credentials(
        tls_ca, tls_cert, tls_key, identity=identity_from_env()
    )
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
        # Relaxed keepalive/ping enforcement so a long, quiet dispatch
        # stream doesn't trip the worker's server into GOAWAY
        # too_many_pings; see cfdb.workflows.grpc_options.
        options=worker_grpc_options(),
    )
    await worker.start()
    try:
        # Publish before entering the serve loop. The worker is already
        # accepting gRPC by now, but until its tags land EcsDiscovery
        # deliberately will not advertise it, so there is no window in
        # which the API dispatches to a worker whose metadata is unknown.
        # stop_event lets a SIGTERM landing mid-retry short-circuit the
        # backoff and proceed straight to drain.
        await _publish_worker_metadata(
            worker,
            stop_event=stop_event,
            attempts=publish_attempts,
            backoff_seconds=publish_backoff_seconds,
        )
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
@click.option(
    "--publish-attempts",
    type=click.IntRange(min=1),
    envvar="CFDB_WORKER_PUBLISH_ATTEMPTS",
    default=DEFAULT_PUBLISH_ATTEMPTS,
    show_default=True,
    help="Attempts to publish worker metadata before exiting.",
)
@click.option(
    "--publish-backoff-seconds",
    type=click.FloatRange(min=0),
    envvar="CFDB_WORKER_PUBLISH_BACKOFF_SECONDS",
    default=DEFAULT_PUBLISH_BACKOFF_SECONDS,
    show_default=True,
    help="Base of the exponential backoff between publish attempts.",
)
def main(
    worker_port: int,
    health_port: int,
    max_lifetime_seconds: float,
    drain_grace_seconds: float,
    tls_ca: Optional[str],
    tls_cert: Optional[str],
    tls_key: Optional[str],
    publish_attempts: int,
    publish_backoff_seconds: float,
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
                publish_attempts=publish_attempts,
                publish_backoff_seconds=publish_backoff_seconds,
            )
        )
    )


if __name__ == "__main__":  # pragma: no cover
    main()
