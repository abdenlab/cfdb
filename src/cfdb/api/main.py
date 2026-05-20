import contextvars
import logging
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path

import wool
from wool.runtime.discovery.lan import LanDiscovery
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from motor.motor_asyncio import AsyncIOMotorClient
from strawberry.fastapi import GraphQLRouter

from cfdb import api
from cfdb.api.gql.schema import schema
from cfdb.api.routers.data import router as data_router
from cfdb.api.routers.index import router as index_router
from cfdb.api.routers.jobs import router as jobs_router
from cfdb.api.routers.sync import router as sync_router
from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.executor import WoolExecutor
from cfdb.workflows.models import ACTIVE_STATUSES
from cfdb.workflows.processors.bam import BamIndexProcessor
from cfdb.workflows.processors.registry import default_registry
from cfdb.workflows.processors.tabix import TabixIntervalProcessor

logging.basicConfig(level=logging.INFO)

#: Upper bound on the lifespan shutdown drain for in-flight workflow
#: tasks. Tasks exceeding this are left for stale-reclamation on the
#: next service start — they were already bounded by the per-job
#: runtime cap and the stale threshold in ``workflows.lock``.
SHUTDOWN_DRAIN_TIMEOUT_SECONDS = 10.0


def redact_url(url: str) -> str:
    """Redact password from a MongoDB connection string for safe logging."""
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", url)


async def _assert_jobs_indexes(db, log: logging.Logger) -> None:
    """Verify ``scripts/create-indexes.js`` has been applied to ``jobs``.

    Specifically checks that the partial-unique index on
    ``workflow_key`` filtered to active statuses exists. Without it the
    workflow mutex doesn't actually serialize concurrent dispatches.

    The expected filter is derived from ``ACTIVE_STATUSES`` so Python is
    the single source of truth — if a new status is added to the enum
    without a matching update to ``scripts/create-indexes.js``, this
    startup check fires loudly rather than silently admitting duplicate
    active rows under the partial index. The ``$in`` list is sorted on
    both sides to handle ordering quirks across MongoDB and DocumentDB
    versions.
    """
    info = await db.jobs.index_information()
    expected_active = sorted(s.value for s in ACTIVE_STATUSES)
    for spec in info.values():
        key_pairs = spec.get("key") or []
        keys = [k for k, _ in key_pairs]
        if not (keys == ["workflow_key"] and spec.get("unique")):
            continue
        pfe = spec.get("partialFilterExpression")
        if not isinstance(pfe, dict):
            continue
        status_clause = pfe.get("status")
        if not isinstance(status_clause, dict):
            continue
        in_values = status_clause.get("$in")
        if not isinstance(in_values, list):
            continue
        if sorted(in_values) == expected_active:
            return
    log.error(
        "jobs.workflow_key partial-unique index missing or out of sync "
        "with ACTIVE_STATUSES=%s; run scripts/create-indexes.js",
        expected_active,
    )
    raise RuntimeError(
        "Required Mongo index 'jobs.workflow_key' (partial-unique, "
        f"filter status in {expected_active}) not found; run "
        "scripts/create-indexes.js against the target database before "
        "starting the API."
    )


def create_mongodb_client() -> AsyncIOMotorClient:
    """Create MongoDB client with optional TLS authentication."""
    log = logging.getLogger(__name__)
    kwargs: dict = {}

    if not api.MONGODB_RETRY_WRITES:
        kwargs["retryWrites"] = False

    if api.MONGODB_TLS_ENABLED:
        log.info("Connecting to MongoDB at %s with TLS", redact_url(api.DATABASE_URL))
        return AsyncIOMotorClient(
            api.DATABASE_URL,
            tls=True,
            tlsCAFile=api.MONGODB_CA_PATH,
            **kwargs,
        )
    log.info(
        "Connecting to MongoDB at %s (no authentication)",
        redact_url(api.DATABASE_URL),
    )
    return AsyncIOMotorClient(api.DATABASE_URL, **kwargs)


@asynccontextmanager
async def lifespan(_: FastAPI):
    log = logging.getLogger(__name__)

    if not api.SYNC_API_KEY:
        log.warning("SYNC_API_KEY not set — sync endpoint is unprotected")

    client = create_mongodb_client()
    api.db = client[api.DATABASE_NAME]
    try:
        # Fail-fast if the workflow mutex index is missing. The partial
        # unique index on ``jobs.workflow_key`` is the database-side
        # enforcement of "exactly one active workflow per source file";
        # without it, ``claim_workflow`` silently degrades to "no mutex"
        # and every miss dispatches a duplicate workflow.
        if api.SYNC_DATA_DIR:
            await _assert_jobs_indexes(api.db, log)

        # When SYNC_DATA_DIR is unset, skip workflow initialization
        # entirely. Routers detect the None state and fall back to their
        # direct-streaming paths.
        if api.SYNC_DATA_DIR:
            sync_root = Path(api.SYNC_DATA_DIR)
            cache_root = sync_root / "cache"
            workdir_root = sync_root / "jobs"
            # Fail-fast on mkdir error: an operator who explicitly set
            # SYNC_DATA_DIR has signalled "workflows on"; silently
            # degrading to disabled hides misconfiguration (wrong path,
            # bad perms, read-only mount) until later 5xx surprises.
            cache_root.mkdir(parents=True, exist_ok=True)
            workdir_root.mkdir(parents=True, exist_ok=True)

            # ``LocalFsCache.put`` uses ``os.replace`` for atomicity,
            # which only works when source and destination live on the
            # same filesystem (otherwise the kernel raises
            # ``OSError(EXDEV)``). Verify the precondition at startup so
            # a multi-volume deployment fails fast with a clear message
            # instead of dying mid-pipeline on the first cache.put.
            cache_st = os.stat(cache_root)
            workdir_st = os.stat(workdir_root)
            if cache_st.st_dev != workdir_st.st_dev:
                raise RuntimeError(
                    "SYNC_DATA_DIR subdirectories must share a filesystem "
                    f"(cache={cache_root!s} st_dev={cache_st.st_dev}, "
                    f"workdir={workdir_root!s} st_dev={workdir_st.st_dev}). "
                    "LocalFsCache.put relies on os.replace atomicity; "
                    "cross-device renames raise OSError(EXDEV). Mount both "
                    "paths under a single volume or set SYNC_DATA_DIR to a "
                    "parent that contains both."
                )

            api.cache = LocalFsCache(cache_root)
            api.processor_registry = default_registry()
            api.processor_registry.register(BamIndexProcessor())
            api.processor_registry.register(TabixIntervalProcessor())

            # Lease workers from the surrounding pool rather than spawning
            # them in-process. In production the workers run as separate
            # ECS tasks discovered via wool's discovery layer; the API's
            # job is to dispatch to whatever capacity exists. Scaling the
            # ECS service is out of band (e.g., on ``NoWorkersAvailable``
            # bursts or on a queue-depth metric).
            #
            # The explicit ``discovery=`` is required to keep wool out of
            # its default ephemeral mode — ``WorkerPool(lease=N)`` alone
            # falls into the ``(spawn=None, discovery=None)`` branch which
            # spawns CPU-count workers locally. Pairing ``lease=N`` with a
            # shared ``LanDiscovery`` namespace puts the pool in
            # discovery-only mode (no spawning); the worker-pool process
            # publishes workers via the same namespace. ``LanDiscovery``
            # rides over zeroconf/mDNS, which sidesteps the macOS
            # ``watchdog``/FSEvents fork-unsafety we hit with
            # ``LocalDiscovery``.
            async with wool.WorkerPool(
                discovery=LanDiscovery(api.WORKFLOW_POOL_NAMESPACE),
                lease=api.WORKFLOW_WORKER_COUNT,
            ):
                # Snapshot the lifespan task's contextvars after the
                # pool's ``__aenter__`` has populated wool's internals.
                api.wool_context = contextvars.copy_context()
                api.executor = WoolExecutor(
                    api.db,
                    api.cache,
                    cache_root,
                    api.processor_registry,
                    workdir_root=workdir_root,
                )
                executor_handle = api.executor
                log.info(
                    "Workflow subsystem enabled: cache=%s workdir=%s "
                    "lease=%d namespace=%s",
                    cache_root,
                    workdir_root,
                    api.WORKFLOW_WORKER_COUNT,
                    api.WORKFLOW_POOL_NAMESPACE,
                )
                try:
                    yield
                finally:
                    drained = await executor_handle.drain(
                        timeout=SHUTDOWN_DRAIN_TIMEOUT_SECONDS
                    )
                    if drained:
                        log.info(
                            "Drained %d workflow task(s) on shutdown", drained
                        )
                    api.executor = None
                    api.cache = None
                    api.processor_registry = None
                    api.wool_context = None
        else:
            log.info(
                "SYNC_DATA_DIR unset — workflow subsystem disabled; "
                "routers will serve files via direct streaming only."
            )
            yield
    finally:
        client.close()
        api.db = None


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def attach_wool_context(request, call_next):
    """Bridge the lifespan's wool contextvars into each request's task.

    uvicorn spawns each ASGI request handler in a fresh empty
    ``contextvars.Context()`` (see uvicorn's ``httptools_impl.py`` /
    ``h11_impl.py``), so the wool internals populated by
    ``wool.WorkerPool.__aenter__`` in the lifespan task —
    ``wool.__proxy__``, ``wool.runtime.discovery.__subscriber_pool__``,
    and other contextvars wool wires up during pool init — are
    invisible to handlers. This middleware iterates the lifespan
    snapshot and ``set``s each contextvar in the request task's
    context before the handler runs, restoring dispatch.

    Why a whole-context snapshot rather than enumerating known
    contextvars: wool ships several internal contextvars and may add
    more. Bridging the whole snapshot keeps cfdb's middleware from
    silently regressing every time wool grows a new one.
    """
    if api.wool_context is not None:
        for var, value in api.wool_context.items():
            var.set(value)
    return await call_next(request)


app.include_router(GraphQLRouter(schema), prefix="/metadata")
app.include_router(data_router)
app.include_router(index_router)
app.include_router(jobs_router)
app.include_router(sync_router)


@app.get("/health")
async def health():
    return JSONResponse({"status": "ok"})
