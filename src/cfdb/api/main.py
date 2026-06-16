import asyncio
import contextvars
import logging
import os
import re
from contextlib import AsyncExitStack, asynccontextmanager
from typing import AsyncIterator, Optional

import wool
from wool.runtime.discovery.lan import LanDiscovery
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from motor.motor_asyncio import AsyncIOMotorClient
from strawberry.fastapi import GraphQLRouter

from cfdb import api
from cfdb.api.gql.schema import schema
from cfdb.api.profile import WorkflowProfile
from cfdb.api.routers.data import router as data_router
from cfdb.api.routers.index import router as index_router
from cfdb.api.routers.jobs import router as jobs_router
from cfdb.api.routers.sync import router as sync_router
from cfdb.workflows.cache import (
    CacheBackend,
    LocalFsCache,
    S3Cache,
    _build_s3_client,
    check_s3_bucket_or_raise,
)
from cfdb.workflows.discovery import EcsDiscovery
from cfdb.workflows.executor import WoolExecutor
from cfdb.workflows.models import ACTIVE_STATUSES
from cfdb.workflows.processors.bam import BamIndexProcessor
from cfdb.workflows.processors.registry import default_registry
from cfdb.workflows.processors.tabix import TabixIntervalProcessor
from cfdb.workflows.provisioner import EcsProvisioner

logging.basicConfig(level=logging.INFO)

#: Upper bound on the lifespan shutdown drain for in-flight workflow
#: tasks. Tasks exceeding this are left for stale-reclamation on the
#: next service start — they were already bounded by the per-job
#: runtime cap and the stale threshold in ``workflows.lock``.
SHUTDOWN_DRAIN_TIMEOUT_SECONDS = 10.0


def redact_url(url: str) -> str:
    """Redact password from a MongoDB connection string for safe logging."""
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", url)


async def _drain_executor(
    executor_handle: "WoolExecutor", log: logging.Logger
) -> None:
    """Drain the workflow executor with the shared shutdown budget."""
    drained = await executor_handle.drain(timeout=SHUTDOWN_DRAIN_TIMEOUT_SECONDS)
    if drained:
        log.info("Drained %d workflow task(s) on shutdown", drained)


async def _aclose_provisioner_bounded(
    provisioner: EcsProvisioner, log: logging.Logger
) -> None:
    """Close the provisioner under a wall-clock cap.

    ``EcsProvisioner.aclose`` awaits cancelled ``RunTask`` tasks; if
    boto3 is configured without socket timeouts and the AWS endpoint is
    unreachable during shutdown, the join can hang indefinitely. Bound
    the wait to the shared shutdown drain budget so a stuck endpoint
    doesn't stall the lifespan until uvicorn SIGKILLs us — accept the
    leak as the price of bounded shutdown.
    """
    try:
        await asyncio.wait_for(
            provisioner.aclose(), timeout=SHUTDOWN_DRAIN_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        log.warning(
            "provisioner.aclose() exceeded %.1fs shutdown budget; "
            "leaking in-flight RunTasks for stale-task reclaim",
            SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
        )


async def _reset_api_globals() -> None:
    """Clear module-level pointers held by ``cfdb.api``.

    Runs last in the teardown stack so the globals are nulled even when
    earlier steps fail. Tests rely on this so a lifespan exception
    doesn't leak the executor / cache / processor registry / wool
    context into a subsequent app instantiation.
    """
    api.executor = None
    api.cache = None
    api.processor_registry = None
    api.wool_context = None


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


async def _build_cache(profile: WorkflowProfile) -> CacheBackend:
    """Build the cache backend dictated by ``profile``.

    For the ``s3-cached`` and ``ecs`` profiles, the boto3 client is
    built once and threaded into both :class:`S3Cache` and
    :func:`check_s3_bucket_or_raise` so the probe targets the same
    endpoint and we don't reach into ``cache._client`` from outside.
    For the ``local`` profile, a :class:`LocalFsCache` rooted at
    ``profile.cache_root`` is returned.
    """
    if profile.s3 is not None:
        client = _build_s3_client(
            endpoint_url=profile.aws_endpoint_url,
            region_name=profile.aws_region,
        )
        cache = S3Cache(
            bucket=profile.s3.bucket,
            prefix=profile.s3.prefix,
            client=client,
        )
        await check_s3_bucket_or_raise(profile.s3.bucket, client=client)
        return cache
    return LocalFsCache(profile.cache_root)


def _build_provisioner(profile: WorkflowProfile) -> Optional[EcsProvisioner]:
    """Build the :class:`EcsProvisioner` for the ``ecs`` profile.

    Returns ``None`` for ``local`` and ``s3-cached`` profiles.
    Partial-ECS validation lives in :meth:`WorkflowProfile.from_env`,
    so by the time this runs the ECS fields are either complete or
    absent.
    """
    if profile.ecs is None:
        return None
    return EcsProvisioner(
        cluster=profile.ecs.cluster,
        task_definition=profile.ecs.task_definition,
        subnets=profile.ecs.subnets,
        security_groups=profile.ecs.security_groups,
        assign_public_ip=profile.ecs.assign_public_ip,
        endpoint_url=profile.aws_endpoint_url,
        region_name=profile.aws_region,
    )


@asynccontextmanager
async def _build_discovery(profile: WorkflowProfile) -> AsyncIterator[object]:
    """Build the wool-compatible discovery layer for ``profile``.

    The ``ecs`` profile yields an :class:`EcsDiscovery` context — the
    background ``ListTasks`` / ``DescribeTasks`` poller starts on
    ``__aenter__`` and is cancelled on ``__aexit__``. The ``local``
    and ``s3-cached`` profiles fall through to :class:`LanDiscovery`
    over zeroconf/mDNS against a manually-started wool pool.

    Either branch yields a shape ``wool.WorkerPool`` accepts via its
    ``discovery=`` arg so the lifespan wires it through without
    branching at the call site.
    """
    if profile.ecs is not None and profile.ecs.task_family is not None:
        async with EcsDiscovery(
            cluster=profile.ecs.cluster,
            task_definition_family=profile.ecs.task_family,
            endpoint_url=profile.aws_endpoint_url,
            region_name=profile.aws_region,
        ) as discovery:
            yield discovery
        return
    yield LanDiscovery(api.WORKFLOW_POOL_NAMESPACE)


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
        # ``WorkflowProfile.from_env`` returns None when SYNC_DATA_DIR is
        # unset (workflow subsystem stays disabled; routers fall back to
        # direct-streaming) and raises on partial ECS config so a typo
        # cannot silently degrade to PoC fallback.
        profile = WorkflowProfile.from_env()

        # Fail-fast if the workflow mutex index is missing. The partial
        # unique index on ``jobs.workflow_key`` is the database-side
        # enforcement of "exactly one active workflow per source file";
        # without it, ``claim_workflow`` silently degrades to "no mutex"
        # and every miss dispatches a duplicate workflow.
        if profile is not None:
            await _assert_jobs_indexes(api.db, log)

            # Fail-fast on mkdir error: an operator who explicitly set
            # SYNC_DATA_DIR has signalled "workflows on"; silently
            # degrading to disabled hides misconfiguration (wrong path,
            # bad perms, read-only mount) until later 5xx surprises.
            profile.cache_root.mkdir(parents=True, exist_ok=True)
            profile.workdir_root.mkdir(parents=True, exist_ok=True)

            # ``LocalFsCache.put`` uses ``os.replace`` for atomicity,
            # which only works when source and destination live on the
            # same filesystem (otherwise the kernel raises
            # ``OSError(EXDEV)``). Verify the precondition at startup so
            # a multi-volume deployment fails fast with a clear message
            # instead of dying mid-pipeline on the first cache.put. Only
            # the local profile needs this — S3Cache.put goes over the
            # network and has no rename-atomicity requirement.
            if profile.kind == "local":
                cache_st = os.stat(profile.cache_root)
                workdir_st = os.stat(profile.workdir_root)
                if cache_st.st_dev != workdir_st.st_dev:
                    raise RuntimeError(
                        "SYNC_DATA_DIR subdirectories must share a filesystem "
                        f"(cache={profile.cache_root!s} st_dev={cache_st.st_dev}, "
                        f"workdir={profile.workdir_root!s} st_dev={workdir_st.st_dev}). "
                        "LocalFsCache.put relies on os.replace atomicity; "
                        "cross-device renames raise OSError(EXDEV). Mount both "
                        "paths under a single volume or set SYNC_DATA_DIR to a "
                        "parent that contains both."
                    )

            api.cache = await _build_cache(profile)
            api.processor_registry = default_registry()
            api.processor_registry.register(BamIndexProcessor())
            api.processor_registry.register(TabixIntervalProcessor())
            provisioner = _build_provisioner(profile)

            # Lease workers from the surrounding pool rather than spawning
            # them in-process. The ``ecs`` profile launches workers on
            # demand via ``EcsProvisioner`` and discovers them through
            # ``EcsDiscovery``'s poll over ``ListTasks`` /
            # ``DescribeTasks``. The ``local`` and ``s3-cached`` profiles
            # fall back to ``LanDiscovery`` (zeroconf/mDNS) against a
            # manually-started wool pool.
            #
            # The explicit ``discovery=`` is required to keep wool out of
            # its default ephemeral mode — ``WorkerPool(lease=N)`` alone
            # falls into the ``(spawn=None, discovery=None)`` branch which
            # spawns CPU-count workers locally.
            async with _build_discovery(profile) as discovery:
                async with wool.WorkerPool(
                    discovery=discovery,
                    lease=api.WORKFLOW_WORKER_COUNT,
                ):
                    # Snapshot the lifespan task's contextvars after the
                    # pool's ``__aenter__`` has populated wool's internals.
                    api.wool_context = contextvars.copy_context()
                    api.executor = WoolExecutor(
                        api.db,
                        api.cache,
                        api.processor_registry,
                        workdir_root=profile.workdir_root,
                        provisioner=provisioner,
                    )
                    executor_handle = api.executor
                    log.info(
                        "Workflow subsystem enabled: profile=%s cache=%s "
                        "workdir=%s lease=%d discovery=%s provisioner=%s",
                        profile.kind,
                        type(api.cache).__name__,
                        profile.workdir_root,
                        api.WORKFLOW_WORKER_COUNT,
                        type(discovery).__name__,
                        "EcsProvisioner" if provisioner is not None else "none",
                    )
                    # Stack each teardown step as an async callback so a
                    # failure earlier in the chain (e.g. ``drain`` raises
                    # on a Mongo blip) cannot skip later steps. Without
                    # this an ``executor.drain`` exception would leave
                    # ``provisioner.aclose`` and the api-global resets
                    # unrun — the very billable-leak bug that
                    # ``provisioner.aclose`` exists to prevent.
                    async with AsyncExitStack() as teardown:
                        teardown.push_async_callback(
                            _reset_api_globals
                        )
                        if provisioner is not None:
                            teardown.push_async_callback(
                                _aclose_provisioner_bounded,
                                provisioner,
                                log,
                            )
                        teardown.push_async_callback(
                            _drain_executor, executor_handle, log
                        )
                        yield
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
