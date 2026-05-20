import contextvars
import os
from typing import TYPE_CHECKING, Final

from motor.motor_asyncio import AsyncIOMotorDatabase

if TYPE_CHECKING:
    from cfdb.workflows.cache import CacheBackend
    from cfdb.workflows.executor import JobExecutor
    from cfdb.workflows.processors.registry import ProcessorRegistry

DATABASE_URL: Final = os.getenv("DATABASE_URL", "mongodb://127.0.0.1:27017")
DATABASE_NAME: Final = os.getenv("DATABASE_NAME", "cfdb")
PAGE_SIZE: Final = 25

# TLS authentication configuration (production)
MONGODB_TLS_ENABLED: Final = os.getenv("MONGODB_TLS_ENABLED", "false").lower() == "true"
MONGODB_CA_PATH: Final = os.getenv(
    "MONGODB_CA_PATH", "/etc/cfdb/certs/global-bundle.pem"
)
MONGODB_RETRY_WRITES: Final = os.getenv("MONGODB_RETRY_WRITES", "false").lower() == "true"

# Sync API authentication
SYNC_API_KEY: Final = os.getenv("SYNC_API_KEY", "")

# Workflow subsystem paths (overridable via environment for deployment
# tuning). When SYNC_DATA_DIR is unset the workflow subsystem is left
# disabled — routers fall through to their direct-streaming paths.
SYNC_DATA_DIR: Final = os.getenv("SYNC_DATA_DIR")


def _parse_int_env(name: str, default: int, *, minimum: int = 0) -> int:
    """Parse an int env var, raising a clear ImportError on malformed values.

    Bare ``int(os.getenv(...))`` produces a confusing ``ValueError`` at
    import time when an operator sets the var to something non-integer
    (or accidentally to the empty string). Raising ``ImportError`` with
    the offending name and value keeps the failure mode loud and
    self-explanatory in the traceback.

    ``minimum`` (default 0) provides a lower-bound check so a caller can
    require a strictly positive value where 0 would be silently broken.
    """
    raw = os.getenv(name)
    if raw is None or raw == "":
        if default < minimum:
            raise ImportError(
                f"Default {default} for {name} is below required minimum {minimum}"
            )
        return default
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ImportError(
            f"Environment variable {name}={raw!r} is not a valid integer"
        ) from exc
    if parsed < minimum:
        raise ImportError(
            f"Environment variable {name}={parsed} must be >= {minimum}"
        )
    return parsed


#: Upper bound on how many wool workers the API leases at once. The API
#: is **lease-only** — it never spawns workers in process. Workers are
#: provisioned externally (in development by manually starting a wool
#: pool with ``--spawn``; in production as ECS tasks) and discovered via
#: wool's discovery layer. When a workflow needs to launch, the executor
#: should also signal the provisioner to scale up so a worker exists by
#: the time dispatch retries surface it; the dispatch retry budget
#: (``cfdb.workflows.executor._DISPATCH_WAIT_SECONDS``) is sized for an
#: ECS cold start.
WORKFLOW_WORKER_COUNT: Final = _parse_int_env(
    "WORKFLOW_WORKER_COUNT", 2, minimum=1
)

#: Wool ``LanDiscovery`` namespace shared by the API (subscriber/leaser)
#: and the worker pool process(es) (publisher). Both sides MUST use the
#: same string; otherwise the API's discovery service won't see the
#: worker registrations and the pool will start with zero leasable
#: workers. The default ``"cfdb-workers"`` matches what the worker pool
#: needs to publish under; an ECS-aware variant may supplant LAN
#: discovery in a future PR.
WORKFLOW_POOL_NAMESPACE: Final = os.getenv("WORKFLOW_POOL_NAMESPACE", "cfdb-workers")

db: AsyncIOMotorDatabase | None = None
cache: "CacheBackend | None" = None
executor: "JobExecutor | None" = None
processor_registry: "ProcessorRegistry | None" = None

#: Snapshot of the lifespan task's ``contextvars.Context()`` taken after
#: ``wool.WorkerPool.__aenter__`` returns. uvicorn deliberately spawns
#: each ASGI request handler inside a fresh empty ``Context()`` (see
#: uvicorn ``httptools_impl.py`` / ``h11_impl.py``) for per-request
#: state isolation, so wool's contextvars set in the lifespan task —
#: ``wool.__proxy__``, ``wool.runtime.discovery.__subscriber_pool__``,
#: and friends — are invisible to handlers without an explicit bridge.
#: The middleware in ``cfdb.api.main`` re-applies the snapshot's wool
#: state on each request before the handler runs. We snapshot the whole
#: context (rather than enumerating individual contextvars) so newly
#: introduced wool internals don't silently break dispatch.
wool_context: contextvars.Context | None = None
