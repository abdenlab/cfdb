import contextvars
import os
from typing import TYPE_CHECKING, Final

from motor.motor_asyncio import AsyncIOMotorDatabase

# A leaf module with no third-party imports of its own, so this stays
# clear of the runtime workflow (and wool) dependency the rest of
# ``cfdb.workflows`` carries — see the TYPE_CHECKING block below.
from cfdb.workflows.constants import DEFAULT_TLS_IDENTITY, TLS_IDENTITY_ENV

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


#: Wool ``LanDiscovery`` namespace shared by the API (subscriber/leaser)
#: and the worker pool process(es) (publisher). Both sides MUST use the
#: same string; otherwise the API's discovery service won't see the
#: worker registrations and the pool will start with zero leasable
#: workers. The default ``"cfdb-workers"`` matches what the worker pool
#: needs to publish under. Ignored when the ECS-discovery profile is
#: active (see ``ECS_CLUSTER`` / ``ECS_WORKER_TASK_DEFINITION``); the
#: ECS path discovers workers by polling the ECS control plane directly
#: and does not need a shared zeroconf namespace.
WORKFLOW_POOL_NAMESPACE: Final = os.getenv("WORKFLOW_POOL_NAMESPACE", "cfdb-workers")

# Worker gRPC mutual-TLS. These three cert paths gate transport
# encryption + peer authentication on the channel the API uses to
# dispatch ``@wool.routine`` work to wool workers. When all three are
# unset the channel stays plaintext (the PoC default); when all three
# are set the API presents its client certificate and the workers
# require a CA-signed one (``mutual=True``). Both sides must hold certs
# signed by the same CA (see ``CFDB_WORKER_TLS_CA``). Partial config
# (some set, some not) fails fast at pool construction — see
# ``cfdb.workflows.credentials.build_worker_credentials``. The
# ``CFDB_WORKER_TLS_*`` names are shared with the worker entrypoints
# (``worker_main`` / ``worker_lan``); each process points CERT/KEY at
# its own leaf material while the CA is common. The fourth setting,
# ``CFDB_WORKER_TLS_IDENTITY``, is shared too: it names the peer a
# *dialing* process expects to be talking to, which is the API on the
# dispatch channel — and also each worker on the one channel wool opens
# back to its own subprocess to drain it (roles are per-connection, not
# per-process; see ``cfdb.workflows.credentials``).

#: Path to the shared CA certificate the API verifies workers against
#: (and that signed the API's own client certificate). Unset disables
#: worker mTLS.
CFDB_WORKER_TLS_CA: Final = os.getenv("CFDB_WORKER_TLS_CA")

#: Path to the API's PEM client certificate, presented to workers when
#: mTLS is enabled. Must be signed by ``CFDB_WORKER_TLS_CA``.
CFDB_WORKER_TLS_CERT: Final = os.getenv("CFDB_WORKER_TLS_CERT")

#: Path to the API's PEM client private key paired with
#: ``CFDB_WORKER_TLS_CERT``.
CFDB_WORKER_TLS_KEY: Final = os.getenv("CFDB_WORKER_TLS_KEY")

#: Logical name the API verifies worker certificates against, in place
#: of the address it dialed — workers answer on dynamic addresses (an
#: awsvpc IP on ECS, a bridge IP in local containers) that no static SAN
#: can enumerate. Defaults to the SAN ``certs/generate-worker-certs.sh``
#: mints, so a freshly generated cert set needs no extra configuration.
#: Set it to the empty string to verify against the dialed address
#: instead. Ignored entirely while mTLS is off.
CFDB_WORKER_TLS_IDENTITY: Final = os.getenv(
    TLS_IDENTITY_ENV, DEFAULT_TLS_IDENTITY
)

# AWS / ECS profile. These knobs are optional — when none of them are
# set the API runs the PoC profile (``LocalFsCache`` + ``LanDiscovery``
# + no worker provisioner) and behaves exactly as it did before the
# Fargate work landed. Production / LocalStack-backed dev sets the
# bucket + cluster + task-def + subnets to activate the ECS-backed
# profile; the same code path serves both because boto3 honors
# ``AWS_ENDPOINT_URL`` to redirect at LocalStack.

#: boto3 endpoint override. In production this is unset and boto3
#: targets real AWS endpoints; LocalStack-backed dev sets it to
#: ``http://localstack:4566`` so the same code talks to the local
#: container. Threaded through to ``_build_s3_client`` /
#: ``build_ecs_client`` (in ``cfdb.workflows.cache`` /
#: ``cfdb.workflows.provisioner``) via the boto3 ``Session`` default
#: chain, so no per-client wiring is needed here.
AWS_ENDPOINT_URL: Final = os.getenv("AWS_ENDPOINT_URL")

#: AWS region. Defaults to ``us-east-1`` so a missing ``AWS_REGION`` in
#: dev doesn't surface as an opaque boto3 ``NoRegionError`` at first
#: request — operators get a working default and override it in
#: production deployments.
AWS_REGION: Final = os.getenv("AWS_REGION", "us-east-1")

#: When set, the lifespan instantiates ``S3Cache`` instead of
#: ``LocalFsCache``. Unset means the API stays on the local
#: filesystem cache.
WORKFLOW_S3_BUCKET: Final = os.getenv("WORKFLOW_S3_BUCKET")

#: Optional key prefix the S3 backend prepends to every cache key.
#: Lets a single bucket host multiple environments (``dev/``,
#: ``staging/``, ``prod/``) without collisions.
WORKFLOW_S3_PREFIX: Final = os.getenv("WORKFLOW_S3_PREFIX", "")

#: ECS cluster name or ARN. Gates the ECS-backed provisioner and
#: discovery profile; unset means the PoC profile stays on
#: ``LanDiscovery`` with no provisioner.
ECS_CLUSTER: Final = os.getenv("ECS_CLUSTER")

#: ECS worker task definition. Accepts either a family name
#: (``cfdb-worker``) or a ``family:revision`` string. The provisioner
#: passes it through to ``RunTask`` verbatim; the discovery loop
#: strips any ``:revision`` suffix to derive its
#: ``family`` filter (see ``ECS_WORKER_TASK_FAMILY`` override below).
ECS_WORKER_TASK_DEFINITION: Final = os.getenv("ECS_WORKER_TASK_DEFINITION")


def _ecs_default_task_family() -> str | None:
    """Derive the discovery ``family`` filter from the task definition.

    ``RunTask`` accepts ``family[:revision]``; ``ListTasks`` accepts
    only the family (no revision). The default split strips the
    revision when present, with ``ECS_WORKER_TASK_FAMILY`` available as
    an explicit override for environments that pin a non-default
    family name.
    """
    explicit = os.getenv("ECS_WORKER_TASK_FAMILY")
    if explicit:
        return explicit
    if ECS_WORKER_TASK_DEFINITION:
        return ECS_WORKER_TASK_DEFINITION.split(":", 1)[0]
    return None


#: Family used by ``EcsDiscovery`` to filter ``ListTasks``. Derived
#: from ``ECS_WORKER_TASK_DEFINITION`` by default; set explicitly via
#: ``ECS_WORKER_TASK_FAMILY`` only when the discovery family differs
#: from the provisioner task-def family (rare).
ECS_WORKER_TASK_FAMILY: Final = _ecs_default_task_family()


def _parse_csv_env(name: str, default: str = "") -> list[str]:
    """Parse a comma-separated env var into a list of trimmed strings.

    Empty strings are dropped so a trailing comma or double comma
    doesn't propagate as an empty subnet/SG entry that boto3 would
    later reject with a less informative error.
    """
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


#: Awsvpc subnet IDs the worker ENIs land in. Required for the ECS
#: profile; an empty list with ``ECS_CLUSTER`` set is a misconfiguration
#: that the lifespan refuses to start under.
ECS_WORKER_SUBNETS: Final = _parse_csv_env("ECS_WORKER_SUBNETS")

#: Awsvpc security groups attached to the worker ENIs. Optional —
#: when empty, ECS applies the VPC default SG.
ECS_WORKER_SECURITY_GROUPS: Final = _parse_csv_env("ECS_WORKER_SECURITY_GROUPS")

def _parse_assign_public_ip(name: str, default: str) -> str:
    """Parse an ECS ``assignPublicIp`` env var with explicit validation.

    ECS rejects anything other than ``ENABLED`` / ``DISABLED``; we
    surface the misconfiguration at module-import time so the lifespan
    doesn't get to Mongo + S3 init before tripping on it. The
    allowed-values set is sourced from
    :data:`cfdb.workflows.provisioner._ASSIGN_PUBLIC_IP_VALUES` (which
    derives it from the :data:`~cfdb.workflows.provisioner.AssignPublicIp`
    Literal) so the two stay in lockstep without duplication.
    """
    # Lazy import: ``cfdb.workflows.provisioner`` pulls boto3, which is
    # heavyweight and unused on lifespan paths that never construct an
    # ECS provisioner. Deferring the import keeps PoC-profile startup
    # snappy.
    from cfdb.workflows.provisioner import _ASSIGN_PUBLIC_IP_VALUES

    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    if raw not in _ASSIGN_PUBLIC_IP_VALUES:
        raise ImportError(
            f"Environment variable {name}={raw!r} must be one of "
            f"{sorted(_ASSIGN_PUBLIC_IP_VALUES)}"
        )
    return raw


#: Whether the worker ENI gets a public IPv4 address. Production
#: should leave this DISABLED and reach AWS via VPC endpoints;
#: LocalStack accepts either value.
ECS_WORKER_ASSIGN_PUBLIC_IP: Final = _parse_assign_public_ip(
    "ECS_WORKER_ASSIGN_PUBLIC_IP", "DISABLED"
)

#: Cap on concurrently-running ephemeral worker tasks on the ECS profile.
#: Before each RunTask the provisioner counts running/starting worker tasks
#: and skips the spawn when already at this cap, so the worker fleet is
#: bounded while excess jobs stay queued (the durable scheduler dispatches
#: them as workers free up — no shedding; the queue is bounded separately by
#: CFDB_WORKFLOW_MAX_ACTIVE). ``0`` disables the cap (rely on the Fargate
#: vCPU quota). Default 16 so an unconfigured ECS deployment fails safe to a
#: small fleet rather than the account quota. Applies only to the ECS
#: profile (no provisioner exists in the local/LAN profile).
ECS_MAX_WORKERS: Final = _parse_int_env("ECS_MAX_WORKERS", 16, minimum=0)

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
