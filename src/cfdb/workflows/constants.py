"""Constants shared across cfdb.workflows.* modules.

Lives in its own leaf module so ``discovery`` can read
``DEFAULT_WORKER_PORT`` without importing ``worker_main`` — importing
``worker_main`` would drag aiohttp into ``discovery``'s import graph
and impose a runtime dependency that none of ``discovery``'s actual
consumers (the API lifespan, tests) need.
"""

from __future__ import annotations

#: Wool gRPC port the worker container binds and clients dial.
#: ``EcsDiscovery`` uses it to assemble worker addresses;
#: ``worker_main`` uses it as the bind port. The two MUST agree, and
#: the ECS task definition's ``portMappings`` / ``healthCheck`` target
#: MUST match.
DEFAULT_WORKER_PORT = 50051

# ECS task tags through which a worker publishes the parts of its
# ``wool.WorkerMetadata`` that only it can know. ``worker_main`` writes
# them onto its own task; ``EcsDiscovery`` reads them back off the
# ``DescribeTasks`` response. Both MUST agree on these keys — a
# mismatch means every worker looks unpublished and none are ever
# advertised (issue #90).
#
# Keys stay within the ECS tag charset (``[\p{L}\p{Z}\p{N}_.:/=+\-@]``)
# and are namespaced under ``wool.`` so they do not collide with
# cost-allocation or ownership tags applied to the same tasks.

#: Wool protocol version the worker runs, from ``WorkerMetadata.version``.
#: wool admits a worker only when the proxy's version is ``<=`` this and
#: shares its major, so an absent or invented value rejects the worker.
WORKER_TAG_VERSION = "wool.version"

#: Whether the worker requires TLS, from ``WorkerMetadata.secure``,
#: serialized as :data:`WORKER_TAG_TRUE` / ``"false"``. A proxy holding
#: credentials admits only secure workers, and one without credentials
#: admits only insecure ones — so this must reflect the worker's actual
#: configuration rather than a default.
WORKER_TAG_SECURE = "wool.secure"

#: Canonical serialization of a true ``secure`` flag. Compared
#: case-insensitively on read so a hand-applied tag still works.
WORKER_TAG_TRUE = "true"

# Worker mutual-TLS identity. These live here, rather than beside the
# rest of the TLS configuration in ``cfdb.workflows.credentials``,
# because ``cfdb.api`` needs the default too and importing
# ``credentials`` would drag wool into the API's config module. Holding
# one definition in a leaf module both sides can import is the whole
# reason this module exists — a second hand-copied literal is exactly
# the drift that produces an unexplainable NoWorkersAvailable.

#: Env var naming the logical identity a client verifies a worker's
#: certificate against, in place of the address it dialed.
TLS_IDENTITY_ENV = "CFDB_WORKER_TLS_IDENTITY"

#: Identity used when :data:`TLS_IDENTITY_ENV` is unset. Matches the SAN
#: ``certs/generate-worker-certs.sh`` mints into the worker leaf, so a
#: freshly generated cert set works with no further configuration. Set
#: the env var to the empty string to fall back to address verification.
#: ``cloudformation/backend.yml``'s ``WorkerTlsIdentity`` parameter
#: defaults to the same value, and a test asserts they agree.
DEFAULT_TLS_IDENTITY = "cfdb-worker"
