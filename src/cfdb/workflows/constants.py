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
