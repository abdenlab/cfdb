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
