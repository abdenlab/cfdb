"""Shared MongoDB client configuration.

Single source of truth for the connection kwargs that both the API
lifespan (``cfdb.api.main.create_mongodb_client``) and the ensure-indexes
CLI (``cfdb.ensure_indexes``) pass to a Motor client, so the TLS /
retry-write options can't drift between the two. Imports only the
``cfdb.api`` config constants (no FastAPI app) so lightweight callers
stay lightweight.
"""

from __future__ import annotations

from cfdb import api


def mongo_client_kwargs() -> dict:
    """Build ``AsyncIOMotorClient`` kwargs from the API's Mongo env config.

    Honors ``MONGODB_RETRY_WRITES`` and the ``MONGODB_TLS_*`` settings.
    Callers pass the result alongside ``DATABASE_URL`` to construct the
    client.
    """
    kwargs: dict = {}
    if not api.MONGODB_RETRY_WRITES:
        kwargs["retryWrites"] = False
    if api.MONGODB_TLS_ENABLED:
        kwargs["tls"] = True
        kwargs["tlsCAFile"] = api.MONGODB_CA_PATH
    return kwargs
