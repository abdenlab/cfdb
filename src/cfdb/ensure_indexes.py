"""Operator CLI to ensure cfdb's MongoDB indexes exist.

Run as ``python -m cfdb.ensure_indexes`` (or the ``cfdb-ensure-indexes``
console script). The API lifespan now self-heals the *operational*
indexes on startup and the sync pipeline ensures the *data* indexes
after a load, so this command is an operator convenience / manual escape
hatch — e.g. to pre-create every index against a fresh cluster, or to
re-apply after an index predicate change without waiting for a sync.

It honors the same MongoDB connection environment as the API
(``DATABASE_URL``, ``DATABASE_NAME``, ``MONGODB_TLS_ENABLED``,
``MONGODB_CA_PATH``, ``MONGODB_RETRY_WRITES``).
"""

from __future__ import annotations

import asyncio
import logging

import click
from motor.motor_asyncio import AsyncIOMotorClient

from cfdb import api
from cfdb.indexes import (
    all_index_specs,
    data_index_specs,
    ensure_indexes,
    operational_index_specs,
)

logger = logging.getLogger(__name__)

__all__ = ["main", "run"]

#: Maps the ``--scope`` choice to the spec builder it selects.
_SCOPES = {
    "operational": operational_index_specs,
    "data": data_index_specs,
    "all": all_index_specs,
}


def _build_client() -> AsyncIOMotorClient:
    """Create a Motor client honoring the API's MongoDB env config.

    Mirrors ``cfdb.api.main.create_mongodb_client`` (TLS + retry-writes)
    without importing the FastAPI app, so the CLI stays lightweight.
    """
    kwargs: dict = {}
    if not api.MONGODB_RETRY_WRITES:
        kwargs["retryWrites"] = False
    if api.MONGODB_TLS_ENABLED:
        kwargs["tls"] = True
        kwargs["tlsCAFile"] = api.MONGODB_CA_PATH
    return AsyncIOMotorClient(api.DATABASE_URL, **kwargs)


async def run(scope: str) -> int:
    """Ensure the indexes for ``scope`` and return the count ensured."""
    specs = _SCOPES[scope]()
    client = _build_client()
    try:
        db = client[api.DATABASE_NAME]
        return await ensure_indexes(db, specs)
    finally:
        client.close()


@click.command()
@click.option(
    "--scope",
    type=click.Choice(sorted(_SCOPES)),
    default="all",
    show_default=True,
    help=(
        "Which index set to ensure: operational (jobs/locks), data "
        "(query-perf), or all."
    ),
)
def main(scope: str) -> None:
    """Idempotently ensure cfdb's MongoDB indexes exist."""
    logging.basicConfig(level=logging.INFO)
    count = asyncio.run(run(scope))
    click.echo(f"Ensured {count} {scope} index(es)")


if __name__ == "__main__":  # pragma: no cover
    main()
