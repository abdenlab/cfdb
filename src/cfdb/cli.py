import logging
import os
from pathlib import Path

import click
import requests
from pymongo import MongoClient

__client__ = None

logging.basicConfig(level=logging.INFO)


def debug(ctx, param, value):
    """
    Enable debugging with debugpy.

    Args:
        ctx (click.Context): Click context.
        param (click.Parameter): Click parameter.
        value (bool): Flag value indicating whether to enable debugging.
    """
    if not value or ctx.resilient_parsing:
        return

    import debugpy

    debugpy.listen(5678)
    logging.debug("Waiting for debugger to attach...")
    debugpy.wait_for_client()
    logging.debug("Debugger attached")


def get_client(port=27017):
    global __client__
    if not __client__:
        __client__ = MongoClient(f"mongodb://localhost:{port}/")
    return __client__


@click.group()
def cli(): ...


@cli.command("sync")
@click.argument("dcc_names", nargs=-1, required=False)
@click.option(
    "--api-url",
    default="http://localhost:8000",
    envvar="CFDB_API_URL",
    help="CFDB API base URL",
)
@click.option(
    "--api-key",
    envvar="SYNC_API_KEY",
    help="API key for sync endpoint",
)
@click.option(
    "--debug",
    "-d",
    callback=debug,
    expose_value=False,
    help="Run with debugger listening on the specified port.",
    is_eager=True,
    type=int,
)
def sync(dcc_names: tuple[str, ...], api_url: str, api_key: str):
    """
    Trigger C2M2 datapackage sync via CFDB API.

    If no DDC names are specified, all supported DCCs will be synced.

    DCC_NAMES: Zero or more DCC names (4dn, hubmap). If omitted, all DCCs are synced.

    Examples:

        cfdb sync

        cfdb sync 4dn

        cfdb sync 4dn hubmap
    """
    # Build URL with query params
    url = f"{api_url}/sync"
    if dcc_names:
        params = "&".join(f"dccs={dcc}" for dcc in dcc_names)
        url = f"{url}?{params}"

    # Make POST request
    headers = {}
    if api_key:
        headers["X-API-Key"] = api_key

    try:
        response = requests.post(url, headers=headers)
    except requests.RequestException as e:
        click.echo(f"Error: Failed to connect to API: {e}", err=True)
        raise SystemExit(1)

    if response.status_code == 202:
        data = response.json()
        click.echo(f"Sync started: task_id={data['task_id']}")
        click.echo(f"DCCs: {', '.join(data['dcc_names'])}")
    elif response.status_code == 409:
        click.echo("Error: A sync is already in progress", err=True)
        raise SystemExit(1)
    elif response.status_code == 401:
        click.echo("Error: Invalid API key", err=True)
        raise SystemExit(1)
    else:
        click.echo(f"Error: {response.status_code} - {response.text}", err=True)
        raise SystemExit(1)


@cli.command("purge-legacy-cache")
@click.option(
    "--s3-bucket",
    default=None,
    envvar="WORKFLOW_S3_BUCKET",
    show_envvar=True,
    help="Bucket holding the workflow cache (S3 profile).",
)
@click.option(
    "--s3-prefix",
    default="",
    envvar="WORKFLOW_S3_PREFIX",
    show_envvar=True,
    help="Key prefix the S3 cache backend writes under.",
)
@click.option(
    "--endpoint-url",
    default=None,
    envvar="AWS_ENDPOINT_URL",
    show_envvar=True,
    help="boto3 endpoint override (LocalStack-backed dev).",
)
@click.option(
    "--region",
    default=None,
    envvar="AWS_REGION",
    show_envvar=True,
    help="AWS region for the boto3 client.",
)
@click.option(
    "--local-root",
    default=None,
    help="Local cache root. Defaults to $SYNC_DATA_DIR/cache.",
    type=click.Path(file_okay=False, exists=True, path_type=Path),
)
@click.option(
    "--apply",
    default=False,
    help="Delete the matched entries. Without it the sweep is a dry run.",
    is_flag=True,
)
@click.option(
    "--yes",
    default=False,
    help="Skip the --apply confirmation prompt (for scripted runs).",
    is_flag=True,
)
def purge_legacy_cache(
    s3_bucket: str | None,
    s3_prefix: str,
    endpoint_url: str | None,
    region: str | None,
    local_root: Path | None,
    apply: bool,
    yes: bool,
):
    """
    Sweep workflow cache entries minted under the retired key scheme.

    Issue #109 folded a processor identity into the cache key, so every
    entry written before it is unreachable: the API derives the new key
    shape and never probes the old one. This command deletes those
    entries, including the orphaned .bedpe / bigInteract index artifacts
    left behind when PR #108 re-typed those formats.

    WARNING: --apply deletes objects irreversibly. Runs as a DRY RUN
    unless it is passed.

    Exactly one store is swept per run. Resolving both an S3 bucket and a
    local cache root is a usage error rather than a precedence rule --
    purging the wrong store cannot be undone, so the command refuses to
    guess. $SYNC_DATA_DIR is consulted for the local root only when no
    bucket is configured.

    Examples:

        cfdb purge-legacy-cache

        cfdb purge-legacy-cache --local-root ./data/cache --apply

        cfdb purge-legacy-cache --s3-bucket cfdb-cache --s3-prefix dev --apply
    """
    from cfdb.workflows.purge import build_s3_client, purge_local, purge_s3

    sync_data_dir = os.getenv("SYNC_DATA_DIR")
    env_local_root = Path(sync_data_dir) / "cache" if sync_data_dir else None
    if local_root is None:
        local_root = env_local_root

    if s3_bucket and local_root is not None:
        # Both stores resolved — refuse rather than guess. This fires on the
        # environment-only pairing too: a container that sets both
        # WORKFLOW_S3_BUCKET and SYNC_DATA_DIR (backend.yml does) would
        # otherwise silently sweep S3 for an operator who meant the local
        # cache, and purging the wrong store is not recoverable.
        source = "--local-root" if env_local_root != local_root else "$SYNC_DATA_DIR"
        raise click.UsageError(
            f"Both an S3 bucket and a local cache root ({source}) resolved; "
            f"pass only one (unset WORKFLOW_S3_BUCKET to target the local root)"
        )
    if not s3_bucket and local_root is None:
        raise click.UsageError(
            "No cache to purge: pass --s3-bucket or --local-root, or set "
            "WORKFLOW_S3_BUCKET / SYNC_DATA_DIR"
        )

    target = (
        f"s3://{s3_bucket}/{s3_prefix.strip('/')}".rstrip("/")
        if s3_bucket
        else str(local_root)
    )

    # Name the target BEFORE sweeping. The store is chosen partly from
    # ambient environment, so an operator must be able to see which one was
    # picked while the run is still stoppable — not after the deletes.
    click.echo(f"Target: {target}")
    if apply and not yes:
        click.confirm(
            f"Irreversibly delete legacy cache entries from {target}?",
            abort=True,
        )

    if s3_bucket:
        report = purge_s3(
            build_s3_client(endpoint_url=endpoint_url, region_name=region),
            s3_bucket,
            prefix=s3_prefix,
            apply=apply,
        )
    else:
        report = purge_local(local_root, apply=apply)

    click.echo(f"Scanned: {report.scanned}")
    click.echo(
        f"Legacy entries: {report.matched} "
        f"({report.bytes_matched:,} bytes, {report.bytes_matched / 1024**3:.2f} GiB)"
    )
    if apply:
        click.echo(f"Deleted: {report.deleted}")
    else:
        click.echo("Dry run — nothing deleted. Re-run with --apply.")

    # A clean sweep and a mis-targeted one both report zero. Distinguish
    # them, so an operator working through the migration runbook cannot tick
    # an environment off on the strength of a prefix typo.
    if report.scanned == 0:
        click.echo(
            f"WARNING: {target} held nothing — check the bucket, prefix, or "
            f"path before treating this environment as swept.",
            err=True,
        )
    elif report.matched == 0:
        click.echo(
            f"WARNING: scanned {report.scanned} entries and matched none. If "
            f"this environment was not already swept, check --s3-prefix "
            f"against the deployment's WORKFLOW_S3_PREFIX.",
            err=True,
        )


if __name__ == "__main__":
    cli()
