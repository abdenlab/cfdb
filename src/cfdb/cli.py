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
    help="Bucket holding the workflow cache (S3 profile).",
)
@click.option(
    "--s3-prefix",
    default="",
    envvar="WORKFLOW_S3_PREFIX",
    help="Key prefix the S3 cache backend writes under.",
)
@click.option(
    "--endpoint-url",
    default=None,
    envvar="AWS_ENDPOINT_URL",
    help="boto3 endpoint override (LocalStack-backed dev).",
)
@click.option(
    "--region",
    default=None,
    envvar="AWS_REGION",
    help="AWS region for the boto3 client.",
)
@click.option(
    "--local-root",
    default=None,
    help="Local cache root. Defaults to $SYNC_DATA_DIR/cache.",
    type=click.Path(file_okay=False, path_type=Path),
)
@click.option(
    "--apply",
    default=False,
    help="Delete the matched entries. Without it the sweep is a dry run.",
    is_flag=True,
)
def purge_legacy_cache(
    s3_bucket: str | None,
    s3_prefix: str,
    endpoint_url: str | None,
    region: str | None,
    local_root: Path | None,
    apply: bool,
):
    """
    Sweep workflow cache entries minted under the retired key scheme.

    Issue #109 folded a processor identity into the cache key, so every
    entry written before it is unreachable: the API derives the new key
    shape and never probes the old one. This command deletes those
    entries, including the orphaned .bedpe / bigInteract index artifacts
    left behind when PR #108 re-typed those formats.

    Runs as a DRY RUN unless --apply is passed. The target is the S3
    bucket when one is configured, otherwise the local cache root.

    Examples:

        cfdb purge-legacy-cache

        cfdb purge-legacy-cache --local-root ./data/cache --apply

        cfdb purge-legacy-cache --s3-bucket cfdb-cache --s3-prefix dev --apply
    """
    from cfdb.workflows.purge import build_s3_client, purge_local, purge_s3

    if local_root is None and not s3_bucket:
        sync_data_dir = os.getenv("SYNC_DATA_DIR")
        if sync_data_dir:
            local_root = Path(sync_data_dir) / "cache"

    if s3_bucket and local_root is not None:
        # Both stores resolved — refuse rather than guess. WORKFLOW_S3_BUCKET
        # in the environment is enough to trigger this alongside an explicit
        # --local-root, and purging the wrong store is not recoverable.
        raise click.UsageError(
            "Both an S3 bucket and a local cache root resolved; pass only "
            "one (unset WORKFLOW_S3_BUCKET to target --local-root)"
        )
    if not s3_bucket and local_root is None:
        raise click.UsageError(
            "No cache to purge: pass --s3-bucket or --local-root, or set "
            "WORKFLOW_S3_BUCKET / SYNC_DATA_DIR"
        )

    if s3_bucket:
        target = f"s3://{s3_bucket}/{s3_prefix.strip('/')}".rstrip("/")
        report = purge_s3(
            build_s3_client(endpoint_url=endpoint_url, region_name=region),
            s3_bucket,
            prefix=s3_prefix,
            apply=apply,
        )
    else:
        target = str(local_root)
        report = purge_local(local_root, apply=apply)

    click.echo(f"Target: {target}")
    click.echo(f"Scanned: {report.scanned}")
    click.echo(
        f"Legacy entries: {report.matched} "
        f"({report.bytes_matched:,} bytes, {report.bytes_matched / 1024**3:.2f} GiB)"
    )
    if apply:
        click.echo(f"Deleted: {report.deleted}")
    else:
        click.echo("Dry run — nothing deleted. Re-run with --apply.")


if __name__ == "__main__":
    cli()
