import logging

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


if __name__ == "__main__":
    cli()
