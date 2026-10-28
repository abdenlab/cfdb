import logging

import click
import requests
from pymongo import MongoClient

from cfdb.dcc_registry import get_all_dcc_names

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

    debugpy.listen(value)
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


@cli.group("dump")
def dump():
    """Export cfdb data to offline analytic formats."""


@dump.command("duckdb")
@click.argument("output", type=click.Path(dir_okay=False, writable=True))
@click.option(
    "--database-url",
    default="mongodb://127.0.0.1:27017",
    envvar="DATABASE_URL",
    help="MongoDB connection string",
    show_default=True,
)
@click.option(
    "--database-name",
    default="cfdb",
    envvar="DATABASE_NAME",
    help="MongoDB database holding the materialized files collection",
    show_default=True,
)
@click.option(
    "--submission",
    "-s",
    "submissions",
    multiple=True,
    type=click.Choice(get_all_dcc_names(), case_sensitive=False),
    help="Restrict the dump to these consortia. Repeatable; omit for all.",
)
@click.option(
    "--to-parquet",
    "to_parquet",
    type=click.Path(file_okay=False),
    default=None,
    help="Also export every table as Parquet into this directory.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Overwrite OUTPUT if it already exists.",
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
def dump_duckdb(
    output: str,
    database_url: str,
    database_name: str,
    submissions: tuple[str, ...],
    to_parquet: str | None,
    force: bool,
):
    """
    Dump the materialized files collection to a relational DuckDB database.

    Streams every document in ``files`` (optionally restricted by
    --submission) and writes OUTPUT, a single .duckdb file holding the
    flattened schema defined in src/cfdb/dump/schema.sql. Requires the
    ``duckdb`` extra:
    pip install "cfdb[duckdb] @ git+https://github.com/abdenlab/cfdb.git".

    Examples:

        cfdb dump duckdb cfdb.duckdb

        cfdb dump duckdb cfdb.duckdb -s 4dn -s encode --to-parquet ./parquet
    """
    try:
        import duckdb
    except ImportError:
        click.echo(
            "Error: the DuckDB dump requires the 'duckdb' extra: "
            'pip install "cfdb[duckdb] @ git+https://github.com/abdenlab/cfdb.git"',
            err=True,
        )
        raise SystemExit(1)

    from pathlib import Path

    from cfdb.db import mongo_client_kwargs
    from cfdb.dump import DumpError, dump_files

    output_path = Path(output)
    if output_path.exists():
        if not force:
            click.echo(
                f"Error: {output_path} already exists (pass --force to overwrite)",
                err=True,
            )
            raise SystemExit(1)
        output_path.unlink()

    normalized = [s.lower() for s in submissions]
    query = {"submission": {"$in": normalized}} if normalized else {}
    client = MongoClient(database_url, **mongo_client_kwargs())
    cursor = client[database_name]["files"].find(query, batch_size=1000)

    con = duckdb.connect(str(output_path))
    completed = False
    try:
        report = dump_files(
            cursor,
            con,
            source_database=database_name,
            submissions=normalized or None,
        )
        completed = True
    except DumpError as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1)
    finally:
        client.close()
        if not completed:
            # A partial dump is worse than no dump: a finally block (rather
            # than except) runs this cleanup on every exit path, including
            # KeyboardInterrupt.
            con.close()
            output_path.unlink(missing_ok=True)

    # From here the dump is complete and self-describing; a failure in the
    # secondary parquet export must not destroy it.
    export_error: Exception | None = None
    if to_parquet:
        parquet_dir = str(Path(to_parquet)).replace("'", "''")
        try:
            con.execute(f"EXPORT DATABASE '{parquet_dir}' (FORMAT PARQUET)")
        except Exception as e:
            export_error = e
    con.close()

    click.echo(report.summary())
    click.echo(f"Wrote {output_path}")
    if export_error is not None:
        click.echo(
            f"Error: Parquet export to {to_parquet} failed ({export_error}); "
            f"{output_path} is complete and was kept",
            err=True,
        )
        raise SystemExit(1)
    if to_parquet:
        click.echo(f"Exported Parquet to {to_parquet}")


if __name__ == "__main__":
    cli()
