"""Helpers shared by processor pipelines.

Factored out so the BAM and tabix processors stay in sync on subprocess
launching, shell quoting, and cache→workdir staging.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shlex
import signal
from pathlib import Path
from typing import Any

from cfdb.workflows.cache import CacheBackend

logger = logging.getLogger(__name__)

_KILL_GRACE_SECONDS = 5.0


async def _terminate_process_group(proc: asyncio.subprocess.Process) -> None:
    """Best-effort SIGTERM (then SIGKILL) of a subprocess and its children.

    Subprocesses are launched with ``start_new_session=True`` so the bash
    parent and every stage of a shell pipeline land in their own process
    group. On cancellation, killing only the bash parent would leave the
    pipeline children reparented to PID 1 and still running — wasting
    worker memory/CPU and racing the cache cleanup. Sending the signal
    to the group via ``killpg`` reaches the whole pipeline.
    """
    if proc.returncode is not None:
        return
    pgid = None
    with contextlib.suppress(ProcessLookupError, OSError):
        pgid = os.getpgid(proc.pid)
    if pgid is None:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(pgid, signal.SIGTERM)
    try:
        await asyncio.wait_for(proc.wait(), timeout=_KILL_GRACE_SECONDS)
        return
    except asyncio.TimeoutError:
        pass
    with contextlib.suppress(ProcessLookupError):
        os.killpg(pgid, signal.SIGKILL)
    with contextlib.suppress(BaseException):
        await proc.wait()


def format_name(file_meta: dict[str, Any]) -> str | None:
    """Return ``file_meta["file_format"]["name"]`` or None if absent.

    A pure helper over the Mongo file-meta document shape, shared by
    the processor base class, the registry, and format-branching
    helpers inside each processor. None when the field is missing or
    the outer value isn't a dict.
    """
    fmt = file_meta.get("file_format")
    if isinstance(fmt, dict):
        return fmt.get("name")
    return None


def shell_quote(path: Path) -> str:
    """Quote a Path for safe interpolation into a shell pipeline."""
    return shlex.quote(str(path))


async def run_argv(argv: list[str]) -> None:
    """Run a subprocess via argv and raise on non-zero exit.

    Wraps ``proc.communicate()`` in a try/finally that kills the whole
    process group on cancellation or unexpected exception. Without that,
    a cancelled task (lifespan drain, ``asyncio.wait_for`` timeout)
    leaves the child running as a PID-1 orphan still holding workdir
    bytes and competing for the cache-write race.
    """
    proc = await asyncio.create_subprocess_exec(
        *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        _, stderr = await proc.communicate()
    except BaseException:
        await _terminate_process_group(proc)
        raise
    if proc.returncode != 0:
        raise RuntimeError(
            f"{argv[0]} exited {proc.returncode}: {stderr.decode(errors='replace')}"
        )


async def run_shell(cmd: str) -> None:
    """Run a shell pipeline string and raise on non-zero exit.

    The pipeline is executed under ``bash -o pipefail`` so a failure in
    any stage surfaces as a non-zero returncode. Without pipefail the
    default ``/bin/sh`` (``dash`` on Debian) reports only the final
    stage's exit code, and an upstream failure (e.g., ``zcat`` aborting
    on truncated gzip) would silently hand partial bytes to the final
    stage and commit a corrupted artifact to the content-addressed
    cache. ``bash`` is present in the ``Dockerfile.api`` base image.

    ``start_new_session=True`` + ``killpg`` on cancellation reaches every
    stage of the pipeline (not just the bash parent), preventing orphan
    children from outliving the cancelled task.
    """
    proc = await asyncio.create_subprocess_exec(
        "bash",
        "-o",
        "pipefail",
        "-c",
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        _, stderr = await proc.communicate()
    except BaseException:
        await _terminate_process_group(proc)
        raise
    if proc.returncode != 0:
        raise RuntimeError(
            f"shell command failed ({proc.returncode}): {cmd}\n"
            f"{stderr.decode(errors='replace')}"
        )


async def copy_from_cache(cache: CacheBackend, key: str, dest: Path) -> None:
    """Stream a cached artifact into ``dest`` for tool consumption.

    Accepts any :class:`CacheBackend` (``cache.get`` is the only call) so
    the S3 profile downloads from S3 here, not just the local FS.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as fh:
        async for chunk in cache.get(key):
            fh.write(chunk)
