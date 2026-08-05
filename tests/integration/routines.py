"""Test routines exercised across the Wool cloudpickle boundary.

Lives in a non-test module (no ``test_`` prefix) so:

1. pytest does not collect anything from here.
2. Cloudpickle resolves these classes by reference rather than by value
   when the Wool worker process unpickles a dispatch envelope. The
   worker's ``sys.path`` includes the project root but not the per-test
   module path; classes living in ``test_*.py`` would require the
   ``cloudpickle.register_pickle_by_value(sys.modules[__name__])``
   workaround to round-trip, which this module deliberately avoids.

Add new cross-boundary stubs here, not inside ``test_*.py`` files.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, Final

import wool

from cfdb.workflows.cache import CacheBackend
from cfdb.workflows.events import Complete, StageComplete, WorkflowEvent
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.base import Processor

#: Canonical 32-char lowercase hex md5 for stub file_meta. ``JobRecord``
#: + ``normalize_md5`` reject anything else, so a single shared
#: constant keeps integration fixtures reproducible.
STUB_MD5: Final[str] = "d41d8cd98f00b204e9800998ecf8427e"


class StubProcessor(Processor):
    """Processor double yielding a canned ``stage_complete``+``complete`` stream.

    Performs no I/O by default so a Wool integration test can exercise
    the cloudpickle round-trip in isolation, without making real
    subprocess or network calls. ``Processor.run`` is now an
    ``AsyncIterator[dict]`` per the executor's streaming-routine
    contract — yielding ``stage_complete`` per artifact then a single
    ``complete`` event with the full mapping.

    Optional knobs (all set via ``__init__`` so the instance is
    cloudpickle-friendly across the wool worker boundary):

    - ``raise_during_stage``: an exception to raise BEFORE the first
      ``stage_complete`` is yielded. Used by the failure-path test in
      ``TestWoolExecutorPickleBoundary`` to verify the executor surfaces
      a clean FAILED status with a path-scrubbed error.
    - ``sleep_seconds``: a delay inserted before the first event so
      drain / second-caller-attach tests have a reliable slow-running
      workflow to race against.
    - ``sleep_between_yields``: a delay inserted AFTER each
      ``stage_complete`` yield. Used by the runtime-cap test: a quick
      first event lets ``_open_stream_once`` return (and the executor mark
      the job RUNNING) so the ``asyncio.timeout(cap)`` block is entered,
      then the between-yields delay outlasts the cap so the wait for the
      next event triggers the cap.
    - ``unpicklable_field``: any value assigned to ``self.unpicklable_field``
      to deliberately poison cloudpickle. Used by the boundary tests
      that need to verify a pickling failure surfaces as a clean
      executor-level error rather than a deadlock.
    """

    processor_version = 0
    supported_formats = frozenset({"BAM"})
    artifact_kinds = (ArtifactKind.DATA, ArtifactKind.INDEX)

    def __init__(
        self,
        artifacts: dict[str, str] | None = None,
        *,
        raise_during_stage: BaseException | None = None,
        sleep_seconds: float = 0.0,
        sleep_between_yields: float = 0.0,
        unpicklable_field: Any | None = None,
    ) -> None:
        self.artifacts = artifacts or {
            ArtifactKind.DATA.value: f"encode/x/data/{STUB_MD5}-v0",
            ArtifactKind.INDEX.value: f"encode/x/index/{STUB_MD5}-v0",
        }
        self.raise_during_stage = raise_during_stage
        self.sleep_seconds = sleep_seconds
        self.sleep_between_yields = sleep_between_yields
        self.unpicklable_field = unpicklable_field

    async def run(
        self,
        file_meta: dict[str, Any],
        workdir: Path,
        cache: CacheBackend,
    ) -> AsyncIterator[WorkflowEvent]:
        if self.sleep_seconds > 0:
            await asyncio.sleep(self.sleep_seconds)
        if self.raise_during_stage is not None:
            raise self.raise_during_stage
        for kind, key in self.artifacts.items():
            yield StageComplete(kind=ArtifactKind(kind), key=key)
            if self.sleep_between_yields > 0:
                await asyncio.sleep(self.sleep_between_yields)
        yield Complete(artifacts=dict(self.artifacts))


@wool.routine
async def echo(value: str) -> str:
    """Return ``value`` from inside the worker process.

    The smallest thing that can cross the dispatch channel. Used by the
    mTLS tests, where the payload is irrelevant and the only question is
    whether the gRPC handshake succeeded at all.
    """
    return value


def stub_file_meta() -> dict[str, Any]:
    """Return a minimal BAM file_meta accepted by ``StubProcessor``."""
    return {
        "dcc": {"dcc_abbreviation": "ENCODE"},
        "local_id": "ENCFF123",
        "md5": STUB_MD5,
        "file_format": {"name": "BAM"},
    }
