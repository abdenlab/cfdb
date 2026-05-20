"""Fixtures shared by workflow tests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Final

import pytest
from wool.runtime.routine.task import do_dispatch

from cfdb.workflows.cache import LocalFsCache

#: Canonical 32-char lowercase hex md5 used by every test fixture.
#: ``JobRecord.md5`` and ``normalize_md5`` reject anything else, so a
#: single shared constant keeps the suite reproducible and makes the
#: "this is a test fixture, not a real upstream md5" intent obvious in
#: every assertion.
FIXTURE_MD5: Final[str] = "d41d8cd98f00b204e9800998ecf8427e"

#: A second distinct md5 for tests that need to differentiate two file
#: identities in the same case.
FIXTURE_MD5_ALT: Final[str] = "098f6bcd4621d373cade4e832627b4f6"


def utcnow_aware() -> datetime:
    """Return an aware UTC ``datetime`` matching ``lock._utcnow()``.

    ``JobRecord`` rejects naive datetimes; tests constructing records by
    hand must use this helper rather than ``datetime.utcnow()`` (naive)
    or a hard-coded ``datetime(...)`` (also naive unless tzinfo is
    passed).
    """
    return datetime.now(timezone.utc)


@pytest.fixture()
def no_wool_dispatch():
    """Run @wool.routine calls in-process instead of dispatching to a worker.

    Wool's ``do_dispatch`` context variable gates whether a decorated
    routine serializes its call and sends it to a worker. In unit tests
    we want the routine body to run locally so we can assert on the
    executor's orchestration without starting a ``WorkerPool``.

    Integration tests should NOT use this fixture — they instead start a
    real ``WorkerPool(spawn=1)`` so the cloudpickle boundary is exercised.
    """
    with do_dispatch(False):
        yield


@pytest.fixture()
def tmp_cache(tmp_path) -> LocalFsCache:
    """Return a ``LocalFsCache`` rooted under ``tmp_path/cache``."""
    return LocalFsCache(tmp_path / "cache")


@pytest.fixture()
def tmp_workdir(tmp_path) -> Path:
    """Return a per-test workdir root under ``tmp_path/jobs``."""
    root = tmp_path / "jobs"
    root.mkdir(parents=True, exist_ok=True)
    return root
