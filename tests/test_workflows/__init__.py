"""Workflow test package.

Exports the shared md5 / datetime fixtures that test modules import.
The actual ``pytest`` fixtures (``no_wool_dispatch``, ``tmp_cache``,
etc.) live in :mod:`conftest` and are auto-loaded.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Final

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


__all__ = ["FIXTURE_MD5", "FIXTURE_MD5_ALT", "utcnow_aware"]
