"""Relational DuckDB dump of the materialized ``files`` collection.

See :mod:`cfdb.dump.loader` for the flattening semantics and
``schema.sql`` for the schema of record (issue #123).
"""

from cfdb.dump.loader import (
    SCHEMA_VERSION,
    DumpError,
    DumpReport,
    dump_files,
)

__all__ = ["SCHEMA_VERSION", "DumpError", "DumpReport", "dump_files"]
