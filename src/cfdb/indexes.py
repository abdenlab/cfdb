"""App-owned MongoDB index definitions and an idempotent applier.

This module is the single source of truth for the indexes cfdb relies
on. It supersedes ``scripts/create-indexes.js`` for every app-driven
code path — the API lifespan ensures the *operational* set on startup
and the sync pipeline ensures the *data* set after a load. The JS file
is retained only as the bootstrap for the local ``Dockerfile.mongodb``
image (which has no Python); a test pins the operational specs in
lockstep between the two so they cannot silently drift.

Two index sets, mirroring the split in the JS file:

* **Operational** — ``jobs`` and ``locks``. These back the workflow
  mutex and must exist *before* the API serves traffic, so the lifespan
  ensures them idempotently on every startup. The ``jobs.workflow_key``
  partial-unique filter and the ``terminal_ttl`` filter are derived from
  :data:`cfdb.workflows.models.ACTIVE_STATUSES` so Python — not a hand
  edited predicate — is the source of truth for which statuses hold the
  mutex.
* **Data** — the query-performance indexes on ``file``, ``dcc``,
  ``biosample`` and friends. These are only useful once a sync has
  loaded data, so they are ensured in the sync/materialize path rather
  than rebuilt against an empty database on every cold start.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pymongo.errors import OperationFailure

from cfdb.workflows.models import ACTIVE_STATUSES, JobStatus

logger = logging.getLogger(__name__)

__all__ = [
    "IndexSpec",
    "operational_index_specs",
    "data_index_specs",
    "all_index_specs",
    "ensure_indexes",
]

#: pymongo ``OperationFailure`` codes raised when an index already exists
#: under the requested name but with different options/key spec. We drop
#: and recreate in that case, matching the JS ``ensureIndex`` helper's
#: drop-on-option-change behavior so a predicate change (e.g. a new
#: active status) re-applies cleanly instead of aborting.
_INDEX_CONFLICT_CODES = (85, 86)  # IndexOptionsConflict, IndexKeySpecsConflict


def _default_index_name(keys: list[tuple[str, int]]) -> str:
    """Reproduce MongoDB's default index name for a key spec.

    MongoDB names an unnamed index ``field_dir[_field_dir...]`` (e.g.
    ``id_namespace_1`` / ``submission_1_id_1``). Deriving the same name
    means specs that mirror the historically-unnamed ``createIndex``
    calls in ``scripts/create-indexes.js`` resolve to the identical
    on-disk index, so ensuring them against a database already bootstrapped
    by the JS file is a no-op rather than a duplicate.
    """
    return "_".join(f"{field_name}_{direction}" for field_name, direction in keys)


@dataclass(frozen=True)
class IndexSpec:
    """A single MongoDB index to ensure on a collection.

    ``name`` defaults to MongoDB's derived name for ``keys`` when not
    given, so plain field indexes need only their key spec.
    """

    collection: str
    keys: list[tuple[str, int]]
    name: str = ""
    unique: bool = False
    partial_filter: dict | None = None
    expire_after_seconds: int | None = None

    def __post_init__(self) -> None:
        if not self.name:
            object.__setattr__(self, "name", _default_index_name(self.keys))

    def create_kwargs(self) -> dict:
        """Build the keyword arguments for ``Collection.create_index``."""
        kwargs: dict = {"name": self.name}
        if self.unique:
            kwargs["unique"] = True
        if self.partial_filter is not None:
            kwargs["partialFilterExpression"] = self.partial_filter
        if self.expire_after_seconds is not None:
            kwargs["expireAfterSeconds"] = self.expire_after_seconds
        return kwargs


def _terminal_statuses() -> list[str]:
    """Status values excluded from the active mutex set (the complement).

    Derived from :data:`ACTIVE_STATUSES` so the ``terminal_ttl`` filter
    stays lockstep with the enum: any status that is not active is, by
    definition, terminal and eligible for TTL reaping.
    """
    return [s.value for s in JobStatus if s not in ACTIVE_STATUSES]


def operational_index_specs() -> list[IndexSpec]:
    """Indexes that must exist before the API serves traffic.

    Mirrors the ``locks`` + ``jobs`` block of
    ``scripts/create-indexes.js``. The ``jobs.workflow_key`` partial
    filter and the ``terminal_ttl`` filter are derived from
    :data:`ACTIVE_STATUSES` rather than hard-coded.
    """
    active = [s.value for s in ACTIVE_STATUSES]
    return [
        # locks.active — drives the sync cutover lock lookup.
        IndexSpec("locks", [("active", 1)]),
        # Partial-unique mutex: at most one active (pending|running) job
        # per source file. Terminal jobs fall outside the filter so a
        # fresh claim can succeed.
        IndexSpec(
            "jobs",
            [("workflow_key", 1)],
            name="workflow_key_active_unique",
            unique=True,
            partial_filter={"status": {"$in": active}},
        ),
        IndexSpec("jobs", [("job_id", 1)], name="job_id_unique", unique=True),
        IndexSpec("jobs", [("status", 1), ("updated_at", 1)], name="status_updated_at"),
        # TTL on terminal rows so the collection doesn't grow unbounded.
        # The partial filter excludes active rows so an in-flight job is
        # never reaped. 7 days gives operators a window to investigate.
        IndexSpec(
            "jobs",
            [("updated_at", 1)],
            name="terminal_ttl",
            expire_after_seconds=60 * 60 * 24 * 7,
            partial_filter={"status": {"$in": _terminal_statuses()}},
        ),
    ]


def data_index_specs() -> list[IndexSpec]:
    """Query-performance indexes for the loaded C2M2 data collections.

    Mirrors the data-collection portion of ``scripts/create-indexes.js``.
    Only useful after a sync has loaded data, so these are ensured in the
    sync/materialize path rather than at API startup.
    """
    specs: list[IndexSpec] = []

    def add(collection: str, *keys: tuple[str, int], unique: bool = False) -> None:
        specs.append(IndexSpec(collection, list(keys), unique=unique))

    # file
    for f in (
        "id_namespace",
        "local_id",
        "project_id_namespace",
        "project_local_id",
        "persistent_id",
        "size_in_bytes",
        "sha256",
        "md5",
        "filename",
        "file_format",
        "compression_format",
        "data_type",
        "assay_type",
        "analysis_type",
        "mime_type",
        "bundle_collection_id_namespace",
        "bundle_collection_local_id",
        "dbgap_study_id",
        "access_url",
        "submission",
        "data_access_level",
    ):
        add("file", (f, 1))
    add("file", ("id_namespace", 1), ("local_id", 1))  # composite key

    # dcc
    for f in (
        "id",
        "dcc_name",
        "dcc_abbreviation",
        "dcc_description",
        "contact_email",
        "contact_name",
        "dcc_url",
        "project_id_namespace",
        "project_local_id",
        "submission",
    ):
        add("dcc", (f, 1))

    # Ontology-style collections: id/name/description + unique (submission, id).
    for collection in ("file_format", "data_type", "assay_type", "anatomy"):
        add(collection, ("id", 1))
        add(collection, ("name", 1))
        add(collection, ("description", 1))
        add(collection, ("submission", 1), ("id", 1), unique=True)

    # collection
    for f in (
        "id_namespace",
        "local_id",
        "persistent_id",
        "abbreviation",
        "name",
        "description",
        "submission",
    ):
        add("collection", (f, 1))
    add("collection", ("id_namespace", 1), ("local_id", 1))  # composite key

    # biosample
    for f in (
        "id_namespace",
        "local_id",
        "project_id_namespace",
        "project_local_id",
        "persistent_id",
        "sample_prep_method",
        "anatomy",
        "biofluid",
        "submission",
    ):
        add("biosample", (f, 1))
    add("biosample", ("id_namespace", 1), ("local_id", 1))  # composite key

    # file_in_collection
    for f in (
        "file_id_namespace",
        "file_local_id",
        "collection_id_namespace",
        "collection_local_id",
        "submission",
    ):
        add("file_in_collection", (f, 1))
    add("file_in_collection", ("file_id_namespace", 1), ("file_local_id", 1))
    add(
        "file_in_collection",
        ("collection_id_namespace", 1),
        ("collection_local_id", 1),
    )

    # biosample_in_collection
    for f in (
        "biosample_id_namespace",
        "biosample_local_id",
        "collection_id_namespace",
        "collection_local_id",
        "submission",
    ):
        add("biosample_in_collection", (f, 1))
    add(
        "biosample_in_collection",
        ("biosample_id_namespace", 1),
        ("biosample_local_id", 1),
    )
    add(
        "biosample_in_collection",
        ("collection_id_namespace", 1),
        ("collection_local_id", 1),
    )

    # subject
    for f in (
        "id_namespace",
        "local_id",
        "project_id_namespace",
        "project_local_id",
        "persistent_id",
        "granularity",
        "sex",
        "ethnicity",
        "submission",
    ):
        add("subject", (f, 1))
    add("subject", ("id_namespace", 1), ("local_id", 1))  # composite key

    # biosample_from_subject
    for f in (
        "biosample_id_namespace",
        "biosample_local_id",
        "subject_id_namespace",
        "subject_local_id",
        "submission",
    ):
        add("biosample_from_subject", (f, 1))
    add(
        "biosample_from_subject",
        ("biosample_id_namespace", 1),
        ("biosample_local_id", 1),
    )
    add(
        "biosample_from_subject",
        ("subject_id_namespace", 1),
        ("subject_local_id", 1),
    )

    # subject_race
    for f in ("subject_id_namespace", "subject_local_id", "race", "submission"):
        add("subject_race", (f, 1))
    add("subject_race", ("subject_id_namespace", 1), ("subject_local_id", 1))

    # collection_anatomy
    add("collection_anatomy", ("collection_id_namespace", 1), ("collection_local_id", 1))
    add("collection_anatomy", ("anatomy", 1))
    add("collection_anatomy", ("submission", 1))

    # subject_in_collection
    add("subject_in_collection", ("collection_id_namespace", 1), ("collection_local_id", 1))
    add("subject_in_collection", ("subject_id_namespace", 1), ("subject_local_id", 1))
    add("subject_in_collection", ("submission", 1))

    # ncbi_taxonomy
    add("ncbi_taxonomy", ("id", 1))
    add("ncbi_taxonomy", ("name", 1))
    add("ncbi_taxonomy", ("clade", 1))
    add("ncbi_taxonomy", ("submission", 1), ("id", 1), unique=True)

    # subject_role_taxonomy
    for f in ("subject_id_namespace", "subject_local_id", "taxonomy_id", "submission"):
        add("subject_role_taxonomy", (f, 1))
    add("subject_role_taxonomy", ("subject_id_namespace", 1), ("subject_local_id", 1))

    # project
    for f in ("id_namespace", "local_id", "name", "abbreviation", "submission"):
        add("project", (f, 1))
    add("project", ("id_namespace", 1), ("local_id", 1))  # composite key

    return specs


def all_index_specs() -> list[IndexSpec]:
    """Operational + data specs, in that order."""
    return operational_index_specs() + data_index_specs()


async def ensure_indexes(db, specs: list[IndexSpec]) -> int:
    """Idempotently ensure ``specs`` exist on ``db``.

    Mirrors the JS ``ensureIndex`` helper: ``create_index`` with a
    matching spec is a no-op, so the steady state makes zero changes; an
    index whose options changed (``IndexOptionsConflict`` /
    ``IndexKeySpecsConflict``) is dropped and recreated so a predicate
    change re-applies cleanly. Returns the number of specs ensured.

    ``db`` is a Motor ``AsyncIOMotorDatabase`` (or an API-compatible
    async stand-in); ``create_index`` / ``drop_index`` are awaited.
    """
    for spec in specs:
        collection = db[spec.collection]
        kwargs = spec.create_kwargs()
        try:
            await collection.create_index(spec.keys, **kwargs)
        except OperationFailure as exc:
            if exc.code in _INDEX_CONFLICT_CODES:
                logger.info(
                    "Index %s.%s options changed; dropping and recreating",
                    spec.collection,
                    spec.name,
                )
                await collection.drop_index(spec.name)
                await collection.create_index(spec.keys, **kwargs)
            else:
                raise
    logger.info("Ensured %d index(es)", len(specs))
    return len(specs)
