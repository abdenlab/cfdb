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
#: drop-on-option-change behavior so a predicate change (e.g. flipping the
#: mutex partial filter) re-applies cleanly instead of aborting.
_INDEX_CONFLICT_CODES = (85, 86)  # IndexOptionsConflict, IndexKeySpecsConflict

#: ``OperationFailure.code`` for IndexNotFound — pymongo has no dedicated
#: exception class, so a ``dropIndex`` of an absent index surfaces as this.
_INDEX_NOT_FOUND_CODE = 27


def _default_index_name(keys: tuple[tuple[str, int], ...]) -> str:
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
    given, so plain field indexes need only their key spec. ``keys`` is
    normalized to a tuple of ``(field, direction)`` tuples in
    ``__post_init__`` so a frozen spec holds only immutable data
    (callers may pass a list for convenience).
    """

    collection: str
    keys: tuple[tuple[str, int], ...]
    name: str = ""
    unique: bool = False
    partial_filter: dict | None = None
    expire_after_seconds: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "keys", tuple((field, direction) for field, direction in self.keys)
        )
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


def operational_index_specs() -> list[IndexSpec]:
    """Indexes that must exist before the API serves traffic.

    Mirrors the ``locks`` + ``jobs`` block of
    ``scripts/create-indexes.js``.

    The ``jobs`` partial indexes filter on the boolean ``active``
    discriminator (``active == status in ACTIVE_STATUSES``, stamped by
    :meth:`JobRecord.to_mongo` and maintained by ``workflows.lock``)
    rather than a ``status`` ``$in`` list. Amazon DocumentDB rejects the
    ``$in`` operator inside a ``partialFilterExpression`` (only ``$eq``,
    ``$exists``, ``$and``, ``$gt/$gte/$lt/$lte`` are supported), so the
    predicate is expressed as implicit equality on ``active`` — which is
    DocumentDB's documented equivalent of ``$eq`` and what the test
    doubles also understand. ``ACTIVE_STATUSES`` remains the conceptual
    source of truth via the ``active`` derivation.
    """
    return [
        # locks.active — drives the sync cutover lock lookup.
        IndexSpec("locks", [("active", 1)]),
        # Partial-unique mutex: at most one active (pending|running) job
        # per source file. Terminal jobs have active=False so they fall
        # outside the filter and a fresh claim can succeed.
        IndexSpec(
            "jobs",
            [("workflow_key", 1)],
            name="workflow_key_active_unique",
            unique=True,
            partial_filter={"active": True},
        ),
        IndexSpec("jobs", [("job_id", 1)], name="job_id_unique", unique=True),
        IndexSpec("jobs", [("status", 1), ("updated_at", 1)], name="status_updated_at"),
        # Serves the durable retry scheduler's due-dispatch lease
        # (workflows.lock.lease_due_dispatch): {status: pending,
        # next_dispatch_at: {$lte: now}} sorted by next_dispatch_at asc.
        # The status equality prefix + next_dispatch_at range/sort are
        # both index-served, so the per-tick poll is not a PENDING scan.
        IndexSpec(
            "jobs",
            [("status", 1), ("next_dispatch_at", 1)],
            name="status_next_dispatch_at",
        ),
        # TTL on terminal rows so the collection doesn't grow unbounded.
        # The partial filter excludes active rows (active=True) so an
        # in-flight job is never reaped. 7 days gives operators a window
        # to investigate.
        IndexSpec(
            "jobs",
            [("updated_at", 1)],
            name="terminal_ttl",
            expire_after_seconds=60 * 60 * 24 * 7,
            partial_filter={"active": False},
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
        "accession_id",
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
        "accession_id",
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


async def _conflicting_index_name(collection, spec: IndexSpec) -> str:
    """Resolve the name of the index conflicting with ``spec``.

    On an options/key conflict the existing index may be registered under
    a different name than ``spec.name`` (e.g. a legacy unnamed index on
    the same key, or a predicate flipped on the same key pattern). Match
    by key pattern via ``index_information`` so the recreate path drops
    the right index rather than blindly dropping ``spec.name`` and tripping
    IndexNotFound. Falls back to ``spec.name`` when no key match is found.
    """
    target_keys = list(spec.keys)
    info = await collection.index_information()
    for existing_name, existing in info.items():
        existing_keys = [tuple(pair) for pair in (existing.get("key") or [])]
        if existing_keys == target_keys:
            return existing_name
    return spec.name


async def ensure_indexes(db, specs: list[IndexSpec]) -> int:
    """Idempotently ensure ``specs`` exist on ``db``.

    Mirrors the JS ``ensureIndex`` helper: ``create_index`` with a
    matching spec is a no-op, so the steady state makes zero changes; an
    index whose options changed (``IndexOptionsConflict`` /
    ``IndexKeySpecsConflict``) is dropped and recreated so a predicate
    change re-applies cleanly. The conflicting index is located by key
    pattern (not assumed to be named ``spec.name``), and an IndexNotFound
    on the drop is swallowed so a racing/already-dropped index doesn't
    abort the recreate. Returns the number of specs ensured.

    Note: the drop+recreate path momentarily leaves the index absent. For
    the ``workflow_key_active_unique`` mutex that is a brief window in
    which a concurrent claim on another instance could admit a duplicate
    active row — but this path only fires on a predicate change, not on
    steady-state startup, so the exposure is limited to a deliberate
    rollout. It is logged at WARNING so such rebuilds are visible.

    ``db`` is a Motor ``AsyncIOMotorDatabase`` (or an API-compatible
    async stand-in); ``create_index`` / ``drop_index`` /
    ``index_information`` are awaited.
    """
    for spec in specs:
        collection = db[spec.collection]
        kwargs = spec.create_kwargs()
        keys = list(spec.keys)
        try:
            await collection.create_index(keys, **kwargs)
        except OperationFailure as exc:
            if exc.code not in _INDEX_CONFLICT_CODES:
                raise
            drop_name = await _conflicting_index_name(collection, spec)
            logger.warning(
                "Index %s.%s conflicts with existing %r; dropping and "
                "recreating (the index is briefly absent during rebuild)",
                spec.collection,
                spec.name,
                drop_name,
            )
            try:
                await collection.drop_index(drop_name)
            except OperationFailure as drop_exc:
                if drop_exc.code != _INDEX_NOT_FOUND_CODE:
                    raise
            await collection.create_index(keys, **kwargs)
    logger.info("Ensured %d index(es)", len(specs))
    return len(specs)
