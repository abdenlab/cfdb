"""Flatten the materialized ``files`` collection into a DuckDB database.

The schema of record is ``schema.sql`` next to this module (issue #123):
C2M2-mirroring entity and junction tables keyed by natural
``(id_namespace, local_id)`` composites, promoted cross-consortium common
columns, per-consortium ``*_extra_*`` extension tables, three convenience
views, and provenance tables that make every dump self-describing.

The loader consumes an iterable of raw ``files`` documents — the CLI
wires in a pymongo cursor; tests feed plain dicts — and validates each
through :class:`cfdb.models.FileMetadataModel`, exactly as the GraphQL
resolvers hydrate the same documents, so the model's read validators
(empty-string-to-None, 4DN CV-token coercion) apply here as they do on
the API read path. ``submission`` is not a model field and is read from
the raw document.

Dedup policy. Entity rows repeat across file documents and nothing
guarantees the repeats are identical — enrichment passes update in
place — so entity rows are first-write-wins, with every divergent later
copy counted and a bounded sample written to ``dump_conflict``:
divergence is reported, never silently resolved. Junction rows instead
take the union over all copies (``SELECT DISTINCT`` at finalize),
because a relationship dropped by first-wins would be silent data loss
rather than a visible conflict. A key that resolves to two different
submissions aborts the dump: the natural keys are only globally unique
because id_namespaces are DCC-scoped, and that assumption is asserted
rather than trusted.

Memory model. Low-cardinality entities (dcc, project, ontology terms,
collections, biosamples, subjects, and their 1:1 extension rows) are
accumulated in Python registries so divergence can be compared column by
column. High-cardinality rows — files, their 1:1 extras, and every
junction — stream to constraint-free TEMP staging tables in batches, so
memory is bounded by the entity registries rather than the file count.
DuckDB has no ``ALTER TABLE ... ADD CONSTRAINT``, so the real tables
declare their foreign keys up front and finalize inserts parents before
children: dcc -> project -> ontology -> entities -> file -> junctions
and extensions.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib import metadata, resources
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Optional

from pydantic import ValidationError

from cfdb.models import Biosample, Collection, FileMetadataModel, Subject

if TYPE_CHECKING:  # duckdb is an optional extra; the connection is passed in.
    import duckdb

logger = logging.getLogger(__name__)

_SCHEMA_SQL = (resources.files(__package__) / "schema.sql").read_text()

#: Parsed from the ``-- schema_version: N`` header so the Python constant
#: cannot drift from the checked-in DDL.
_VERSION_MATCH = re.search(r"--\s*schema_version:\s*(\d+)", _SCHEMA_SQL)
if _VERSION_MATCH is None:
    raise RuntimeError(
        "schema.sql carries no '-- schema_version: N' header line; "
        "cfdb.dump parses it at import, so restore the header"
    )
SCHEMA_VERSION = int(_VERSION_MATCH.group(1))

#: Registry-backed tables, in foreign-key insert order.
_REGISTRY_INSERT_ORDER = (
    "dcc",
    "project",
    "file_format",
    "data_type",
    "assay_type",
    "anatomy",
    "ncbi_taxonomy",
    "collection",
    "biosample",
    "subject",
    "collection_extra_fourdn",
    "collection_extra_encode",
    "collection_extra_hubmap",
    "biosample_extra_encode",
    "subject_extra_hubmap",
)

#: Streamed tables, staged in TEMP shadows and inserted at finalize — the
#: file table first (its extras and junctions reference it).
_STAGED_FILE_TABLES = (
    "file",
    "file_extra_fourdn",
    "file_extra_encode",
    "file_extra_hubmap",
    "file_extra_fourdn_extra_file",
)
_JUNCTION_TABLES = (
    "file_in_collection",
    "biosample_in_collection",
    "subject_in_collection",
    "biosample_from_subject",
    "collection_anatomy",
    "subject_race",
)
_STAGED_TABLES = _STAGED_FILE_TABLES + _JUNCTION_TABLES


class DumpError(RuntimeError):
    """A condition that invalidates the dump as a whole."""


@dataclass
class DumpReport:
    """What a finished dump did, for the CLI summary and tests."""

    file_count: int
    rows: dict[str, int]
    conflicts: dict[str, int]
    stats: dict[str, int]

    def summary(self) -> str:
        lines = [f"files dumped: {self.file_count}"]
        lines += [
            f"  {table}: {count} rows" for table, count in sorted(self.rows.items())
        ]
        if self.conflicts:
            lines.append("divergent entity copies (first write kept):")
            lines += [f"  {t}: {n}" for t, n in sorted(self.conflicts.items())]
        counters = {k: v for k, v in self.stats.items() if v}
        if counters:
            lines.append("counters:")
            lines += [f"  {k}: {v}" for k, v in sorted(counters.items())]
        return "\n".join(lines)


@dataclass
class _Registry:
    """First-write-wins row store for one low-cardinality table."""

    table: str
    key_cols: tuple[str, ...] = ("id_namespace", "local_id")
    rows: dict[tuple, dict] = field(default_factory=dict)
    #: Keys registered as placeholders (a project known only through a
    #: foreign key). A real row replaces a stub silently; a stub never
    #: overwrites anything and never counts as a conflict.
    stubs: set[tuple] = field(default_factory=set)


def _utc_naive(value: datetime) -> datetime:
    """Normalize to UTC and drop tzinfo for DuckDB's TIMESTAMP."""
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _cfdb_version() -> Optional[str]:
    try:
        return metadata.version("cfdb")
    except metadata.PackageNotFoundError:
        return None


def _cfdb_commit() -> Optional[str]:
    """Best-effort HEAD sha when run from a cfdb checkout; None elsewhere.

    Anchored to this module's directory, never the process CWD, so the
    probe resolves the repository containing the installed package. The
    trackedness check rejects a site-packages that merely sits inside
    some other repository (a venv created in an analysis repo): only an
    editable checkout has this file tracked by the repo that contains it.
    """
    module_path = Path(__file__).resolve()
    try:
        tracked = subprocess.run(
            ["git", "ls-files", "--error-unmatch", module_path.name],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=module_path.parent,
        )
        if tracked.returncode != 0:
            return None
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=module_path.parent,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    sha = result.stdout.strip()
    return sha if result.returncode == 0 and sha else None


def _table_columns(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    rows = con.execute(f'PRAGMA table_info("{table}")').fetchall()
    return [row[1] for row in rows]


class _Dumper:
    """Single-use orchestrator behind :func:`dump_files`."""

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        source_database: str,
        source_collection: str,
        submissions: Optional[list[str]],
        conflict_sample_cap: int,
        batch_size: int,
    ) -> None:
        self.con = con
        self.source_database = source_database
        self.source_collection = source_collection
        self.requested_submissions = list(submissions) if submissions else None
        self.conflict_sample_cap = conflict_sample_cap
        self.batch_size = batch_size

        self.stats: Counter[str] = Counter()
        self.conflicts: Counter[str] = Counter()
        self.conflict_samples: list[tuple] = []
        self._sample_counts: Counter[str] = Counter()
        self.seen_submissions: set[str] = set()
        self._dcc_id_by_submission: dict[str, str] = {}
        self.file_count = 0

        self.registries: dict[str, _Registry] = {
            "dcc": _Registry("dcc", key_cols=("id",)),
            "project": _Registry("project"),
            "file_format": _Registry("file_format", key_cols=("id",)),
            "data_type": _Registry("data_type", key_cols=("id",)),
            "assay_type": _Registry("assay_type", key_cols=("id",)),
            "anatomy": _Registry("anatomy", key_cols=("id",)),
            "ncbi_taxonomy": _Registry("ncbi_taxonomy", key_cols=("id",)),
            "collection": _Registry("collection"),
            "biosample": _Registry("biosample"),
            "subject": _Registry("subject"),
            "collection_extra_fourdn": _Registry("collection_extra_fourdn"),
            "collection_extra_encode": _Registry("collection_extra_encode"),
            "collection_extra_hubmap": _Registry("collection_extra_hubmap"),
            "biosample_extra_encode": _Registry("biosample_extra_encode"),
            "subject_extra_hubmap": _Registry("subject_extra_hubmap"),
        }
        self._columns: dict[str, list[str]] = {}
        self._buffers: dict[str, list[dict]] = {table: [] for table in _STAGED_TABLES}

    # ── schema and staging ──────────────────────────────────────────────

    def bootstrap(self) -> None:
        self.con.execute(_SCHEMA_SQL)
        for table in _REGISTRY_INSERT_ORDER + _STAGED_TABLES:
            self._columns[table] = _table_columns(self.con, table)
        for table in _STAGED_TABLES:
            # LIMIT 0 copies the column shape without the constraints, so
            # staging accepts rows in any order and duplicates survive
            # until finalize dedupes or rejects them.
            self.con.execute(
                f'CREATE TEMP TABLE "stage_{table}" AS SELECT * FROM "{table}" LIMIT 0'
            )

    def _stage(self, table: str, row: dict) -> None:
        buffer = self._buffers[table]
        buffer.append(row)
        if len(buffer) >= self.batch_size:
            self._flush(table)

    def _flush(self, table: str) -> None:
        buffer = self._buffers[table]
        if not buffer:
            return
        self._insert_rows(f"stage_{table}", self._columns[table], buffer)
        buffer.clear()

    def _insert_rows(self, table: str, columns: list[str], rows: list[dict]) -> None:
        column_list = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        self.con.executemany(
            f'INSERT INTO "{table}" ({column_list}) VALUES ({placeholders})',
            [tuple(row.get(c) for c in columns) for row in rows],
        )

    # ── registries ──────────────────────────────────────────────────────

    def _register(self, table: str, row: dict, *, stub: bool = False) -> None:
        registry = self.registries[table]
        key = tuple(row[c] for c in registry.key_cols)
        existing = registry.rows.get(key)
        if existing is None:
            registry.rows[key] = row
            if stub:
                registry.stubs.add(key)
            return
        # The two-submission assertion runs before the stub short-circuits:
        # a stub carries the referencing document's real submission, so the
        # comparison is well-defined in every existing/incoming × real/stub
        # combination, and only the attribute upgrade stays silent.
        if "submission" in existing and existing["submission"] != row.get("submission"):
            key_names = ", ".join(registry.key_cols)
            raise DumpError(
                f"{table} key {key!r} appears under two submissions "
                f"({existing['submission']!r} and {row.get('submission')!r}); "
                f"({key_names}) is no longer globally unique and the "
                "dump schema's keying assumption does not hold"
            )
        if stub:
            return
        if key in registry.stubs:
            registry.rows[key] = row
            registry.stubs.discard(key)
            return
        diffs = [c for c in existing if existing.get(c) != row.get(c)]
        if not diffs:
            return
        self.conflicts[table] += 1
        natural_key = "|".join(str(part) for part in key)
        for column in diffs:
            if self._sample_counts[table] >= self.conflict_sample_cap:
                break
            self._sample_counts[table] += 1
            kept = existing.get(column)
            discarded = row.get(column)
            self.conflict_samples.append(
                (
                    table,
                    natural_key,
                    column,
                    None if kept is None else str(kept),
                    None if discarded is None else str(discarded),
                )
            )

    # ── coercions ───────────────────────────────────────────────────────

    def _coerce_timestamp(
        self, value: Optional[str], counter: str
    ) -> Optional[datetime]:
        if not value:
            return None
        try:
            return _utc_naive(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            self.stats[counter] += 1
            return None

    def _precoerce_int(self, container: Any, key: str, counter: str) -> None:
        """Coerce a known string-typed numeric field on the raw document.

        4DN and HuBMAP load ``size_in_bytes`` (and 4DN its sidecar
        ``file_size``) as strings through the C2M2 TSV path. Left alone,
        a non-numeric value would fail the whole document's model
        validation; coerced here, the failure costs one NULL and one
        counted coercion instead of the document.
        """
        if not isinstance(container, dict):
            return
        value = container.get(key)
        if value is None or isinstance(value, int) and not isinstance(value, bool):
            return
        if isinstance(value, str):
            text = value.strip()
            if not text:
                container[key] = None
                return
            try:
                container[key] = int(text)
            except ValueError:
                container[key] = None
                self.stats[counter] += 1
            return
        if isinstance(value, float) and value.is_integer():
            container[key] = int(value)
            return
        container[key] = None
        self.stats[counter] += 1

    def _precoerce(self, raw: dict) -> None:
        self._precoerce_int(raw, "size_in_bytes", "size_in_bytes_uncoercible")
        extra = raw.get("extra")
        fourdn = extra.get("fourdn") if isinstance(extra, dict) else None
        extra_files = fourdn.get("extra_files") if isinstance(fourdn, dict) else None
        if isinstance(extra_files, list):
            for entry in extra_files:
                self._precoerce_int(entry, "file_size", "extra_file_size_uncoercible")

    # ── document processing ─────────────────────────────────────────────

    def _skip_invalid(self, raw: dict, reason: str) -> None:
        self.stats["documents_invalid"] += 1
        logger.warning(
            "Skipping document %s/%s: %s",
            raw.get("submission"),
            raw.get("local_id"),
            reason,
        )

    def process(self, raw: dict) -> None:
        self._precoerce(raw)
        try:
            model = FileMetadataModel.model_validate(raw)
        except ValidationError as exc:
            self._skip_invalid(raw, f"model validation failed: {exc.errors()[:3]}")
            return
        submission = raw.get("submission")
        if not submission or not isinstance(submission, str):
            self._skip_invalid(raw, "no submission field")
            return
        if not model.id_namespace or not model.local_id:
            self._skip_invalid(raw, "missing (id_namespace, local_id) key")
            return
        if not model.dcc.id:
            self._skip_invalid(raw, "dcc carries no id")
            return

        self.seen_submissions.add(submission)
        self.file_count += 1

        self._register_dcc(model, submission)
        project_fk = self._project_fk(
            model.project_id_namespace, model.project_local_id, submission, model
        )
        self._stage_file(model, submission, project_fk)
        self._stage_file_extras(model)

        for collection in model.collections:
            self._process_collection(model, collection, submission)

    def _register_dcc(self, model: FileMetadataModel, submission: str) -> None:
        dcc = model.dcc
        # dcc.submission is declared UNIQUE, so the id <-> submission mapping
        # must be 1:1. _register's submission check asserts one direction
        # (one id under two submissions); this asserts the other, so the
        # violation is a one-line DumpError instead of a finalize traceback.
        known_id = self._dcc_id_by_submission.setdefault(submission, dcc.id)
        if known_id != dcc.id:
            raise DumpError(
                f"submission {submission!r} carries two dcc ids "
                f"({known_id!r} and {dcc.id!r}); dcc.submission is declared "
                "UNIQUE and the schema's 1:1 mapping assumption does not hold"
            )
        self._register(
            "dcc",
            {
                "id": dcc.id,
                "submission": submission,
                "dcc_name": dcc.dcc_name,
                "dcc_abbreviation": dcc.dcc_abbreviation,
                "dcc_description": dcc.dcc_description,
                "contact_email": dcc.contact_email or None,
                "contact_name": dcc.contact_name or None,
                "dcc_url": dcc.dcc_url or None,
                "project_id_namespace": dcc.project_id_namespace or None,
                "project_local_id": dcc.project_local_id or None,
            },
        )

    def _project_fk(
        self,
        id_namespace: Optional[str],
        local_id: Optional[str],
        submission: str,
        model: Optional[FileMetadataModel] = None,
    ) -> tuple[Optional[str], Optional[str]]:
        """Normalize a project FK pair and guarantee its target row exists.

        The joined project object and the FK columns travel separately in
        the source documents, so an FK can name a project the join never
        materialized — and biosamples and subjects carry project FKs with
        no joined object at all. A stub row (all-NULL attributes) keeps
        the foreign key satisfiable without inventing data; the real
        object upgrades the stub whenever a document carries it.
        """
        id_namespace = id_namespace or None
        local_id = local_id or None
        if (id_namespace is None) != (local_id is None):
            self.stats["project_fk_incomplete"] += 1
            return None, None
        if id_namespace is None:
            return None, None
        project = model.project if model is not None else None
        if (
            project is not None
            and project.id_namespace == id_namespace
            and project.local_id == local_id
        ):
            self._register(
                "project",
                {
                    "id_namespace": id_namespace,
                    "local_id": local_id,
                    "submission": submission,
                    "name": project.name or None,
                    "abbreviation": project.abbreviation,
                    "description": project.description,
                    "persistent_id": project.persistent_id,
                },
            )
        else:
            registry = self.registries["project"]
            if (id_namespace, local_id) not in registry.rows:
                self.stats["project_stubs_created"] += 1
            self._register(
                "project",
                {
                    "id_namespace": id_namespace,
                    "local_id": local_id,
                    "submission": submission,
                    "name": None,
                    "abbreviation": None,
                    "description": None,
                    "persistent_id": None,
                },
                stub=True,
            )
        return id_namespace, local_id

    def _term_fk(self, table: str, term: Any) -> Optional[str]:
        """Register a CV term and return its id for an FK column, or None.

        The term models default both ``id`` and ``name`` to ``""`` and
        pass validation that way, so either can arrive blank on a
        model-valid document. A term without an id cannot be keyed and a
        term without a name cannot satisfy the NOT NULL name column, so
        both are skipped and counted here — nulling the referencing FK
        column in the same decision, which is what keeps a dirty term a
        counter in ``dump_stat`` rather than a constraint crash at
        finalize.
        """
        if term is None:
            return None
        if not term.id:
            self.stats["term_id_missing"] += 1
            return None
        if not term.name:
            self.stats["term_name_missing"] += 1
            return None
        row = {
            "id": term.id,
            "name": term.name,
            "description": term.description,
        }
        if table == "ncbi_taxonomy":
            row["clade"] = term.clade
        self._register(table, row)
        return term.id

    @staticmethod
    def _any_value(row: dict, key_cols: tuple[str, ...]) -> bool:
        return any(
            value is not None for column, value in row.items() if column not in key_cols
        )

    def _stage_file(
        self,
        model: FileMetadataModel,
        submission: str,
        project_fk: tuple[Optional[str], Optional[str]],
    ) -> None:
        self._stage(
            "file",
            {
                "id_namespace": model.id_namespace,
                "local_id": model.local_id,
                "submission": submission,
                "accession_id": model.accession_id,
                "filename": model.filename or None,
                "persistent_id": model.persistent_id,
                "creation_time": self._coerce_timestamp(
                    model.creation_time, "creation_time_unparseable"
                ),
                "size_in_bytes": model.size_in_bytes,
                "sha256": model.sha256,
                "md5": model.md5,
                "file_format": self._term_fk("file_format", model.file_format),
                "compression_format": model.compression_format,
                "data_type": self._term_fk("data_type", model.data_type),
                "assay_type": self._term_fk("assay_type", model.assay_type),
                "analysis_type": model.analysis_type,
                "mime_type": model.mime_type,
                "project_id_namespace": project_fk[0],
                "project_local_id": project_fk[1],
                "bundle_collection_id_namespace": model.bundle_collection_id_namespace,
                "bundle_collection_local_id": model.bundle_collection_local_id,
                "dbgap_study_id": model.dbgap_study_id,
                "access_url": model.access_url,
                "status": model.status,
                "data_access_level": model.data_access_level,
                "genome_assembly": model.genome_assembly,
                "genome_annotation": model.genome_annotation,
                "output_type": model.output_type,
                "output_type_detail": model.output_type_detail,
                "biological_replicates": model.biological_replicates,
                "technical_replicates": model.technical_replicates,
                "assay_info": model.assay_info,
                "condition": model.condition,
            },
        )

    def _stage_file_extras(self, model: FileMetadataModel) -> None:
        extra = model.extra
        if extra is None:
            return
        key = {"id_namespace": model.id_namespace, "local_id": model.local_id}
        key_cols = ("id_namespace", "local_id")
        if extra.fourdn is not None:
            fourdn = extra.fourdn
            row = {
                **key,
                "enriched_file_format": fourdn.enriched_file_format,
                "dataset": fourdn.dataset,
                "biosource_name": fourdn.biosource_name,
                "replicate_info": fourdn.replicate_info,
                "cell_line_tier": fourdn.cell_line_tier,
            }
            if self._any_value(row, key_cols):
                self._stage("file_extra_fourdn", row)
            for ordinal, entry in enumerate(fourdn.extra_files or []):
                self._stage(
                    "file_extra_fourdn_extra_file",
                    {
                        **key,
                        "ordinal": ordinal,
                        "href": entry.href,
                        "md5sum": entry.md5sum,
                        "file_size": entry.file_size,
                        "file_format": entry.file_format,
                    },
                )
        if extra.encode is not None:
            encode = extra.encode
            row = {
                **key,
                "annotation_type": encode.annotation_type,
                "organism": encode.organism,
                "controlled_by": encode.controlled_by,
                "s3_uri": encode.s3_uri,
                "azure_url": encode.azure_url,
                "file_analysis_title": encode.file_analysis_title,
                "file_analysis_status": encode.file_analysis_status,
                "read_length": encode.read_length,
                "mapped_read_length": encode.mapped_read_length,
                "run_type": encode.run_type,
                "paired_end": encode.paired_end,
                "paired_with": encode.paired_with,
                "index_of": encode.index_of,
                "derived_from": encode.derived_from,
                "audit_warning": encode.audit_warning,
                "audit_not_compliant": encode.audit_not_compliant,
                "audit_error": encode.audit_error,
            }
            if self._any_value(row, key_cols):
                self._stage("file_extra_encode", row)
        if extra.hubmap is not None:
            hubmap = extra.hubmap
            row = {
                **key,
                "rel_path": hubmap.rel_path,
                "is_data_product": hubmap.is_data_product,
            }
            if self._any_value(row, key_cols):
                self._stage("file_extra_hubmap", row)

    def _process_collection(
        self, model: FileMetadataModel, collection: Collection, submission: str
    ) -> None:
        if not collection.id_namespace or not collection.local_id:
            self.stats["entities_missing_key"] += 1
            return
        self._register(
            "collection",
            {
                "id_namespace": collection.id_namespace,
                "local_id": collection.local_id,
                "submission": submission,
                "accession_id": collection.accession_id,
                "persistent_id": collection.persistent_id,
                "creation_time": self._coerce_timestamp(
                    collection.creation_time, "creation_time_unparseable"
                ),
                "abbreviation": collection.abbreviation,
                "name": collection.name or None,
                "description": collection.description,
                "lab": collection.lab,
                "experiment_type": collection.experiment_type,
                "experiment_target": collection.experiment_target,
                "analyte_class": collection.analyte_class,
            },
        )
        self._register_collection_extras(collection)
        collection_key = {
            "collection_id_namespace": collection.id_namespace,
            "collection_local_id": collection.local_id,
        }
        self._stage(
            "file_in_collection",
            {
                "file_id_namespace": model.id_namespace,
                "file_local_id": model.local_id,
                **collection_key,
            },
        )
        for anatomy in collection.anatomy:
            anatomy_id = self._term_fk("anatomy", anatomy)
            if anatomy_id is None:
                continue
            self._stage("collection_anatomy", {**collection_key, "anatomy": anatomy_id})
        for subject in collection.subjects:
            if not self._register_subject(subject, submission):
                continue
            self._stage(
                "subject_in_collection",
                {
                    "subject_id_namespace": subject.id_namespace,
                    "subject_local_id": subject.local_id,
                    **collection_key,
                },
            )
        for biosample in collection.biosamples:
            if not self._register_biosample(biosample, submission):
                continue
            self._stage(
                "biosample_in_collection",
                {
                    "biosample_id_namespace": biosample.id_namespace,
                    "biosample_local_id": biosample.local_id,
                    **collection_key,
                },
            )

    def _register_collection_extras(self, collection: Collection) -> None:
        extra = collection.extra
        if extra is None:
            return
        key = {
            "id_namespace": collection.id_namespace,
            "local_id": collection.local_id,
        }
        key_cols = ("id_namespace", "local_id")
        if extra.fourdn is not None:
            fourdn = extra.fourdn
            row = {
                **key,
                "display_title": fourdn.display_title,
                "targeted_factor": fourdn.targeted_factor,
                "digestion_enzyme": fourdn.digestion_enzyme,
                "crosslinking_method": fourdn.crosslinking_method,
                "crosslinking_temperature": fourdn.crosslinking_temperature,
                "crosslinking_time": fourdn.crosslinking_time,
                "ligation_temperature": fourdn.ligation_temperature,
                "ligation_volume": fourdn.ligation_volume,
                "ligation_time": fourdn.ligation_time,
                "digestion_temperature": fourdn.digestion_temperature,
                "digestion_time": fourdn.digestion_time,
                "tagging_method": fourdn.tagging_method,
                "fragmentation_method": fourdn.fragmentation_method,
                "biotin_removed": fourdn.biotin_removed,
                "library_prep_kit": fourdn.library_prep_kit,
                "average_fragment_size": fourdn.average_fragment_size,
                "fragment_size_range": fourdn.fragment_size_range,
                "status": fourdn.status,
                "date_created": self._coerce_timestamp(
                    fourdn.date_created, "date_created_unparseable"
                ),
            }
            if self._any_value(row, key_cols):
                self._register("collection_extra_fourdn", row)
        if extra.encode is not None:
            encode = extra.encode
            row = {
                **key,
                "project": encode.project,
                "platform": encode.platform,
                "dbxrefs": encode.dbxrefs,
                "rbns_protein_concentration": encode.rbns_protein_concentration,
                "annotation_type": encode.annotation_type,
                "software_used": encode.software_used,
                "encyclopedia_version": encode.encyclopedia_version,
            }
            if self._any_value(row, key_cols):
                self._register("collection_extra_encode", row)
        if extra.hubmap is not None:
            hubmap = extra.hubmap
            row = {
                **key,
                "pipeline": hubmap.pipeline,
                "processing": hubmap.processing,
                "group_name": hubmap.group_name,
                "visualization": hubmap.visualization,
                "vitessce_hints": hubmap.vitessce_hints,
                "metadata": (
                    # sort_keys so two equal, key-reordered copies compare
                    # equal instead of registering a false divergence.
                    None
                    if hubmap.metadata is None
                    else json.dumps(hubmap.metadata, sort_keys=True)
                ),
            }
            if self._any_value(row, key_cols):
                self._register("collection_extra_hubmap", row)

    def _register_biosample(self, biosample: Biosample, submission: str) -> bool:
        if not biosample.id_namespace or not biosample.local_id:
            self.stats["entities_missing_key"] += 1
            return False
        anatomy_id = self._term_fk("anatomy", biosample.anatomy)
        project_fk = self._project_fk(
            biosample.project_id_namespace, biosample.project_local_id, submission
        )
        self._register(
            "biosample",
            {
                "id_namespace": biosample.id_namespace,
                "local_id": biosample.local_id,
                "submission": submission,
                "project_id_namespace": project_fk[0],
                "project_local_id": project_fk[1],
                "persistent_id": biosample.persistent_id,
                "creation_time": self._coerce_timestamp(
                    biosample.creation_time, "creation_time_unparseable"
                ),
                "sample_prep_method": biosample.sample_prep_method,
                "anatomy": anatomy_id,
                "biofluid": biosample.biofluid,
            },
        )
        if biosample.extra is not None and biosample.extra.encode is not None:
            encode = biosample.extra.encode
            row = {
                "id_namespace": biosample.id_namespace,
                "local_id": biosample.local_id,
                "biosample_type": encode.biosample_type,
                "life_stage": encode.life_stage,
                "age": encode.age,
                "age_units": encode.age_units,
                "biosample_treatments": encode.biosample_treatments,
                "biosample_treatments_amount": encode.biosample_treatments_amount,
                "biosample_treatments_duration": encode.biosample_treatments_duration,
                "biosample_genetic_modifications": (
                    encode.biosample_genetic_modifications
                ),
                "library_made_from": encode.library_made_from,
                "library_depleted_in": encode.library_depleted_in,
                "library_extraction_method": encode.library_extraction_method,
                "library_lysis_method": encode.library_lysis_method,
                "library_crosslinking_method": encode.library_crosslinking_method,
                "library_strand_specific": encode.library_strand_specific,
                "library_fragmentation_method": encode.library_fragmentation_method,
                "library_size_range": encode.library_size_range,
            }
            if self._any_value(row, ("id_namespace", "local_id")):
                self._register("biosample_extra_encode", row)
        for subject in biosample.subjects:
            if not self._register_subject(subject, submission):
                continue
            self._stage(
                "biosample_from_subject",
                {
                    "biosample_id_namespace": biosample.id_namespace,
                    "biosample_local_id": biosample.local_id,
                    "subject_id_namespace": subject.id_namespace,
                    "subject_local_id": subject.local_id,
                },
            )
        return True

    def _register_subject(self, subject: Subject, submission: str) -> bool:
        if not subject.id_namespace or not subject.local_id:
            self.stats["entities_missing_key"] += 1
            return False
        taxonomy_id = self._term_fk("ncbi_taxonomy", subject.taxonomy)
        project_fk = self._project_fk(
            subject.project_id_namespace, subject.project_local_id, submission
        )
        self._register(
            "subject",
            {
                "id_namespace": subject.id_namespace,
                "local_id": subject.local_id,
                "submission": submission,
                "project_id_namespace": project_fk[0],
                "project_local_id": project_fk[1],
                "persistent_id": subject.persistent_id,
                "creation_time": self._coerce_timestamp(
                    subject.creation_time, "creation_time_unparseable"
                ),
                "granularity": subject.granularity,
                "sex": subject.sex,
                "ethnicity": subject.ethnicity,
                "age_at_enrollment": subject.age_at_enrollment,
                "age_at_sampling": subject.age_at_sampling,
                "taxonomy": taxonomy_id,
            },
        )
        for race in subject.race:
            if not race:
                continue
            self._stage(
                "subject_race",
                {
                    "subject_id_namespace": subject.id_namespace,
                    "subject_local_id": subject.local_id,
                    "race": race,
                },
            )
        if subject.extra is not None and subject.extra.hubmap is not None:
            hubmap = subject.extra.hubmap
            row = {
                "id_namespace": subject.id_namespace,
                "local_id": subject.local_id,
                "age_value": hubmap.age_value,
                "age_unit": hubmap.age_unit,
                "sex": hubmap.sex,
                "race": hubmap.race,
                "body_mass_index_value": hubmap.body_mass_index_value,
                "body_mass_index_unit": hubmap.body_mass_index_unit,
                "cause_of_death": hubmap.cause_of_death,
                "death_event": hubmap.death_event,
                "mechanism_of_injury": hubmap.mechanism_of_injury,
                "medical_history": hubmap.medical_history,
                "social_history": hubmap.social_history,
                "height_value": hubmap.height_value,
                "height_unit": hubmap.height_unit,
                "weight_value": hubmap.weight_value,
                "weight_unit": hubmap.weight_unit,
            }
            if self._any_value(row, ("id_namespace", "local_id")):
                self._register("subject_extra_hubmap", row)
        return True

    # ── finalize ────────────────────────────────────────────────────────

    def _assert_unique_file_keys(self) -> None:
        offenders = self.con.execute(
            """
            SELECT id_namespace, local_id, count(*) AS copies,
                   count(DISTINCT submission) AS submissions
            FROM stage_file
            GROUP BY id_namespace, local_id
            HAVING count(*) > 1
            LIMIT 5
            """
        ).fetchall()
        if offenders:
            detail = "; ".join(
                f"({ns!r}, {lid!r}) x{copies} across {subs} submission(s)"
                for ns, lid, copies, subs in offenders
            )
            raise DumpError(
                f"duplicate file keys in the source (first 5 shown): {detail}"
            )

    def finalize(self) -> DumpReport:
        for table in _STAGED_TABLES:
            self._flush(table)
        self._assert_unique_file_keys()

        # A live connection was handed in, so duckdb resolves at runtime even
        # though the module only imports it for type checking.
        import duckdb

        table = ""
        try:
            for table in _REGISTRY_INSERT_ORDER:
                registry = self.registries[table]
                if registry.rows:
                    self._insert_rows(
                        table, self._columns[table], list(registry.rows.values())
                    )

            for table in _STAGED_FILE_TABLES:
                self.con.execute(
                    f'INSERT INTO "{table}" SELECT * FROM "stage_{table}"'
                )
            for table in _JUNCTION_TABLES:
                self.con.execute(
                    f'INSERT INTO "{table}" SELECT DISTINCT * FROM "stage_{table}"'
                )
        except duckdb.ConstraintException as e:
            raise DumpError(
                f"finalize insert into {table!r} violated a schema constraint "
                f"({e}); the source breaks an assumption the schema declares"
            ) from e
        for table in _STAGED_TABLES:
            self.con.execute(f'DROP TABLE "stage_{table}"')

        if self.conflict_samples:
            self.con.executemany(
                "INSERT INTO dump_conflict VALUES (?, ?, ?, ?, ?)",
                self.conflict_samples,
            )

        rows: dict[str, int] = {}
        for table in _REGISTRY_INSERT_ORDER + _STAGED_TABLES:
            rows[table] = self.con.execute(
                f'SELECT count(*) FROM "{table}"'
            ).fetchone()[0]

        surviving_stubs = len(self.registries["project"].stubs)
        if surviving_stubs:
            self.stats["project_stubs_surviving"] = surviving_stubs
        stats = dict(self.stats)
        stat_rows = [(f"rows_{t}", n) for t, n in sorted(rows.items())]
        stat_rows += [(f"conflicts_{t}", n) for t, n in sorted(self.conflicts.items())]
        stat_rows += sorted(stats.items())
        self.con.executemany("INSERT INTO dump_stat VALUES (?, ?)", stat_rows)

        submissions = self.requested_submissions or sorted(self.seen_submissions)
        self.con.execute(
            "INSERT INTO dump_info VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                SCHEMA_VERSION,
                _utc_naive(datetime.now(timezone.utc)),
                _cfdb_version(),
                _cfdb_commit(),
                self.source_database,
                self.source_collection,
                submissions,
                self.file_count,
            ],
        )

        return DumpReport(
            file_count=self.file_count,
            rows=rows,
            conflicts=dict(self.conflicts),
            stats=stats,
        )


def dump_files(
    docs: Iterable[dict],
    con: duckdb.DuckDBPyConnection,
    *,
    source_database: str,
    source_collection: str = "files",
    submissions: Optional[list[str]] = None,
    conflict_sample_cap: int = 1000,
    batch_size: int = 10_000,
) -> DumpReport:
    """Flatten ``files`` documents into an empty DuckDB database.

    Executes ``schema.sql`` on ``con`` — which must therefore be a fresh
    database — streams ``docs`` through the flattening described in the
    module docstring, and returns a :class:`DumpReport`. Raises
    :class:`DumpError` when the source violates an assumption the schema
    depends on: duplicate file keys, or one key under two submissions.

    The precoercion of string-typed numerics (``size_in_bytes``,
    ``extra_files[].file_size``) writes into the supplied dicts in place
    — immaterial for a pymongo cursor, visible to a caller that keeps
    the dicts.
    """
    dumper = _Dumper(
        con,
        source_database=source_database,
        source_collection=source_collection,
        submissions=submissions,
        conflict_sample_cap=conflict_sample_cap,
        batch_size=batch_size,
    )
    dumper.bootstrap()
    for raw in docs:
        dumper.process(raw)
    return dumper.finalize()
