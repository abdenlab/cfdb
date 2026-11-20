"""HuBMAP files search API client and CFDB transformation service.

Builds CFDB file documents directly from HuBMAP's file-grain search index,
following the precedent ENCODE set in :mod:`cfdb.services.encode`. One index
document is one file; every document in the index is public, so no pruning
pass exists. Dataset-level context the files index does not carry — donor
demographics, pipeline, genome assembly — is prefetched once from the
dataset-level index into an in-memory map keyed by ``dataset_uuid`` and
joined during transformation, so documents land fully populated in a single
insert pass with no post-load enrichment.

API URLs
--------
File search (search_after pagination, one document per file):
  https://search.api.hubmapconsortium.org/v3/files/search

Dataset search (search_after pagination, one document per dataset):
  https://search.api.hubmapconsortium.org/v3/portal/search

Organ code -> UBERON: no request. A checked-in snapshot of the UBKG organs
endpoint, ``HUBMAP_ORGAN_TO_UBERON`` in :mod:`cfdb.services.ontology_mappings`.

Field Mapping (HuBMAP files search API -> CFDB)
-----------------------------------------------

File
~~~~
file_uuid                               -> local_id (HuBMAP issues no file
                                           accessions; accession_id stays null)
rel_path (basename)                     -> filename
size                                    -> size_in_bytes (stored as int64)
md5_checksum                            -> md5
sha256_checksum                         -> sha256
filename (compression suffix stripped)  -> file_format (EDAM-mapped)
filename                                -> compression_format (suffix-derived,
                                           shared with the ENCODE derivation)
assets URL (dataset_uuid + rel_path)    -> access_url
data_access_level                       -> data_access_level
dataset_status                          -> status
dataset workflow_description            -> genome_assembly (regex-derived,
                                           joined from the dataset index)

Enriched File
~~~~~~~~~~~~~
rel_path                                -> extra.hubmap.rel_path
is_data_product ("True"/"False" string) -> extra.hubmap.is_data_product
dataset workflow_description            -> extra.hubmap.genome_assembly

Collection (one per file, from its parent dataset)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dataset_uuid                            -> collections[].local_id
dataset_hubmap_id                       -> collections[].name
portal dataset URL                      -> collections[].persistent_id
dataset_type                            -> collections[].experiment_type
analyte_class                           -> collections[].analyte_class
organs[] (code -> UBERON, static table) -> collections[].anatomy

Enriched Collection (dataset index join)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
pipeline                                -> collections[].extra.hubmap.pipeline
processing                              -> collections[].extra.hubmap.processing
group_name                              -> collections[].extra.hubmap.group_name
visualization                           -> collections[].extra.hubmap.visualization
vitessce-hints                          -> collections[].extra.hubmap.vitessce_hints
metadata                                -> collections[].extra.hubmap.metadata
dataset_status                          -> collections[].extra.hubmap.dataset_status

Subject
~~~~~~~
donors[].uuid                           -> collections[].subjects[].local_id
(HuBMAP is human-only)                  -> collections[].subjects[].taxonomy

Enriched Subject (dataset index join, from donor.mapped_metadata)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
sex                                     -> subjects[].extra.hubmap.sex
race                                    -> subjects[].extra.hubmap.race
age_value                               -> subjects[].extra.hubmap.age_value
age_unit                                -> subjects[].extra.hubmap.age_unit
height_value                            -> subjects[].extra.hubmap.height_value
height_unit                             -> subjects[].extra.hubmap.height_unit
weight_value                            -> subjects[].extra.hubmap.weight_value
weight_unit                             -> subjects[].extra.hubmap.weight_unit
body_mass_index_value                   -> subjects[].extra.hubmap.body_mass_index_value
body_mass_index_unit                    -> subjects[].extra.hubmap.body_mass_index_unit
cause_of_death                          -> subjects[].extra.hubmap.cause_of_death
death_event                             -> subjects[].extra.hubmap.death_event
mechanism_of_injury                     -> subjects[].extra.hubmap.mechanism_of_injury
medical_history                         -> subjects[].extra.hubmap.medical_history
social_history                          -> subjects[].extra.hubmap.social_history

DCC
~~~
Static / config-derived:                   dcc.id, dcc.dcc_name,
                                           dcc.dcc_abbreviation,
                                           dcc.dcc_description,
                                           dcc.contact_email, dcc.contact_name,
                                           dcc.dcc_url, dcc.project_id_namespace,
                                           dcc.project_local_id
"""

import asyncio
import logging
import os
import re
from collections import Counter
from typing import AsyncGenerator, Optional
from urllib.parse import quote

import aiohttp

from cfdb.dcc_registry import get_dcc_config
from cfdb.services.ingest_common import (
    COMPRESSION_SUFFIX_TO_EDAM,
    UNMAPPABLE_COMPRESSION_SUFFIXES,
    derive_compression_format,
    timeout_from_env,
)
from cfdb.services.ontology_mappings import (
    HUBMAP_ORGAN_TO_UBERON,
    get_file_format,
    get_taxonomy,
)

logger = logging.getLogger(__name__)

#: Pause between search requests: at most 10 requests a second.
REQUEST_INTERVAL_SECONDS = 0.1

ASSETS_BASE_URL = "https://assets.hubmapconsortium.org"
PORTAL_DATASET_URL = "https://portal.hubmapconsortium.org/browse/dataset/{uuid}"

#: Environment variable overriding which file extensions are ingested, and
#: the allowlist applied when it is unset. An allowlist rather than a filter
#: retrofitted later: the files index holds 9.9M documents and 98% of them
#: are ``.tif`` microscopy tiles the API cannot serve meaningfully, so
#: ingesting the extension space wholesale would grow the corpus ~11x with
#: dead weight. The default is the genomics, tabular, and imaging-derived
#: table tail of the live distribution, ~52k files.
_FILE_EXTENSIONS_ENV = "HUBMAP_FILE_EXTENSIONS"
_FILE_EXTENSIONS_DEFAULT: tuple[str, ...] = (
    ".h5ad",
    ".h5mu",
    ".h5",
    ".hdf5",
    # ".bcf" is deliberately absent: every .bcf in the index is a CODEX
    # raw-acquisition sidecar (raw/Cyc1_reg1/1.bcf), not binary VCF. Nor
    # does "bcf" map to a format, so an operator who opts them in gets files
    # with no file_format rather than ones labelled as variant calls.
    ".gz",
    ".mtx",
    ".arrow",
    ".parquet",
    ".bed",
    ".narrowpeak",
    ".bam",
    ".sf",
    ".snap",
    ".csv",
    ".tsv",
)


def file_extensions_from_env() -> tuple[str, ...]:
    """Resolve the file-extension allowlist to ingest.

    Reads a comma-separated ``HUBMAP_FILE_EXTENSIONS``, stripping each entry,
    lower-casing it, prefixing a missing leading dot, and dropping blanks —
    so ``h5ad, .MTX`` and ``.h5ad,.mtx`` configure the same thing, and what
    is sent to the search API matches the index's dot-prefixed
    ``file_extension`` values exactly. An unset variable yields the default
    allowlist; a variable set to an empty (or all-blank) value yields an
    empty tuple, on which the sync skips HuBMAP and leaves the existing
    slice in place — the same outcome an empty ``ENCODE_ANNOTATION_TYPES``
    has for ENCODE's annotation phases — and which is warned about for the
    same reason: empty values arrive by accident routinely, and a silently
    skipped load shows up only as an absence.

    Repeats are collapsed, first occurrence winning, so a duplicated token
    cannot widen the server-side ``terms`` filter or reorder the logged
    allowlist.
    """
    raw = os.getenv(_FILE_EXTENSIONS_ENV)
    if raw is None:
        return _FILE_EXTENSIONS_DEFAULT

    entries = (entry.strip().lower() for entry in raw.split(","))
    normalized = (
        entry if entry.startswith(".") else f".{entry}"
        for entry in entries
        if entry
    )
    allowlist = tuple(dict.fromkeys(normalized))

    if not allowlist:
        logger.warning(
            "%s is set but empty: the HuBMAP sync will be skipped and the "
            "existing HuBMAP slice left in place. Unset it entirely to "
            "restore the default allowlist (%s).",
            _FILE_EXTENSIONS_ENV,
            ", ".join(_FILE_EXTENSIONS_DEFAULT),
        )

    return allowlist


_GENOME_ASSEMBLY_RE = re.compile(
    r"\b(GRCh38|hg38|GRCh37|hg19|GRCm39|mm10)\b", re.IGNORECASE
)

# Normalize variant names to canonical assembly identifiers
_ASSEMBLY_ALIASES = {
    "hg38": "GRCh38",
    "grch38": "GRCh38",
    "hg19": "GRCh37",
    "grch37": "GRCh37",
    "mm10": "GRCm38",
    "grcm39": "GRCm39",
}


def extract_genome_assembly(workflow_description: str) -> Optional[str]:
    """Extract genome assembly from a HuBMAP workflow description string."""
    match = _GENOME_ASSEMBLY_RE.search(workflow_description)
    if match:
        raw = match.group(1).lower()
        return _ASSEMBLY_ALIASES.get(raw, raw)
    return None


# Compression suffixes stripped before mapping a filename's extension to a
# file format: the named EDAM-expressible wrappers plus the ones no EDAM term
# covers. Longest first so ``.gzip`` is never half-stripped as ``.gz``'s
# shorter cousin. ``.zip`` is excluded deliberately — a ZIP holds many
# members with many formats, so the name under it describes an archive
# member, not the file.
_STRIPPABLE_COMPRESSION_SUFFIXES: tuple[str, ...] = tuple(
    sorted(
        (
            suffix
            for suffix in (
                *COMPRESSION_SUFFIX_TO_EDAM,
                *UNMAPPABLE_COMPRESSION_SUFFIXES,
            )
            if suffix != ".zip"
        ),
        key=len,
        reverse=True,
    )
)


def derive_file_format(filename: str) -> Optional[dict]:
    """Map a filename to an EDAM file-format term.

    The files index's ``file_extension`` names the outermost suffix, which
    for a ``.fastq.gz`` is the compression wrapper rather than the format —
    so the derivation strips one known compression suffix from the filename
    first, then maps whatever extension remains through the shared
    EDAM table. ``None`` when no extension remains or none maps.
    """
    base = filename.lower().rsplit("/", 1)[-1]
    for suffix in _STRIPPABLE_COMPRESSION_SUFFIXES:
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    if "." not in base:
        return None
    return get_file_format(base.rsplit(".", 1)[-1])


#: Per-request timeout for one search page, in seconds.
_PAGE_TIMEOUT_SECONDS = 60

#: Attempts per search page before the failure is raised, and the base of
#: the linear backoff between them. A walk is ~54 sequential pages behind a
#: deferred clear, so one transient 5xx would otherwise cost the sync after
#: the old slice is gone; re-sending the same ``search_after`` page is
#: idempotent, which is what makes retrying safe.
_PAGE_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2.0

#: Upstream statuses worth retrying. Anything else is a request the server
#: will refuse again, so it is raised at once.
_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})

#: Environment variable bounding the HuBMAP sync's network fetches, and its
#: default. The file stream runs inside the cutover lock, which gates the
#: whole read surface, so the sync gets one deadline -- the reasoning
#: ENCODE's metadata budget records. The budget alone does not keep the
#: sync lock fresh: that lock's staleness counts from the start of the
#: whole multi-DCC run, so the sync clamps this budget to what is left of
#: it (see ``_stale_safe_deadline`` in :mod:`cfdb.services.sync`).
_SYNC_TIMEOUT_ENV = "HUBMAP_SYNC_TIMEOUT_SECONDS"
_SYNC_TIMEOUT_DEFAULT_SECONDS = 1800


def sync_budget_seconds() -> int:
    """Resolve the HuBMAP files-stream budget, in seconds.

    Read per call, like ENCODE's budget, so a malformed value fails the sync
    that reads it rather than the import of this module.
    """
    return timeout_from_env(_SYNC_TIMEOUT_ENV, _SYNC_TIMEOUT_DEFAULT_SECONDS)


class _RetryablePageError(RuntimeError):
    """A page failure that a fresh attempt at the same page may not repeat."""


def _page_hits(data: object, label: str) -> tuple[list[dict], Optional[int]]:
    """Return a search response's hits and exact total, refusing a bad page.

    The total is the index's ``hits.total.value`` when it is exact
    (``relation: eq``, which ``track_total_hits`` guarantees), else None.

    Elasticsearch answers 200 with whatever the healthy shards returned when
    a shard fails or the query times out, and the walk would then resume
    past documents it never received. A body with no hits envelope is
    refused too: read as an empty page it would end the walk, which on the
    first page means clearing the corpus and loading nothing.
    """
    hits = data.get("hits") if isinstance(data, dict) else None
    if not isinstance(hits, dict) or not isinstance(hits.get("hits"), list):
        raise _RetryablePageError(
            f"HuBMAP {label} search API returned a page with no hits envelope"
        )
    if data.get("timed_out"):
        raise _RetryablePageError(
            f"HuBMAP {label} search timed out upstream; the page is partial"
        )
    failed = (data.get("_shards") or {}).get("failed") or 0
    if failed:
        raise _RetryablePageError(
            f"HuBMAP {label} search had {failed} failed shard(s); the page "
            "is partial"
        )
    total = hits.get("total")
    exact = (
        total.get("value")
        if isinstance(total, dict) and total.get("relation") == "eq"
        else None
    )
    return hits["hits"], exact if isinstance(exact, int) else None


async def _search_page(
    session: aiohttp.ClientSession,
    url: str,
    query: dict,
    label: str,
    *,
    deadline: float | None = None,
) -> tuple[list[dict], Optional[int]]:
    """POST one search page and return its hits and exact total, retrying.

    Transport errors, timeouts, retryable statuses, and partial or malformed
    pages are retried up to ``_PAGE_ATTEMPTS`` times with linear backoff;
    the last failure is raised. A non-retryable status raises at once.

    Args:
        session: Open client session.
        url: Search endpoint.
        query: Elasticsearch request body, ``search_after`` included.
        label: Names the index in log and error messages.
        deadline: Event-loop clock reading by which the page must arrive.
            Each attempt's timeout is clamped to what remains, and an
            exhausted deadline raises ``asyncio.TimeoutError`` rather than
            being retried -- aiohttp reads a non-positive total as "no
            timeout", so passing a spent remainder through would turn the
            budget into an unbounded request.
    """
    loop = asyncio.get_running_loop()
    for attempt in range(1, _PAGE_ATTEMPTS + 1):
        timeout = float(_PAGE_TIMEOUT_SECONDS)
        if deadline is not None:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError(
                    f"HuBMAP {label} fetch did not finish within the sync's "
                    f"deadline ({_SYNC_TIMEOUT_ENV}, clamped to the sync "
                    "lock's stale threshold)"
                )
            timeout = min(timeout, remaining)

        try:
            async with session.post(
                url,
                json=query,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as response:
                if response.status != 200:
                    message = (
                        f"HuBMAP {label} search API error: "
                        f"HTTP {response.status}"
                    )
                    if response.status in _RETRYABLE_STATUSES:
                        raise _RetryablePageError(message)
                    raise RuntimeError(message)
                try:
                    data = await response.json()
                except ValueError as e:
                    raise _RetryablePageError(
                        f"HuBMAP {label} search API returned undecodable "
                        f"JSON: {e}"
                    ) from e
            return _page_hits(data, label)
        except (aiohttp.ClientError, asyncio.TimeoutError, _RetryablePageError) as e:
            if attempt == _PAGE_ATTEMPTS:
                logger.error(
                    f"HuBMAP {label} search page failed after {attempt} "
                    f"attempts: {e!r}"
                )
                raise
            logger.warning(
                f"HuBMAP {label} search page failed (attempt {attempt} of "
                f"{_PAGE_ATTEMPTS}): {e!r}; retrying"
            )
            await asyncio.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise AssertionError("unreachable: the last attempt returns or raises")


# Fields to request from the dataset-level index. Only what the files index
# cannot supply: per-dataset processing context, donor demographics, and the
# free-text workflow description the genome assembly is extracted from.
_DATASET_SOURCE_FIELDS = [
    "uuid",
    "pipeline",
    "processing",
    "group_name",
    "visualization",
    "vitessce-hints",
    "metadata",
    "donor.mapped_metadata",
    "donor.uuid",
    "donors.mapped_metadata",
    "donors.uuid",
    "ingest_metadata.workflow_description",
]


async def fetch_dataset_metadata_bulk(
    *, deadline: float | None = None
) -> dict[str, dict]:
    """
    Fetch all HuBMAP dataset metadata from the dataset-level search index.

    Uses Elasticsearch ``search_after`` pagination to iterate all datasets in
    batches of 1000. Returns a dict keyed by dataset ``uuid`` so the file
    transformation can join on the ``dataset_uuid`` every files-index
    document carries.

    Each page is retried on transient failure (see ``_search_page``); a
    failure that outlasts the retries raises rather than returning the
    partial map, since a silently partial map would mean silently
    under-enriched documents.

    Args:
        deadline: Event-loop clock reading by which the whole fetch must
            finish; each page's timeout is clamped to what remains. None
            leaves only the per-page timeout.

    Returns:
        {dataset_uuid: {pipeline, processing, group_name, visualization,
                        vitessce_hints, metadata, donor_uuid, donor_metadata,
                        donors: {donor_uuid: mapped_metadata},
                        genome_assembly}}
    """
    config = get_dcc_config("hubmap")
    api_base = config["api_base"]
    search_url = f"{api_base}/portal/search"

    results: dict[str, dict] = {}
    page_size = 1000
    search_after: Optional[list] = None

    async with aiohttp.ClientSession() as session:
        while True:
            query: dict = {
                "query": {"term": {"entity_type.keyword": "Dataset"}},
                "size": page_size,
                "_source": _DATASET_SOURCE_FIELDS,
                "sort": [{"_id": "asc"}],
            }

            if search_after is not None:
                query["search_after"] = search_after

            hits, _ = await _search_page(
                session, search_url, query, "dataset", deadline=deadline
            )

            await asyncio.sleep(REQUEST_INTERVAL_SECONDS)

            if not hits:
                break

            for hit in hits:
                src = hit.get("_source", {})
                uuid = src.get("uuid")
                if not uuid:
                    continue

                entry: dict = {}

                for key in ("pipeline", "processing", "group_name", "visualization"):
                    val = src.get(key)
                    if val is not None:
                        entry[key] = val

                vitessce_hints = src.get("vitessce-hints")
                if vitessce_hints:
                    entry["vitessce_hints"] = vitessce_hints

                # Full assay-specific metadata dict
                metadata = src.get("metadata")
                if metadata:
                    entry["metadata"] = metadata

                # Donor demographics. ``donor`` names one donor; a dataset
                # pooled from several also lists every one under ``donors``,
                # so both feed the per-donor map ``build_donor_lookup``
                # merges -- reading ``donor`` alone covers the others only
                # when each happens to be some other dataset's ``donor``.
                donor = src.get("donor") or {}
                donor_metadata = donor.get("mapped_metadata")
                if donor_metadata:
                    entry["donor_metadata"] = donor_metadata
                donor_uuid = donor.get("uuid")
                if donor_uuid:
                    entry["donor_uuid"] = donor_uuid
                donors = {
                    d["uuid"]: d["mapped_metadata"]
                    for d in [donor, *(src.get("donors") or [])]
                    if isinstance(d, dict)
                    and d.get("uuid")
                    and d.get("mapped_metadata")
                }
                if donors:
                    entry["donors"] = donors

                # Genome assembly from workflow description
                ingest = src.get("ingest_metadata") or {}
                workflow_desc = ingest.get("workflow_description")
                if workflow_desc:
                    assembly = extract_genome_assembly(workflow_desc)
                    if assembly:
                        entry["genome_assembly"] = assembly

                results[uuid] = entry

            # Prepare next page
            search_after = hits[-1].get("sort")
            if not search_after:
                break

            if len(results) % 5000 < page_size:
                logger.info(
                    f"HuBMAP dataset fetch: {len(results)} datasets fetched so far"
                )

    logger.info(f"HuBMAP dataset fetch: {len(results)} total datasets fetched")
    return results


def build_donor_lookup(dataset_metadata: dict[str, dict]) -> dict[str, dict]:
    """Merge every dataset's donors into one demographics map by donor uuid.

    Each dataset entry from :func:`fetch_dataset_metadata_bulk` carries its
    donors -- the singular ``donor`` and the plural ``donors`` alike -- so a
    donor of a pooled dataset joins even when no dataset names it singly.
    The first dataset to name a donor wins; a donor's demographics are the
    same wherever it appears.
    """
    lookup: dict[str, dict] = {}
    for meta in dataset_metadata.values():
        for donor_uuid, donor_metadata in (meta.get("donors") or {}).items():
            lookup.setdefault(donor_uuid, donor_metadata)
        if meta.get("donor_uuid") and meta.get("donor_metadata"):
            lookup.setdefault(meta["donor_uuid"], meta["donor_metadata"])
    return lookup


# Fields to request from the files index — the whole per-file surface the
# transformation maps.
_FILE_SOURCE_FIELDS = [
    "file_uuid",
    "rel_path",
    "file_extension",
    "size",
    "md5_checksum",
    "sha256_checksum",
    "data_access_level",
    "is_data_product",
    "dataset_uuid",
    "dataset_hubmap_id",
    "dataset_type",
    "dataset_status",
    "analyte_class",
    "donors",
    "organs",
]


async def fetch_file_metadata(
    extensions: tuple[str, ...],
    *,
    deadline: float | None = None,
) -> AsyncGenerator[dict, None]:
    """
    Stream file documents from the HuBMAP files search index.

    Uses ``search_after`` pagination on ``file_uuid.keyword`` in batches of
    1000. When ``extensions`` is non-empty, the allowlist is applied
    server-side as a ``terms`` filter on ``file_extension.keyword``, so the
    excluded 9.7M microscopy tiles never cross the wire. The query also
    filters on ``data_access_level: public``: the index holds only public
    files today, which is what retired the C2M2 path's pruning pass, and
    the filter makes that an invariant of the load rather than an
    assumption about a beta index. An empty allowlist yields nothing — the
    explicit-empty configuration documented on
    :func:`file_extensions_from_env`.

    Each page is retried on transient failure, and a partial page (failed
    shards, an upstream timeout, a missing hits envelope) is refused rather
    than walked past; see ``_search_page``. The first page asks for an
    exact total, and a walk that ends with fewer documents than it raises.
    A failure that outlasts the retries still raises mid-stream, and behind
    the loader's deferred clear that leaves a partial slice -- loudly
    rather than silently, but only a shadow-collection swap would make it
    atomic.

    Args:
        extensions: Dot-prefixed, lower-case extension allowlist.
        deadline: Event-loop clock reading by which the whole stream must
            finish; each page's timeout is clamped to what remains. None
            leaves only the per-page timeout.

    Yields:
        The ``_source`` dict of each matching file document.
    """
    if not extensions:
        return

    config = get_dcc_config("hubmap")
    api_base = config["api_base"]
    search_url = f"{api_base}/files/search"

    page_size = 1000
    search_after: Optional[list] = None
    yielded = 0
    expected: Optional[int] = None

    async with aiohttp.ClientSession() as session:
        while True:
            query: dict = {
                "query": {
                    "bool": {
                        "filter": [
                            {"terms": {"file_extension.keyword": list(extensions)}},
                            {"term": {"data_access_level.keyword": "public"}},
                        ]
                    }
                },
                "size": page_size,
                "_source": _FILE_SOURCE_FIELDS,
                "sort": [{"file_uuid.keyword": "asc"}],
            }

            if search_after is not None:
                query["search_after"] = search_after
            else:
                # Exact on the first page only: the count is what the walk
                # is checked against when it ends.
                query["track_total_hits"] = True

            hits, total = await _search_page(
                session, search_url, query, "files", deadline=deadline
            )
            if search_after is None:
                expected = total

            await asyncio.sleep(REQUEST_INTERVAL_SECONDS)

            if not hits:
                break

            for hit in hits:
                yield hit.get("_source", {})
                yielded += 1

            if yielded % 10000 < page_size:
                logger.info(f"HuBMAP file fetch: {yielded} files fetched so far")

            # Prepare next page
            search_after = hits[-1].get("sort")
            if not search_after:
                break

    if expected is not None and yielded < expected:
        raise RuntimeError(
            f"HuBMAP files walk ended short: {yielded} of the {expected} "
            "files the index reported on its first page"
        )
    logger.info(f"HuBMAP file fetch: {yielded} total files fetched")


# Scalar demographics HuBMAP returns as single-element lists; unwrapped to
# the scalar on the way in.
_DONOR_SCALAR_FIELDS = (
    "age_value",
    "age_unit",
    "sex",
    "race",
    "body_mass_index_value",
    "body_mass_index_unit",
    "cause_of_death",
    "death_event",
    "mechanism_of_injury",
    "height_value",
    "height_unit",
    "weight_value",
    "weight_unit",
)

# Genuinely multi-valued demographics, kept as lists.
_DONOR_LIST_FIELDS = ("medical_history", "social_history")


def _donor_extra(donor_metadata: dict) -> dict:
    """Shape one donor's ``mapped_metadata`` into ``subjects[].extra.hubmap``."""
    extra: dict = {}
    for key in _DONOR_SCALAR_FIELDS:
        val = donor_metadata.get(key)
        if val is None:
            continue
        if isinstance(val, list) and len(val) == 1:
            val = val[0]
        extra[key] = val

    for key in _DONOR_LIST_FIELDS:
        val = donor_metadata.get(key)
        if isinstance(val, list) and val:
            extra[key] = val

    return extra


def _parse_flag(value: object) -> Optional[bool]:
    """Parse one of the files index's stringly-typed booleans.

    The index serializes ``is_data_product`` as the strings ``"True"`` and
    ``"False"``, so a plain truthiness test would read ``"False"`` as true.
    Anything that is neither spelling yields ``None`` -- an unrecognized
    value is unknown, and reporting it as ``False`` would fabricate a fact.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return {"true": True, "false": False}.get(value.strip().lower())
    return None


def _dcc_fields() -> dict:
    """The static HuBMAP DCC fields, shared by the inline file-document copy
    and the ``dcc`` collection record."""
    config = get_dcc_config("hubmap")
    id_namespace = config["id_namespace"]

    return {
        "id": "cfde_registry_dcc:hubmap",
        "dcc_name": "HuBMAP",
        "dcc_abbreviation": "HuBMAP",
        "dcc_description": (
            "The Human BioMolecular Atlas Program (HuBMAP) is an NIH Common "
            "Fund initiative developing an open, global atlas of the human "
            "body at the cellular level."
        ),
        "contact_email": "help@hubmapconsortium.org",
        "contact_name": "HuBMAP Help Desk",
        "dcc_url": "https://hubmapconsortium.org",
        "project_id_namespace": id_namespace,
        "project_local_id": "HuBMAP",
    }


def build_hubmap_dcc_record() -> dict:
    """
    Build DCC collection record for HuBMAP.

    Returns:
        DCC document for insertion into dcc collection
    """
    return {"submission": "hubmap", **_dcc_fields()}


def transform_file_to_cfdb(
    src: dict,
    *,
    dataset_metadata: dict[str, dict],
    donor_lookup: dict[str, dict],
    organ_map: dict[str, dict] | None = None,
    stats: Counter | None = None,
) -> Optional[dict]:
    """
    Transform one files-index document into a CFDB file document.

    Args:
        src: The ``_source`` dict of a files-index document, as yielded by
            :func:`fetch_file_metadata`.
        dataset_metadata: Dataset-level context keyed by dataset uuid, from
            :func:`fetch_dataset_metadata_bulk`. A missing dataset degrades
            the document (no demographics, pipeline, or assembly) rather
            than dropping it.
        donor_lookup: Donor demographics keyed by donor uuid, built from
            ``dataset_metadata`` by the caller.
        organ_map: Organ-code → UBERON term mapping; defaults to the
            checked-in ``HUBMAP_ORGAN_TO_UBERON``. An unmapped code skips
            that anatomy term rather than fabricating an id.
        stats: Optional counter the transform reports into — per-extension
            document tallies, skip reasons, unmapped organ codes, and
            datasets or donors absent from the join maps — so the sync can
            log one distribution instead of a line per file.

    Returns:
        CFDB-compatible dict for insertion into the files collection, or
        None for a document missing the fields that make it addressable
        (``file_uuid``, ``rel_path``, ``dataset_uuid``).
    """
    config = get_dcc_config("hubmap")
    id_namespace = config["id_namespace"]

    file_uuid = src.get("file_uuid")
    rel_path = src.get("rel_path")
    dataset_uuid = src.get("dataset_uuid")
    if not file_uuid or not rel_path or not dataset_uuid:
        if stats is not None:
            stats["skipped:incomplete"] += 1
        logger.warning(
            "Skipping HuBMAP file document missing file_uuid, rel_path, or "
            f"dataset_uuid (file_uuid={file_uuid!r})"
        )
        return None

    filename = rel_path.rsplit("/", 1)[-1]

    try:
        size_in_bytes = int(src["size"])
    except (KeyError, TypeError, ValueError):
        size_in_bytes = None

    if organ_map is None:
        organ_map = HUBMAP_ORGAN_TO_UBERON

    dataset_meta = dataset_metadata.get(dataset_uuid)
    if dataset_meta is None:
        if stats is not None:
            stats["unjoined_dataset"] += 1
        dataset_meta = {}

    doc: dict = {
        "submission": "hubmap",
        "id_namespace": id_namespace,
        "local_id": file_uuid,
        # HuBMAP issues no file accessions (#102); explicit so the field is
        # queryably null rather than merely absent.
        "accession_id": None,
        "filename": filename,
        "size_in_bytes": size_in_bytes,
        "md5": src.get("md5_checksum") or None,
        "sha256": src.get("sha256_checksum") or None,
        "access_url": (
            f"{ASSETS_BASE_URL}/{dataset_uuid}/{quote(rel_path, safe='/')}"
        ),
        "status": src.get("dataset_status"),
        "data_access_level": src.get("data_access_level"),
    }

    file_format = derive_file_format(filename)
    if file_format:
        doc["file_format"] = file_format

    # Same absent-when-undetermined contract as the ENCODE derivation; see
    # derive_compression_format.
    compression_format = derive_compression_format(filename)
    if compression_format is not None:
        doc["compression_format"] = compression_format

    extra_hubmap: dict = {"rel_path": rel_path}

    is_data_product = _parse_flag(src.get("is_data_product"))
    if is_data_product is not None:
        extra_hubmap["is_data_product"] = is_data_product

    genome_assembly = dataset_meta.get("genome_assembly")
    if genome_assembly:
        doc["genome_assembly"] = genome_assembly
        extra_hubmap["genome_assembly"] = genome_assembly

    doc["extra"] = {"hubmap": extra_hubmap}

    # --- Subjects, from the file's donors joined to dataset demographics ---
    taxonomy = get_taxonomy("Homo sapiens")
    subjects = []
    # The index repeats a donor once per contributing tissue block -- one
    # live file lists the same uuid 26 times -- so dedupe, as for organs.
    seen_donors: set[str] = set()
    for donor in src.get("donors") or []:
        donor_uuid = (donor or {}).get("uuid")
        if not donor_uuid or donor_uuid in seen_donors:
            continue
        seen_donors.add(donor_uuid)
        subject: dict = {
            "id_namespace": id_namespace,
            "local_id": donor_uuid,
            "project_id_namespace": id_namespace,
            "project_local_id": "HuBMAP",
        }
        if taxonomy:
            subject["taxonomy"] = taxonomy
        donor_metadata = donor_lookup.get(donor_uuid)
        if not donor_metadata and stats is not None:
            stats["unjoined_donor"] += 1
        if donor_metadata:
            donor_extra = _donor_extra(donor_metadata)
            if donor_extra:
                subject["extra"] = {"hubmap": donor_extra}
        subjects.append(subject)

    # --- Anatomy, from the file's organs through the organ table ---
    anatomy = []
    seen_codes: set[str] = set()
    for organ in src.get("organs") or []:
        code = (organ or {}).get("code")
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        term = organ_map.get(code)
        if not term:
            if stats is not None:
                stats[f"unmapped_organ:{code}"] += 1
            continue
        # The table's UBERON label, not the file's own label: labels drift
        # between files for the same code ("Main Bronchus (Right)" vs
        # "Bronchus (Right)"), which would give one id two names.
        anatomy.append({"id": term["id"], "name": term["name"]})

    # --- Collection, one per file: its parent dataset ---
    collection: dict = {
        "id_namespace": id_namespace,
        "local_id": dataset_uuid,
        "name": src.get("dataset_hubmap_id") or dataset_uuid,
        "persistent_id": PORTAL_DATASET_URL.format(uuid=dataset_uuid),
        "biosamples": [],
        "subjects": subjects,
    }
    if anatomy:
        collection["anatomy"] = anatomy

    dataset_type = src.get("dataset_type")
    if dataset_type:
        collection["experiment_type"] = dataset_type
    analyte_class = src.get("analyte_class")
    if analyte_class:
        collection["analyte_class"] = analyte_class

    collection_extra: dict = {}
    for key in (
        "pipeline",
        "processing",
        "group_name",
        "visualization",
        "vitessce_hints",
        "metadata",
    ):
        val = dataset_meta.get(key)
        if val is not None:
            collection_extra[key] = val
    dataset_status = src.get("dataset_status")
    if dataset_status:
        collection_extra["dataset_status"] = dataset_status
    if collection_extra:
        collection["extra"] = {"hubmap": collection_extra}

    doc["collections"] = [collection]

    doc["dcc"] = _dcc_fields()

    if stats is not None:
        stats[f"extension:{src.get('file_extension')}"] += 1

    return doc
