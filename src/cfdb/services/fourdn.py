"""4DN Search API client for bulk file metadata enrichment.

Fetches file, experiment, and biosource metadata from the 4DN Search API
to enrich C2M2-materialized documents. Two enrichment passes run during
sync: collection enrichment (pre-materialization) and file enrichment
(post-materialization).

API URLs
--------
File metadata:
  https://data.4dnucleome.org/search/?type=FileProcessed
  https://data.4dnucleome.org/search/?type=FileFastq
Experiment metadata:
  https://data.4dnucleome.org/search/?type=ExperimentHiC
  https://data.4dnucleome.org/search/?type=ExperimentSeq
  https://data.4dnucleome.org/search/?type=ExperimentDamid
  https://data.4dnucleome.org/search/?type=ExperimentChiapet
Biosource tiers:
  https://data.4dnucleome.org/search/?type=Biosource&cell_line_tier=Tier+1
  https://data.4dnucleome.org/search/?type=Biosource&cell_line_tier=Tier+2

Entity Matching
---------------
File          persistent_id contains 4DNF[A-Z0-9]+ accession
Collection    persistent_id contains 4DNE[A-Z][A-Z0-9]+ accession

Both patterns are matched case-insensitively and the extractors return the
accession already case-folded, so the value that keys the Search API round
trip is the same one stored in ``accession_id``.

Accession Stamping (persistent_id → CFDB)
-----------------------------------------
Both run *pre*-materialization, against the raw C2M2 collections, so the
materializer carries the values into ``files`` on every rebuild. Writing
them post-materialization instead would leave them to be erased by any
standalone ``make materialize-dcc`` / ``make materialize-files``.

file.persistent_id 4DNF* accession       → file.accession_id (case-folded)
collection.persistent_id 4DNE* accession → collection.accession_id
                                           (case-folded)

Field Mapping (4DN API → CFDB)
-------------------------------

File (post-materialization)
~~~~~~~~~~~~~~~~~~~~~~~~~~~
genome_assembly                         → genome_assembly
file_type                               → output_type
file_type_detailed                      → output_type_detail
track_and_facet_info.condition          → condition
track_and_facet_info.assay_info         → assay_info
track_and_facet_info.replicate_info     → biological_replicates (parsed),
                                          technical_replicates (parsed)

Enriched File
~~~~~~~~~~~~~
track_and_facet_info.replicate_info     → extra.replicate_info
track_and_facet_info.biosource_name     → extra.fourdn.biosource_name
track_and_facet_info.dataset            → extra.fourdn.dataset
extra_files[]                           → extra.fourdn.extra_files
  .href                                   .href
  .md5sum                                 .md5sum
  .file_size                              .file_size
  .file_format                            .file_format
Biosource.cell_line_tier (derived)      → extra.fourdn.cell_line_tier

Collection (pre-materialization)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
lab.display_title                       → collections[].lab
experiment_type.display_title           → collections[].experiment_type

Enriched Collection
~~~~~~~~~~~~~~~~~~~
display_title                           → collections[].extra.fourdn.display_title
digestion_enzyme.display_title          → collections[].extra.fourdn.digestion_enzyme
targeted_factor[].display_title         → collections[].extra.fourdn.targeted_factor
crosslinking_method                     → collections[].extra.fourdn.crosslinking_method
crosslinking_temperature                → collections[].extra.fourdn.crosslinking_temperature
crosslinking_time                       → collections[].extra.fourdn.crosslinking_time
ligation_temperature                    → collections[].extra.fourdn.ligation_temperature
ligation_volume                         → collections[].extra.fourdn.ligation_volume
ligation_time                           → collections[].extra.fourdn.ligation_time
digestion_temperature                   → collections[].extra.fourdn.digestion_temperature
digestion_time                          → collections[].extra.fourdn.digestion_time
tagging_method                          → collections[].extra.fourdn.tagging_method
fragmentation_method                    → collections[].extra.fourdn.fragmentation_method
biotin_removed                          → collections[].extra.fourdn.biotin_removed
library_prep_kit                        → collections[].extra.fourdn.library_prep_kit
average_fragment_size                   → collections[].extra.fourdn.average_fragment_size
fragment_size_range                     → collections[].extra.fourdn.fragment_size_range
status                                  → collections[].extra.fourdn.status
date_created                            → collections[].extra.fourdn.date_created
"""

import asyncio
import logging
import re
from collections.abc import Iterable
from typing import Optional

import aiohttp

from cfdb.accessions import normalize_accession
from cfdb.dcc_registry import get_dcc_config
from cfdb.models import (
    NUMERIC_PROTOCOL_FIELDS,
    coerce_4dn_cv_token,
    coerce_scalar_to_str,
)

logger = logging.getLogger(__name__)

# Rate limit: max 10 requests/second → 100ms between requests
_REQUEST_INTERVAL = 0.1

# Accessions per Search-API request when fetching file metadata by accession.
# Kept well under the Fourfront 10,000-row result-window cap (and short
# enough to keep the request URL within limits) so each batch returns every
# requested file rather than a truncated deep-pagination window.
_FILE_METADATA_BATCH_SIZE = 100

# 4DN accession pattern: 4DNF followed by alphanumeric characters.
#
# Case-insensitive deliberately. 4DN publishes accessions upper-cased and
# every one of the 53,697 files currently in the corpus is, but an
# upper-case-only pattern degrades badly rather than simply missing: on a
# mixed-case value it matches the upper-case prefix and returns a
# *truncated* accession (``4DNFImcjxzkh`` -> ``4DNFI``), which is a
# plausible-looking wrong answer rather than a None the callers already
# count and log. Worse, every such value truncates to the same short
# prefix, so a handful of mixed-case rows would collide onto one accession.
#
# The extractors below therefore fold what they match, making the canonical
# accession the only value any caller can obtain. Matching leniently while
# returning the raw match would have moved the failure rather than removed
# it: the extracted value is also the key for the Search API round trip,
# and the portal answers with its own upper-case form, so a mixed-case
# match would join against nothing and that file would silently lose all
# its enrichment -- while still carrying a correct accession_id, and
# without being counted in the unparseable warning that is the operator's
# only signal.
_ACCESSION_RE = re.compile(r"4DNF[A-Z0-9]+", re.IGNORECASE)

# 4DN experiment/experiment set accession pattern: 4DNEX* or 4DNES*.
# Case-insensitive for the same reason as above.
_EXPERIMENT_ACCESSION_RE = re.compile(r"4DNE[A-Z][A-Z0-9]+", re.IGNORECASE)


def extract_accession(persistent_id: str) -> Optional[str]:
    """
    Extract 4DN file accession from a persistent ID URL.

    Handles format: https://data.4dnucleome.org/files-processed/4DNFI1234ABC/@@download/4DNFI1234ABC.mcool
    or: https://data.4dnucleome.org/4DNFI1234ABC

    Returns the accession case-folded to its canonical form (e.g.,
    "4DNFI1234ABC") or None. Folded here rather than at each call site so
    the one value every caller holds is the one both the stored field and
    the Search API are keyed on.
    """
    if not persistent_id:
        return None
    match = _ACCESSION_RE.search(persistent_id)
    return normalize_accession(match.group(0)) if match else None


def extract_experiment_accession(persistent_id: str) -> Optional[str]:
    """
    Extract 4DN experiment or experiment set accession from a persistent ID URL.

    Handles accessions starting with 4DNEX (experiments) or 4DNES (experiment sets).

    Returns the accession case-folded to its canonical form (e.g.,
    "4DNEXH4ZUIH6") or None, for the same reason as
    :func:`extract_accession`.
    """
    if not persistent_id:
        return None
    match = _EXPERIMENT_ACCESSION_RE.search(persistent_id)
    return normalize_accession(match.group(0)) if match else None


def parse_extra_files(extra_files_raw: list) -> list[dict]:
    """Normalize 4DN ``extra_files`` entries for persistence.

    Copies the ``href`` / ``md5sum`` / ``file_size`` / ``file_format``
    fields from each raw entry, dropping any that are absent. The 4DN
    portal API returns ``file_format`` as an embedded CV object; store
    just its token (via :func:`coerce_4dn_cv_token`) so persisted
    documents match the ``ExtraFile.file_format`` type and the ``/index``
    sidecar normalization. Entries that yield no fields are dropped.
    """
    parsed_files: list[dict] = []
    for ef in extra_files_raw:
        parsed: dict = {}
        for k in ("href", "md5sum", "file_size", "file_format"):
            v = ef.get(k)
            if k == "file_format":
                v = coerce_4dn_cv_token(v)
            if v is not None:
                parsed[k] = v
        if parsed:
            parsed_files.append(parsed)
    return parsed_files


async def fetch_file_metadata_bulk(accessions: Iterable[str]) -> dict[str, dict]:
    """
    Fetch file metadata from the 4DN Search API for the given accessions.

    Queries the Search API filtered by accession in bounded batches rather
    than deep-paginating every 4DN file. The Fourfront/Elasticsearch Search
    API caps every result window at 10,000 rows (a ``from``-paginated scan
    silently stops there and reports ``total`` clamped to 10,000), so the
    old full scan retrieved only the first ~10k of each file type and left
    the tens of thousands of remaining files un-enriched. Filtering each
    query by a batch of accessions keeps its result set far under the
    window, so every requested file is fetched regardless of corpus size.

    A single ``type=File`` query covers both ``FileProcessed`` and
    ``FileFastq`` (and any other file subtype).

    Args:
        accessions: 4DN file accessions (e.g. ``4DNF...``) to fetch metadata
            for — typically the accessions of the materialized 4DN files.

    Returns:
        {accession: {genome_assembly, file_type, file_type_detailed, condition,
                      biosource_name, dataset, experiment_type, assay_info,
                      replicate_info, extra_files}}
    """
    config = get_dcc_config("4dn")
    api_base = config["api_base"]
    results: dict[str, dict] = {}

    # Dedupe and order for deterministic batching.
    unique = sorted({acc for acc in accessions if acc})
    if not unique:
        return results

    field_params = (
        "&field=accession"
        "&field=genome_assembly"
        "&field=file_type"
        "&field=file_type_detailed"
        "&field=track_and_facet_info"
        "&field=extra_files"
    )

    failed_batches = 0
    async with aiohttp.ClientSession() as session:
        for start in range(0, len(unique), _FILE_METADATA_BATCH_SIZE):
            batch = unique[start : start + _FILE_METADATA_BATCH_SIZE]
            acc_params = "".join(f"&accession={acc}" for acc in batch)
            url = (
                f"{api_base}/search/"
                f"?type=File"
                f"{acc_params}"
                f"{field_params}"
                f"&limit={len(batch)}"
                f"&format=json"
            )

            try:
                async with session.get(
                    url,
                    headers={"Accept": "application/json", "User-Agent": "cfdb/1.0"},
                    timeout=aiohttp.ClientTimeout(total=60),
                ) as response:
                    if response.status != 200:
                        # Skip only this batch — never silently truncate the
                        # whole fetch (the deep-pagination bug this replaces).
                        logger.warning(
                            "4DN Search API error for accession batch "
                            f"({len(batch)} accessions): HTTP {response.status}"
                        )
                        failed_batches += 1
                        continue

                    data = await response.json()

            except aiohttp.ClientError as e:
                logger.warning(
                    "4DN Search API network error for accession batch "
                    f"({len(batch)} accessions): {e}"
                )
                failed_batches += 1
                continue

            await asyncio.sleep(_REQUEST_INTERVAL)

            for item in data.get("@graph", []):
                accession = item.get("accession")
                if not accession:
                    continue

                entry: dict = {}

                # Direct fields
                genome_assembly = item.get("genome_assembly")
                if genome_assembly:
                    entry["genome_assembly"] = genome_assembly

                file_type_val = item.get("file_type")
                if file_type_val:
                    entry["file_type"] = file_type_val

                file_type_detailed = item.get("file_type_detailed")
                if file_type_detailed:
                    entry["file_type_detailed"] = file_type_detailed

                # Fields from track_and_facet_info
                track_info = item.get("track_and_facet_info", {})
                if track_info:
                    for key in (
                        "condition",
                        "biosource_name",
                        "dataset",
                        "experiment_type",
                        "assay_info",
                        "replicate_info",
                    ):
                        val = track_info.get(key)
                        if val:
                            entry[key] = val

                # Extra files (index files like .px2, .bai)
                extra_files = parse_extra_files(item.get("extra_files", []))
                if extra_files:
                    entry["extra_files"] = extra_files

                if entry:
                    results[accession] = entry

    logger.info(
        f"4DN API: fetched metadata for {len(results)}/{len(unique)} "
        f"requested files ({failed_batches} batch(es) failed)"
    )
    return results


async def fetch_biosource_tiers() -> dict[str, str]:
    """
    Fetch biosource tier classifications from the 4DN Search API.

    Queries Tier 1 and Tier 2 biosources (a small set of ~17 classified cell lines).

    Returns:
        {biosource_display_title: tier_string} e.g., {"GM12878": "Tier 1"}
    """
    config = get_dcc_config("4dn")
    api_base = config["api_base"]
    results: dict[str, str] = {}

    tiers = ["Tier+1", "Tier+2"]

    async with aiohttp.ClientSession() as session:
        for tier in tiers:
            tier_label = tier.replace("+", " ")
            url = (
                f"{api_base}/search/"
                f"?type=Biosource"
                f"&field=cell_line_tier"
                f"&field=display_title"
                f"&cell_line_tier={tier}"
                f"&limit=100"
                f"&format=json"
            )

            try:
                async with session.get(
                    url,
                    headers={"Accept": "application/json", "User-Agent": "cfdb/1.0"},
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    if response.status != 200:
                        logger.warning(
                            f"4DN Biosource API error for {tier_label}: HTTP {response.status}"
                        )
                        continue

                    data = await response.json()

            except aiohttp.ClientError as e:
                logger.warning(f"4DN Biosource API network error: {e}")
                continue

            await asyncio.sleep(_REQUEST_INTERVAL)

            for item in data.get("@graph", []):
                title = item.get("display_title")
                cell_line_tier = item.get("cell_line_tier")
                if title and cell_line_tier:
                    results[title] = cell_line_tier

            logger.info(f"4DN API: {len(results)} biosource tier entries for {tier_label}")

    logger.info(f"4DN API: {len(results)} total biosource tier entries fetched")
    return results


# Fields to request from experiment types (union — absent fields silently omitted per type)
_EXPERIMENT_FIELDS = [
    "accession",
    "display_title",
    "experiment_type",
    "targeted_factor",
    "digestion_enzyme",
    "crosslinking_method",
    "crosslinking_temperature",
    "crosslinking_time",
    "ligation_temperature",
    "ligation_volume",
    "ligation_time",
    "digestion_temperature",
    "digestion_time",
    "tagging_method",
    "fragmentation_method",
    "biotin_removed",
    "library_prep_kit",
    "average_fragment_size",
    "fragment_size_range",
    "lab",
    "status",
    "date_created",
]

# Fields that are objects with display_title to extract
_DISPLAY_TITLE_FIELDS = {"experiment_type", "digestion_enzyme", "lab"}

# Protocol fields the 4DN API sends as JSON numbers but the model declares
# Optional[str]; stringify them at parse time so persisted data is clean at
# rest (mirrors the EnrichedFourdnCollection read validator). Wrapped in a set
# for O(1) membership checks; the canonical list lives in cfdb.models.
_NUMERIC_PROTOCOL_FIELDS = set(NUMERIC_PROTOCOL_FIELDS)

_EXPERIMENT_TYPES = [
    "ExperimentHiC",
    "ExperimentSeq",
    "ExperimentDamid",
    "ExperimentChiapet",
]


def parse_experiment_metadata(item: dict) -> dict:
    """Normalize a single 4DN experiment ``@graph`` item for persistence.

    Extracts the display-title object fields, the ``targeted_factor`` BioFeature
    titles, the experiment ``display_title``, and the scalar protocol fields,
    dropping any that are absent or empty. The numeric protocol fields (e.g.
    ``crosslinking_temperature``) are returned by the 4DN API as JSON numbers
    but the model declares them ``Optional[str]``; stringify them via
    :func:`coerce_scalar_to_str` so persisted documents match the
    ``EnrichedFourdnCollection`` type. Returns the built entry (empty when the
    item yields no fields).
    """
    entry: dict = {}

    # Extract display_title from object fields
    for field_name in _DISPLAY_TITLE_FIELDS:
        obj = item.get(field_name)
        if isinstance(obj, dict):
            title = obj.get("display_title")
            if title:
                entry[field_name] = title

    # Extract targeted_factor: array of BioFeature objects
    targeted_factor_raw = item.get("targeted_factor")
    if isinstance(targeted_factor_raw, list) and targeted_factor_raw:
        titles = [
            tf.get("display_title")
            for tf in targeted_factor_raw
            if isinstance(tf, dict) and tf.get("display_title")
        ]
        if titles:
            entry["targeted_factor"] = titles

    # Extract display_title as experiment name
    display_title = item.get("display_title")
    if display_title:
        entry["display_title"] = display_title

    # Extract scalar fields, stringifying the numeric protocol fields
    for field_name in (
        "crosslinking_method",
        "crosslinking_temperature",
        "crosslinking_time",
        "ligation_temperature",
        "ligation_volume",
        "ligation_time",
        "digestion_temperature",
        "digestion_time",
        "tagging_method",
        "fragmentation_method",
        "biotin_removed",
        "library_prep_kit",
        "average_fragment_size",
        "fragment_size_range",
        "status",
        "date_created",
    ):
        val = item.get(field_name)
        if val is not None and val != "":
            if field_name in _NUMERIC_PROTOCOL_FIELDS:
                val = coerce_scalar_to_str(val)
            entry[field_name] = val

    return entry


async def fetch_experiment_metadata_bulk() -> dict[str, dict]:
    """
    Fetch experiment metadata from the 4DN Search API.

    Queries ExperimentHiC, ExperimentSeq, ExperimentDamid, and ExperimentChiapet
    types and extracts structured metadata including targeted_factor, experiment_type,
    digestion_enzyme, and protocol parameters.

    Returns:
        {accession: {display_title, experiment_type, targeted_factor, ...}}
    """
    config = get_dcc_config("4dn")
    api_base = config["api_base"]
    results: dict[str, dict] = {}

    field_params = "".join(f"&field={f}" for f in _EXPERIMENT_FIELDS)

    async with aiohttp.ClientSession() as session:
        for exp_type in _EXPERIMENT_TYPES:
            offset = 0
            limit = 1000

            while True:
                url = (
                    f"{api_base}/search/"
                    f"?type={exp_type}"
                    f"{field_params}"
                    f"&limit={limit}"
                    f"&from={offset}"
                    f"&format=json"
                )

                try:
                    async with session.get(
                        url,
                        headers={"Accept": "application/json", "User-Agent": "cfdb/1.0"},
                        timeout=aiohttp.ClientTimeout(total=60),
                    ) as response:
                        if response.status != 200:
                            logger.error(
                                f"4DN Experiment API error for {exp_type}: HTTP {response.status}"
                            )
                            break

                        data = await response.json()

                except aiohttp.ClientError as e:
                    logger.error(f"4DN Experiment API network error: {e}")
                    break

                await asyncio.sleep(_REQUEST_INTERVAL)

                graph = data.get("@graph", [])
                if not graph:
                    break

                for item in graph:
                    accession = item.get("accession")
                    if not accession:
                        continue

                    entry = parse_experiment_metadata(item)
                    if entry:
                        results[accession] = entry

                total = data.get("total", 0)
                offset += limit

                if offset >= total:
                    break

            logger.info(
                f"4DN API: fetched {exp_type} experiment metadata, "
                f"{len(results)} entries so far"
            )

    logger.info(f"4DN API: {len(results)} total experiment metadata entries fetched")
    return results
