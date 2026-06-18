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
from typing import Optional

import aiohttp

from cfdb.dcc_registry import get_dcc_config
from cfdb.models import coerce_4dn_cv_token, coerce_scalar_to_str

logger = logging.getLogger(__name__)

# Rate limit: max 10 requests/second → 100ms between requests
_REQUEST_INTERVAL = 0.1

# 4DN accession pattern: 4DNF followed by alphanumeric characters
_ACCESSION_RE = re.compile(r"4DNF[A-Z0-9]+")

# 4DN experiment/experiment set accession pattern: 4DNEX* or 4DNES*
_EXPERIMENT_ACCESSION_RE = re.compile(r"4DNE[A-Z][A-Z0-9]+")


def extract_accession(persistent_id: str) -> Optional[str]:
    """
    Extract 4DN file accession from a persistent ID URL.

    Handles format: https://data.4dnucleome.org/files-processed/4DNFI1234ABC/@@download/4DNFI1234ABC.mcool
    or: https://data.4dnucleome.org/4DNFI1234ABC

    Returns accession string (e.g., "4DNFI1234ABC") or None.
    """
    if not persistent_id:
        return None
    match = _ACCESSION_RE.search(persistent_id)
    return match.group(0) if match else None


def extract_experiment_accession(persistent_id: str) -> Optional[str]:
    """
    Extract 4DN experiment or experiment set accession from a persistent ID URL.

    Handles accessions starting with 4DNEX (experiments) or 4DNES (experiment sets).

    Returns accession string (e.g., "4DNEXH4ZUIH6") or None.
    """
    if not persistent_id:
        return None
    match = _EXPERIMENT_ACCESSION_RE.search(persistent_id)
    return match.group(0) if match else None


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


async def fetch_file_metadata_bulk() -> dict[str, dict]:
    """
    Fetch file metadata from the 4DN Search API for FileProcessed and FileFastq types.

    Paginates through all results and returns a dict keyed by accession.

    Returns:
        {accession: {genome_assembly, file_type, file_type_detailed, condition,
                      biosource_name, dataset, experiment_type, assay_info,
                      replicate_info}}
    """
    config = get_dcc_config("4dn")
    api_base = config["api_base"]
    results: dict[str, dict] = {}

    file_types = ["FileProcessed", "FileFastq"]

    async with aiohttp.ClientSession() as session:
        for file_type in file_types:
            offset = 0
            limit = 1000

            while True:
                url = (
                    f"{api_base}/search/"
                    f"?type={file_type}"
                    f"&field=accession"
                    f"&field=genome_assembly"
                    f"&field=file_type"
                    f"&field=file_type_detailed"
                    f"&field=track_and_facet_info"
                    f"&field=extra_files"
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
                                f"4DN Search API error for {file_type}: HTTP {response.status}"
                            )
                            break

                        data = await response.json()

                except aiohttp.ClientError as e:
                    logger.error(f"4DN Search API network error: {e}")
                    break

                await asyncio.sleep(_REQUEST_INTERVAL)

                graph = data.get("@graph", [])
                if not graph:
                    break

                for item in graph:
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

                total = data.get("total", 0)
                offset += limit

                if offset % 5000 == 0:
                    logger.info(
                        f"Fetched {min(offset, total)}/{total} {file_type} records from 4DN API"
                    )

                if offset >= total:
                    break

            logger.info(
                f"4DN API: fetched {file_type} metadata, "
                f"{sum(1 for a, e in results.items() if e)} entries so far"
            )

    logger.info(f"4DN API: {len(results)} total file metadata entries fetched")
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
# rest (mirrors the EnrichedFourdnCollection read validator).
_NUMERIC_PROTOCOL_FIELDS = {
    "crosslinking_temperature",
    "crosslinking_time",
    "ligation_temperature",
    "ligation_volume",
    "ligation_time",
    "digestion_temperature",
    "digestion_time",
    "average_fragment_size",
}

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
