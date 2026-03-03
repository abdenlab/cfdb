"""HuBMAP Search API integration for dataset metadata enrichment.

Fetches dataset, donor, and file metadata from the HuBMAP Search API
(Elasticsearch-backed) to enrich C2M2-materialized documents. Three
enrichment targets run during sync: collections and subjects
(pre-materialization) and files (post-materialization).

API URLs
--------
Bulk dataset search (search_after pagination):
  https://search.api.hubmapconsortium.org/v3/portal/search

Entity Matching
---------------
Collection    persistent_id matches doi_url
Subject       local_id contains donor uuid
File          matched via collection doi_url → dataset, then filename

Field Mapping (HuBMAP Search API → CFDB)
------------------------------------------

File (post-materialization)
~~~~~~~~~~~~~~~~~~~~~~~~~~~
data_access_level                       → data_access_level
ingest_metadata.workflow_description    → genome_assembly (regex-derived)

Enriched File
~~~~~~~~~~~~~
files[].rel_path                        → extra.hubmap.rel_path
files[].is_data_product                 → extra.hubmap.is_data_product

Collection (pre-materialization)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dataset_type                            → collections[].experiment_type
analyte_class                           → collections[].analyte_class

Enriched Collection
~~~~~~~~~~~~~~~~~~~
pipeline                                → collections[].extra.hubmap.pipeline
processing                              → collections[].extra.hubmap.processing
group_name                              → collections[].extra.hubmap.group_name
visualization                           → collections[].extra.hubmap.visualization
vitessce-hints                          → collections[].extra.hubmap.vitessce_hints
metadata                                → collections[].extra.hubmap.metadata

Enriched Subject (pre-materialization, from donor.mapped_metadata)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
sex                                     → subjects[].extra.hubmap.sex
race                                    → subjects[].extra.hubmap.race
age_value                               → subjects[].extra.hubmap.age_value
age_unit                                → subjects[].extra.hubmap.age_unit
height_value                            → subjects[].extra.hubmap.height_value
height_unit                             → subjects[].extra.hubmap.height_unit
weight_value                            → subjects[].extra.hubmap.weight_value
weight_unit                             → subjects[].extra.hubmap.weight_unit
body_mass_index_value                   → subjects[].extra.hubmap.body_mass_index_value
body_mass_index_unit                    → subjects[].extra.hubmap.body_mass_index_unit
cause_of_death                          → subjects[].extra.hubmap.cause_of_death
death_event                             → subjects[].extra.hubmap.death_event
mechanism_of_injury                     → subjects[].extra.hubmap.mechanism_of_injury
medical_history                         → subjects[].extra.hubmap.medical_history
social_history                          → subjects[].extra.hubmap.social_history
"""

import asyncio
import logging
import re
from typing import Optional

import aiohttp

from cfdb.dcc_registry import get_dcc_config

logger = logging.getLogger(__name__)

# Rate limit: max 10 requests/second → 100ms between requests
_REQUEST_INTERVAL = 0.1

# Fields to request from the HuBMAP Search API for enrichment
_BULK_SOURCE_FIELDS = [
    "doi_url",
    "dataset_type",
    "pipeline",
    "processing",
    "group_name",
    "analyte_class",
    "visualization",
    "vitessce-hints",
    "metadata",
    "data_access_level",
    "files",
    "donor.mapped_metadata",
    "donor.uuid",
    "ingest_metadata.workflow_description",
]


_GENOME_ASSEMBLY_RE = re.compile(r"\b(GRCh38|hg38|GRCh37|hg19|GRCm39|mm10)\b", re.IGNORECASE)

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


async def fetch_dataset_metadata_bulk() -> dict[str, dict]:
    """
    Fetch all published HuBMAP dataset metadata from the Search API.

    Uses Elasticsearch ``search_after`` pagination to iterate all published
    datasets in batches of 1000.  Returns a dict keyed by ``doi_url`` so
    callers can match to C2M2 collection ``persistent_id``.

    Returns:
        {doi_url: {dataset_type, pipeline, processing, group_name,
                    analyte_class, visualization, vitessce_hints, metadata,
                    data_access_level, files, donor_metadata,
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
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"entity_type.keyword": "Dataset"}},
                            {"exists": {"field": "doi_url"}},
                        ]
                    }
                },
                "size": page_size,
                "_source": _BULK_SOURCE_FIELDS,
                "sort": [{"_id": "asc"}],
            }

            if search_after is not None:
                query["search_after"] = search_after

            try:
                async with session.post(
                    search_url,
                    json=query,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=60),
                ) as response:
                    if response.status != 200:
                        logger.error(
                            f"HuBMAP Search API error: HTTP {response.status}"
                        )
                        break

                    data = await response.json()

            except aiohttp.ClientError as e:
                logger.error(f"HuBMAP Search API network error: {e}")
                break

            await asyncio.sleep(_REQUEST_INTERVAL)

            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break

            for hit in hits:
                src = hit.get("_source", {})
                doi_url = src.get("doi_url")
                if not doi_url:
                    continue

                entry: dict = {}

                # Dataset-level fields
                for key in (
                    "dataset_type",
                    "pipeline",
                    "processing",
                    "group_name",
                    "analyte_class",
                    "visualization",
                    "data_access_level",
                ):
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

                # File listings
                files = src.get("files")
                if files:
                    entry["files"] = files

                # Donor demographics
                donor = src.get("donor") or {}
                donor_metadata = donor.get("mapped_metadata")
                if donor_metadata:
                    entry["donor_metadata"] = donor_metadata
                donor_uuid = donor.get("uuid")
                if donor_uuid:
                    entry["donor_uuid"] = donor_uuid

                # Genome assembly from workflow description
                ingest = src.get("ingest_metadata") or {}
                workflow_desc = ingest.get("workflow_description")
                if workflow_desc:
                    assembly = extract_genome_assembly(workflow_desc)
                    if assembly:
                        entry["genome_assembly"] = assembly

                if entry:
                    results[doi_url] = entry

            # Prepare next page
            search_after = hits[-1].get("sort")
            if not search_after:
                break

            if len(results) % 5000 < page_size:
                logger.info(
                    f"HuBMAP bulk fetch: {len(results)} datasets fetched so far"
                )

    logger.info(f"HuBMAP bulk fetch: {len(results)} total datasets fetched")
    return results
