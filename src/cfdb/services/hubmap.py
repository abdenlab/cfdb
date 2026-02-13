"""HuBMAP Search API integration for access level metadata and enrichment."""

import asyncio
import logging
import re
from typing import Optional

import aiohttp
from pydantic import BaseModel

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


class HuBMAPSearchResult(BaseModel):
    """HuBMAP Search API entity result."""

    uuid: str
    status: Optional[str] = None
    data_access_level: Optional[str] = None
    entity_type: Optional[str] = None


def extract_uuid_from_persistent_id(persistent_id: str) -> Optional[str]:
    """
    Extract UUID from HuBMAP persistent ID.

    Handles formats:
    - doi:10.35079/HBM123.ABCD.456
    - HBM123.ABCD.456
    - Direct UUID format (8-4-4-4-12)

    Returns UUID string or None if extraction fails.
    """
    if not persistent_id:
        return None

    # Try direct UUID pattern match
    uuid_pattern = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    match = re.search(uuid_pattern, persistent_id, re.IGNORECASE)
    if match:
        return match.group(0).lower()

    return None


async def fetch_access_metadata(uuid: str) -> Optional[HuBMAPSearchResult]:
    """
    Fetch access level metadata from HuBMAP Search API.

    Args:
        uuid: HuBMAP entity UUID

    Returns:
        HuBMAPSearchResult or None if fetch fails (graceful degradation)
    """
    search_url = f"https://search.api.hubmapconsortium.org/v3/entities/{uuid}"

    logger.debug(f"Fetching HuBMAP access metadata: {uuid}")

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                search_url, timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return HuBMAPSearchResult(
                        uuid=data.get("uuid", uuid),
                        status=data.get("status"),
                        data_access_level=data.get("data_access_level"),
                        entity_type=data.get("entity_type"),
                    )
                elif response.status == 404:
                    logger.debug(f"HuBMAP entity not found: {uuid}")
                    return None
                else:
                    logger.warning(f"HuBMAP Search API error: HTTP {response.status}")
                    return None

        except asyncio.TimeoutError:
            logger.debug(f"Timeout fetching HuBMAP metadata: {uuid}")
            return None
        except aiohttp.ClientError as e:
            logger.debug(f"Network error fetching HuBMAP metadata: {e}")
            return None
        except Exception as e:
            logger.warning(f"Unexpected error fetching HuBMAP metadata: {e}")
            return None


async def fetch_access_metadata_batch(persistent_ids: list[str]) -> dict[str, str]:
    """
    Batch fetch access levels for datasets by persistent_id (DOI URL).

    Uses HuBMAP Search API to query by doi_url field.

    Args:
        persistent_ids: List of DOI URLs (e.g., "https://doi.org/10.35079/HBM673.JJRZ.435")

    Returns:
        Dict mapping persistent_id -> data_access_level
    """
    if not persistent_ids:
        return {}

    results: dict[str, str] = {}

    # Query HuBMAP Search API for datasets matching these DOI URLs
    search_url = "https://search.api.hubmapconsortium.org/v3/portal/search"

    async with aiohttp.ClientSession() as session:
        # Build query for all DOIs using doi_url.keyword field
        query = {
            "query": {
                "bool": {
                    "should": [
                        {"term": {"doi_url.keyword": doi}} for doi in persistent_ids
                    ],
                    "minimum_should_match": 1,
                }
            },
            "size": len(persistent_ids),
            "_source": ["doi_url", "data_access_level"],
        }

        try:
            async with session.post(
                search_url,
                json=query,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    for hit in data.get("hits", {}).get("hits", []):
                        src = hit.get("_source", {})
                        doi_url = src.get("doi_url")
                        level = src.get("data_access_level")
                        if doi_url and level:
                            results[doi_url] = level
        except asyncio.TimeoutError:
            logger.warning("Timeout batch fetching HuBMAP access levels")
        except aiohttp.ClientError as e:
            logger.warning(f"Network error batch fetching HuBMAP access levels: {e}")
        except Exception as e:
            logger.warning(f"Error batch fetching HuBMAP access levels: {e}")

    return results


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
