"""HuBMAP Search API integration for access level metadata."""

import asyncio
import logging
import re
from typing import Optional

import aiohttp
from pydantic import BaseModel

logger = logging.getLogger(__name__)


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
