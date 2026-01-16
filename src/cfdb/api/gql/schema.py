import asyncio
import pprint
from typing import List, Optional

import strawberry

from cfdb import api
from cfdb.api.gql.inputs import (
    FileMetadataInput,
    to_dict,
    to_query,
)
from cfdb.api.gql.types import (
    FileMetadataType,
    ObjectIdScalar,
)
from cfdb.models import FileMetadataModel
from cfdb.services import locks
from cfdb.services.hubmap import fetch_access_metadata_batch


def from_pydantic(gql_type, obj):
    if obj is None:
        return obj
    for field_name, field_type in gql_type.__annotations__.items():
        if field_name in obj:
            if hasattr(field_type, "__strawberry_definition__"):
                obj[field_name] = from_pydantic(field_type, obj[field_name])
            elif hasattr(field_type, "of_type") and hasattr(
                field_type.of_type, "__strawberry_definition__"
            ):
                if isinstance(obj[field_name], list):
                    obj[field_name] = [
                        from_pydantic(field_type.of_type, o) for o in obj[field_name]
                    ]
                else:
                    obj[field_name] = from_pydantic(field_type.of_type, obj[field_name])

    return gql_type(**obj)


async def check_and_cache_access_levels(files: list[dict]) -> list[dict]:
    """
    Check and cache access levels for HuBMAP files with null data_access_level.
    Uses batch querying for efficiency.
    """
    # Find HuBMAP files needing access level check
    unchecked = [
        f
        for f in files
        if f.get("submission") == "hubmap" and f.get("data_access_level") is None
    ]

    if not unchecked:
        return files

    # Extract DOI URLs from persistent_ids
    # HuBMAP files share DOI with their parent dataset
    doi_to_files: dict[str, list[dict]] = {}
    for f in unchecked:
        doi = f.get("persistent_id")
        if doi:
            doi_to_files.setdefault(doi, []).append(f)

    if not doi_to_files:
        return files

    # Batch fetch access levels from HuBMAP Search API
    access_levels = await fetch_access_metadata_batch(list(doi_to_files.keys()))

    # Update files in memory and collect MongoDB update tasks
    assert api.db is not None
    update_tasks = []
    for doi, level in access_levels.items():
        if doi in doi_to_files:
            for f in doi_to_files[doi]:
                f["data_access_level"] = level
            # Cache in MongoDB (one update per DOI, not per file)
            update_tasks.append(
                api.db.file.update_many(
                    {"persistent_id": doi, "submission": "hubmap"},
                    {"$set": {"data_access_level": level}},
                )
            )

    # Run all MongoDB updates concurrently
    if update_tasks:
        await asyncio.gather(*update_tasks, return_exceptions=True)

    return files


@strawberry.type
class Query:
    @strawberry.field
    async def files(
        self,
        _: strawberry.Info,
        input: list[FileMetadataInput] | None = None,
        page: int = 0,
        page_size: int = api.PAGE_SIZE,
    ) -> List[FileMetadataType]:
        # Wait for any database cutover to complete
        await locks.wait_for_cutover()

        assert api.db is not None
        query = to_query(to_dict(input)) if input else {}
        print(pprint.pformat(query))

        # Add default access level filter for HuBMAP files
        # Exclude consortium/protected, allow public and null (to be checked)
        hubmap_access_filter = {
            "$or": [
                {"submission": {"$ne": "hubmap"}},  # Non-HuBMAP files pass through
                {"data_access_level": "public"},  # Public HuBMAP files
                {"data_access_level": None},  # Unchecked files (will verify)
                {"data_access_level": {"$exists": False}},  # Missing field
            ]
        }

        # Merge with user query
        if query:
            query = {"$and": [query, hubmap_access_filter]}
        else:
            query = hubmap_access_filter

        # Over-fetch to fill page after filtering non-public files
        # Start with 2x requested size, fetch more if needed
        public_files: list[dict] = []
        skip = page * page_size
        fetch_multiplier = 2
        max_iterations = 5  # Prevent infinite loops

        for _ in range(max_iterations):
            fetch_size = page_size * fetch_multiplier
            files = (
                await api.db.files.find(query)
                .skip(skip)
                .limit(fetch_size)
                .to_list(length=None)
            )

            if not files:
                break  # No more files to fetch

            # Check and cache access levels for HuBMAP files with null access level
            checked_files = await check_and_cache_access_levels(files)

            # Filter to only public files
            for f in checked_files:
                if (
                    f.get("submission") != "hubmap"
                    or f.get("data_access_level") == "public"
                ):
                    public_files.append(f)
                    if len(public_files) >= page_size:
                        break

            if len(public_files) >= page_size:
                break

            # Need more files - continue from where we left off
            skip += fetch_size
            fetch_multiplier = 1  # Subsequent fetches are page_size at a time

        return [
            from_pydantic(FileMetadataType, FileMetadataModel(**file).model_dump())
            for file in public_files[:page_size]
        ]

    @strawberry.field
    async def file(
        self, _: strawberry.Info, id: ObjectIdScalar
    ) -> Optional[FileMetadataType]:
        # Wait for any database cutover to complete
        await locks.wait_for_cutover()

        assert api.db is not None
        file = await api.db.files.find_one({"_id": id})
        if file:
            return from_pydantic(
                FileMetadataType, FileMetadataModel(**file).model_dump()
            )
        return None


schema = strawberry.Schema(query=Query)
