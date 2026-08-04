import asyncio
from typing import List, Optional

import strawberry

from cfdb import api
from cfdb.api.gql.inputs import (
    FileMetadataInput,
    to_dict,
    to_query,
)
from cfdb.api.gql.types import (
    DistinctFieldType,
    FileList,
    FileMetadataType,
    ObjectIdScalar,
)
from cfdb.models import FileMetadataModel
from cfdb.services import locks


ALLOWED_DISTINCT_FIELDS: frozenset[str] = frozenset(
    {
        "dcc.id",
        "dcc.dcc_name",
        "dcc.dcc_abbreviation",
        "data_type.id",
        "data_type.name",
        "assay_type.id",
        "assay_type.name",
        "file_format.id",
        "file_format.name",
        "compression_format",
        "mime_type",
        "analysis_type",
        "genome_assembly",
        "genome_annotation",
        "output_type",
        "status",
        "data_access_level",
    }
)


def from_pydantic(gql_type, obj):
    """Build a Strawberry type from a fully-dumped model dict.

    ``obj`` is expected to be a complete ``model_dump()`` of the
    corresponding pydantic model (every field key present); callers pass
    ``FileMetadataModel(...).model_dump()``. Mutates ``obj`` in place,
    recursively converting nested-model fields (and lists thereof) into
    their Strawberry types before constructing ``gql_type``.

    Field nesting is resolved by peeling Strawberry's wrapper types: a
    ``StrawberryOptional`` / ``StrawberryList`` exposes ``of_type`` but
    not ``__strawberry_definition__``, while a concrete generated type
    exposes ``__strawberry_definition__``. This distinction is a
    Strawberry-internal contract — re-verify it on a major Strawberry
    upgrade, as a change there would silently leave nested values as raw
    dicts (the bug fixed in #51/#52). Scalar and JSON fields peel to a
    leaf with no ``__strawberry_definition__`` and are left untouched.
    """
    if obj is None:
        return obj
    for field_name, field_type in gql_type.__annotations__.items():
        if field_name not in obj or obj[field_name] is None:
            continue
        # Peel any nesting of Strawberry wrappers (StrawberryOptional /
        # StrawberryList — both expose ``of_type`` but not
        # ``__strawberry_definition__``) down to the concrete Strawberry
        # type. This handles bare ``X``, ``Optional[X]``, ``List[X]`` and
        # ``Optional[List[X]]`` alike — the last being how nested model
        # lists such as ``extra.fourdn.extra_files`` are typed.
        inner = field_type
        while hasattr(inner, "of_type") and not hasattr(
            inner, "__strawberry_definition__"
        ):
            inner = inner.of_type
        if not hasattr(inner, "__strawberry_definition__"):
            continue  # scalar / JSON field — leave the value untouched
        value = obj[field_name]
        if isinstance(value, list):
            obj[field_name] = [from_pydantic(inner, o) for o in value]
        else:
            obj[field_name] = from_pydantic(inner, value)

    return gql_type(**obj)


@strawberry.type
class Query:
    @strawberry.field
    async def files(
        self,
        _: strawberry.Info,
        input: list[FileMetadataInput] | None = None,
        page: int = 0,
        page_size: int = api.PAGE_SIZE,
    ) -> FileList:
        # Wait for any database cutover to complete
        await locks.wait_for_cutover()

        assert api.db is not None
        query = to_query(to_dict(input)) if input else {}

        skip = page * page_size
        # ``count_documents`` takes its filter positionally: Motor names
        # that parameter ``filter`` but the test double names it ``query``,
        # so a keyword argument would break against one of the two.
        total_count, files = await asyncio.gather(
            api.db.files.count_documents(query),
            api.db.files.find(query).skip(skip).limit(page_size).to_list(length=None),
        )

        return FileList(
            total_count=total_count,
            items=[
                from_pydantic(FileMetadataType, FileMetadataModel(**file).model_dump())
                for file in files
            ],
        )

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

    @strawberry.field
    async def distinct_values(
        self,
        _: strawberry.Info,
        fields: list[str],
        input: list[FileMetadataInput] | None = None,
    ) -> List[DistinctFieldType]:
        disallowed = set(fields) - ALLOWED_DISTINCT_FIELDS
        if disallowed:
            raise ValueError(
                f"Field(s) not queryable: {', '.join(sorted(disallowed))}"
            )

        await locks.wait_for_cutover()

        assert api.db is not None
        query = to_query(to_dict(input)) if input else {}

        all_values = await asyncio.gather(
            *(api.db.files.distinct(field, query) for field in fields)
        )
        return [
            DistinctFieldType(field=field, values=values)
            for field, values in zip(fields, all_values)
        ]

    @strawberry.field
    async def file_count(
        self,
        _: strawberry.Info,
        input: list[FileMetadataInput] | None = None,
    ) -> int:
        # Wait for any database cutover to complete
        await locks.wait_for_cutover()

        assert api.db is not None
        query = to_query(to_dict(input)) if input else {}

        return await api.db.files.count_documents(query)


schema = strawberry.Schema(query=Query)
