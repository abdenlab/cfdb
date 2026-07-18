from typing import List, Optional, Union, get_type_hints

import strawberry
import strawberry.scalars
from bson import ObjectId
from pydantic import BaseModel

from cfdb.models import FileMetadataModel


@strawberry.scalar(
    serialize=lambda v: str(v),
    parse_value=lambda v: ObjectId(v),
)
class ObjectIdScalar:
    """A MongoDB ObjectId represented as a string in GraphQL."""

    ...


@strawberry.type
class DistinctFieldType:
    field: str
    values: strawberry.scalars.JSON


def _is_dict_type(t):
    """Check if a type is dict or a parameterized dict (dict[K, V])."""
    return t is dict or getattr(t, "__origin__", None) is dict


def _find_basemodel(annotation):
    """Find a BaseModel class in a type annotation, at any nesting depth.

    Returns (model_class, depth) where depth is:
        0 — direct BaseModel (e.g., SomeModel)
        1 — one level of wrapping (e.g., Optional[SomeModel], List[SomeModel])
        2 — two levels (e.g., Optional[List[SomeModel]])
    Returns (None, -1) if no BaseModel is found.
    """
    # Direct BaseModel
    try:
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            return annotation, 0
    except TypeError:
        pass

    args = getattr(annotation, "__args__", None)
    if not args:
        return None, -1

    for arg in args:
        if arg is type(None):
            continue
        # Level 1: Optional[Model] or List[Model]
        try:
            if isinstance(arg, type) and issubclass(arg, BaseModel):
                return arg, 1
        except TypeError:
            pass
        # Level 2: Optional[List[Model]]
        inner_args = getattr(arg, "__args__", None)
        if inner_args:
            for inner in inner_args:
                try:
                    if isinstance(inner, type) and issubclass(inner, BaseModel):
                        return inner, 2
                except TypeError:
                    pass

    return None, -1


# Cache for built Strawberry types to avoid duplicates
_type_cache: dict[type, type] = {}


def build_strawberry_type(type):
    if type in _type_cache:
        return _type_cache[type]

    @strawberry.experimental.pydantic.type(model=type)
    @annotate(type, type.__name__)
    class T: ...

    _type_cache[type] = T
    return T


def _resolve_json_type(field_type):
    """Replace dict types with strawberry.scalars.JSON for GraphQL compatibility."""
    if _is_dict_type(field_type):
        return strawberry.scalars.JSON
    args = getattr(field_type, "__args__", None)
    if args and any(_is_dict_type(a) for a in args):
        new_args = tuple(
            strawberry.scalars.JSON if _is_dict_type(a) else a
            for a in args
        )
        if len(new_args) == 2 and type(None) in new_args:
            non_none = next(a for a in new_args if a is not type(None))
            return Optional[non_none]
        return Union[new_args]
    return field_type


def _rebuild_type(field_type, model_cls, strawberry_cls):
    """Replace a BaseModel class inside a (possibly nested) type annotation
    with its Strawberry equivalent, preserving Optional/List wrappers."""
    # Direct: Model -> StrawberryModel
    try:
        if isinstance(field_type, type) and issubclass(field_type, BaseModel):
            return strawberry_cls
    except TypeError:
        pass

    origin = getattr(field_type, "__origin__", None)
    args = getattr(field_type, "__args__", None)
    if not args:
        return field_type

    new_args = []
    for arg in args:
        if arg is type(None):
            new_args.append(arg)
        elif isinstance(arg, type) and arg is model_cls:
            new_args.append(strawberry_cls)
        else:
            # Recurse one level for List[Model] inside Optional
            inner_origin = getattr(arg, "__origin__", None)
            inner_args = getattr(arg, "__args__", None)
            if inner_origin is list and inner_args:
                replaced_inner = tuple(
                    strawberry_cls if (isinstance(ia, type) and ia is model_cls) else ia
                    for ia in inner_args
                )
                new_args.append(List[replaced_inner[0]] if len(replaced_inner) == 1 else arg)
            else:
                new_args.append(arg)

    return origin[tuple(new_args)] if origin else field_type


def annotate(model, name=None):
    def wrapper(cls):
        if name:
            cls.__name__ = f"{name}Type"
        for field_name, field_type in get_type_hints(model).items():
            model_cls, _ = _find_basemodel(field_type)
            if model_cls is None:
                if field_type is ObjectId:
                    cls.__annotations__[field_name] = ObjectIdScalar
                else:
                    cls.__annotations__[field_name] = _resolve_json_type(field_type)
            else:
                T = build_strawberry_type(model_cls)
                cls.__annotations__[field_name] = _rebuild_type(field_type, model_cls, T)
        return cls

    return wrapper


@strawberry.experimental.pydantic.type(model=FileMetadataModel)
@annotate(FileMetadataModel)
class FileMetadataType: ...


@strawberry.type
class FileList:
    """A page of files together with the total number of matches.

    ``total_count`` counts every document matching the query, independent
    of the ``page``/``page_size`` window ``items`` was drawn through, so
    clients can size pagination controls.
    """

    total_count: int
    items: List[FileMetadataType]
