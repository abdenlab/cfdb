from typing import Optional, Union, get_type_hints

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


def is_pydantic_model(annotation):
    try:
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            return True
    except TypeError:
        pass

    if subtypes := getattr(annotation, "__args__", None):
        return any(
            isinstance(subtype, type) and issubclass(subtype, BaseModel)
            for subtype in subtypes
        )
    return False


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
    if getattr(field_type, "__origin__", None) is dict:
        return strawberry.scalars.JSON
    args = getattr(field_type, "__args__", None)
    if args and any(getattr(a, "__origin__", None) is dict for a in args):
        new_args = tuple(
            strawberry.scalars.JSON if getattr(a, "__origin__", None) is dict else a
            for a in args
        )
        if len(new_args) == 2 and type(None) in new_args:
            non_none = next(a for a in new_args if a is not type(None))
            return Optional[non_none]
        return Union[new_args]
    return field_type


def annotate(model, name=None):
    def wrapper(cls):
        if name:
            cls.__name__ = f"{name}Type"
        for field_name, field_type in get_type_hints(model).items():
            if not is_pydantic_model(field_type):
                if field_type is ObjectId:
                    cls.__annotations__[field_name] = ObjectIdScalar
                else:
                    cls.__annotations__[field_name] = _resolve_json_type(field_type)
            else:
                try:
                    if isinstance(field_type, type) and issubclass(
                        field_type, BaseModel
                    ):
                        T = build_strawberry_type(field_type)
                        cls.__annotations__[field_name] = T
                except TypeError:
                    pass

                if subtypes := getattr(field_type, "__args__", None):
                    for subtype in subtypes:
                        try:
                            if isinstance(subtype, type) and issubclass(
                                subtype, BaseModel
                            ):
                                T = build_strawberry_type(subtype)

                                _T = field_type.__origin__[  # type: ignore[union-attr]
                                    T,
                                    *(t for t in subtypes if t is not subtype),
                                ]
                                cls.__annotations__[field_name] = _T
                                break
                        except TypeError:
                            pass
        return cls

    return wrapper


@strawberry.experimental.pydantic.type(model=FileMetadataModel)
@annotate(FileMetadataModel)
class FileMetadataType: ...
