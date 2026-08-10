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


# Bounds of a signed 64-bit integer — the range ``BigInt`` accepts, matching
# what MongoDB stores in a BSON int64 and what a C2M2 file size can be.
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

# Above this magnitude a JSON number is no longer exactly representable in an
# IEEE-754 double, which is the only numeric type a browser client has. No
# byte size can reach it (2**53 bytes is ~9 PB), so this is a guard against a
# non-size value being routed through ``BigInt``, not a live concern for
# ``size_in_bytes``.
_JS_SAFE_INTEGER_MAX = 2**53 - 1


def _coerce_big_int(value):
    """Coerce a ``BigInt`` value in either direction, rejecting non-integers.

    Serialization and parsing share one implementation because the wire form
    is a JSON number: the value that goes out is the value that comes back.
    ``bool`` is excluded explicitly because it is an ``int`` subclass in
    Python and ``true`` is not a size.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"BigInt cannot represent non-integer value: {value!r}")
    if not _INT64_MIN <= value <= _INT64_MAX:
        raise ValueError(
            f"BigInt cannot represent non 64-bit signed integer value: {value}"
        )
    return value


@strawberry.scalar(
    description=(
        "A signed 64-bit integer, serialized as a JSON number. Widens the "
        "GraphQL `Int` scalar, which the specification fixes at 32 bits and "
        "which therefore cannot represent a file larger than ~2 GB. Values "
        f"beyond {_JS_SAFE_INTEGER_MAX} (2^53-1) exceed what an IEEE-754 "
        "double represents exactly and would lose precision in a JavaScript "
        "client; byte sizes cannot reach that magnitude."
    ),
    serialize=_coerce_big_int,
    parse_value=_coerce_big_int,
)
class BigInt:
    """A signed 64-bit integer represented as a JSON number in GraphQL."""

    ...


# Model fields whose Python ``int`` annotation must NOT become a GraphQL
# ``Int``. Keyed by ``(model, field name)`` so widening one model's field
# does not silently widen an unrelated field that happens to share its name.
_SCALAR_OVERRIDES: dict[tuple[type, str], object] = {
    (FileMetadataModel, "size_in_bytes"): BigInt,
}


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


def _substitute_scalar(field_type, scalar):
    """Replace a field's scalar type, preserving an ``Optional`` wrapper.

    Overridden fields are declared on the model as ``T`` or ``Optional[T]``,
    so no deeper nesting needs handling.
    """
    args = getattr(field_type, "__args__", None)
    if args and type(None) in args:
        return Optional[scalar]
    return scalar


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
            override = _SCALAR_OVERRIDES.get((model, field_name))
            if override is not None:
                cls.__annotations__[field_name] = _substitute_scalar(
                    field_type, override
                )
                continue
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
