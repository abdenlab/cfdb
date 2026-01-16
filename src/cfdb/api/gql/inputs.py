from __future__ import annotations

import strawberry


@strawberry.input
class AnatomyInput:
    id: list[str] | None = None
    name: list[str] | None = None
    description: list[str] | None = None


@strawberry.input
class AssayTypeInput:
    id: list[str] | None = None
    name: list[str] | None = None
    description: list[str] | None = None


@strawberry.input
class SubjectInput:
    id_namespace: list[str] | None = None
    local_id: list[str] | None = None
    project_id_namespace: list[str] | None = None
    project_local_id: list[str] | None = None
    persistent_id: list[str] | None = None
    creation_time: list[str] | None = None
    granularity: list[str] | None = None
    sex: list[str] | None = None
    ethnicity: list[str] | None = None
    age_at_enrollment: list[float] | None = None
    age_at_sampling: list[float] | None = None
    race: list[str] | None = None


@strawberry.input
class BiosampleInput:
    id_namespace: list[str] | None = None
    local_id: list[str] | None = None
    project_id_namespace: list[str] | None = None
    project_local_id: list[str] | None = None
    persistent_id: list[str] | None = None
    creation_time: list[str] | None = None
    sample_prep_method: list[str] | None = None
    anatomy: list[AnatomyInput] | None = None
    biofluid: list[str] | None = None
    subjects: list[SubjectInput] | None = None


@strawberry.input
class CollectionInput:
    biosamples: list[BiosampleInput] | None = None
    id_namespace: list[str] | None = None
    local_id: list[str] | None = None
    persistent_id: list[str] | None = None
    creation_time: list[str] | None = None
    abbreviation: list[str] | None = None
    name: list[str] | None = None
    description: list[str] | None = None
    anatomy: list[AnatomyInput] | None = None
    subjects: list[SubjectInput] | None = None


@strawberry.input
class DataTypeInput:
    id: list[str] | None = None
    name: list[str] | None = None
    description: list[str] | None = None


@strawberry.input
class DCCInput:
    id: list[str] | None = None
    dcc_name: list[str] | None = None
    dcc_abbreviation: list[str] | None = None
    dcc_description: list[str] | None = None
    contact_email: list[str] | None = None
    contact_name: list[str] | None = None
    dcc_url: list[str] | None = None
    project_id_namespace: list[str] | None = None
    project_local_id: list[str] | None = None


@strawberry.input
class FileFormatInput:
    id: list[str] | None = None
    name: list[str] | None = None
    description: list[str] | None = None


@strawberry.input
class FileMetadataInput:
    dcc: list[DCCInput] | None = None
    collections: list[CollectionInput] | None = None
    id_namespace: list[str] | None = None
    local_id: list[str] | None = None
    project_id_namespace: list[str] | None = None
    project_local_id: list[str] | None = None
    persistent_id: list[str] | None = None
    creation_time: list[str] | None = None
    size_in_bytes: list[int] | None = None
    sha256: list[str] | None = None
    md5: list[str] | None = None
    filename: list[str] | None = None
    file_format: list[FileFormatInput] | None = None
    compression_format: list[str] | None = None
    data_type: list[DataTypeInput] | None = None
    assay_type: list[AssayTypeInput] | None = None
    analysis_type: list[str] | None = None
    mime_type: list[str] | None = None
    bundle_collection_id_namespace: list[str] | None = None
    bundle_collection_local_id: list[str] | None = None
    dbgap_study_id: list[str] | None = None
    access_url: list[str] | None = None
    data_access_level: list[str] | None = None


def to_dict(obj):
    """
    Convert a nested strawberry input object into a dict.
    """
    if isinstance(obj, list):
        return [to_dict(item) for item in obj]
    if not hasattr(obj, "__strawberry_definition__"):
        return obj
    result = {}
    for field in obj.__strawberry_definition__.fields:
        value = getattr(obj, field.name)
        result[field.name] = to_dict(value)
    return result


def to_query(obj, prefix=""):
    """
    Convert a nested dict/list structure into a flattened MongoDB query.
    """
    if isinstance(obj, dict):
        and_clause = []
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, (dict, list)):
                flattened = to_query(v, key)
                if isinstance(flattened, dict) and "$and" in flattened:
                    and_clause.extend(flattened["$and"])
                else:
                    and_clause.append(flattened)
            elif v is not None:
                and_clause.append({key: v})
        if len(and_clause) == 1:
            return and_clause[0]
        else:
            return {"$and": and_clause}
    elif isinstance(obj, list):
        or_clause = []
        for item in obj:
            flattened = to_query(item, prefix)
            if isinstance(flattened, dict) and "$or" in flattened:
                or_clause.extend(flattened["$or"])
            else:
                or_clause.append(flattened)
        if len(or_clause) == 1:
            return or_clause[0]
        else:
            return {"$or": or_clause}
    else:
        return {prefix: obj} if prefix else obj
