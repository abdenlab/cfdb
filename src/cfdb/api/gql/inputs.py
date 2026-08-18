from __future__ import annotations

import strawberry

from cfdb.accessions import normalize_accession
from cfdb.api.gql.types import BigInt

#: Fully-flattened filter paths whose values are folded by
#: :func:`cfdb.accessions.normalize_accession` before becoming a MongoDB
#: predicate. Enumerated in full rather than matched on the last dotted
#: segment: the set of paths whose stored form is folded is fixed and known
#: statically, and leaf matching would silently opt in any future field
#: named ``accession_id`` at any depth. A DCC-native ``extra.<dcc>.accession_id``
#: -- the shape this codebase already uses for upstream values -- would be
#: stored exactly as the DCC published it while being folded on query,
#: which is the failure ``cfdb.accessions`` warns about: nothing raises,
#: the documents simply become permanently unmatchable. Matching the whole
#: path makes a new field fail closed (unfolded, byte-exact, like every
#: other field) instead.
_NORMALIZED_PATHS = {"accession_id", "collections.accession_id"}


@strawberry.input
class AnatomyInput:
    id: list[str] | None = None
    name: list[str] | None = None
    description: list[str] | None = None


@strawberry.input
class NcbiTaxonomyInput:
    id: list[str] | None = None
    name: list[str] | None = None
    clade: list[str] | None = None


@strawberry.input
class ProjectInput:
    id_namespace: list[str] | None = None
    local_id: list[str] | None = None
    name: list[str] | None = None
    abbreviation: list[str] | None = None
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
    taxonomy: list[NcbiTaxonomyInput] | None = None


@strawberry.input
class EnrichedEncodeBiosampleInput:
    biosample_type: list[str] | None = None
    life_stage: list[str] | None = None
    age: list[str] | None = None
    age_units: list[str] | None = None
    biosample_treatments: list[str] | None = None
    biosample_treatments_amount: list[str] | None = None
    biosample_treatments_duration: list[str] | None = None
    biosample_genetic_modifications: list[str] | None = None
    library_made_from: list[str] | None = None
    library_depleted_in: list[str] | None = None
    library_extraction_method: list[str] | None = None
    library_lysis_method: list[str] | None = None
    library_crosslinking_method: list[str] | None = None
    library_strand_specific: list[str] | None = None
    library_fragmentation_method: list[str] | None = None
    library_size_range: list[str] | None = None


@strawberry.input
class EnrichedBiosampleInput:
    encode: list[EnrichedEncodeBiosampleInput] | None = None


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
    extra: list[EnrichedBiosampleInput] | None = None


@strawberry.input
class EnrichedFourdnCollectionInput:
    display_title: list[str] | None = None
    targeted_factor: list[str] | None = None
    digestion_enzyme: list[str] | None = None
    crosslinking_method: list[str] | None = None
    crosslinking_temperature: list[str] | None = None
    crosslinking_time: list[str] | None = None
    ligation_temperature: list[str] | None = None
    ligation_volume: list[str] | None = None
    ligation_time: list[str] | None = None
    digestion_temperature: list[str] | None = None
    digestion_time: list[str] | None = None
    tagging_method: list[str] | None = None
    fragmentation_method: list[str] | None = None
    biotin_removed: list[str] | None = None
    library_prep_kit: list[str] | None = None
    average_fragment_size: list[str] | None = None
    fragment_size_range: list[str] | None = None
    status: list[str] | None = None
    date_created: list[str] | None = None


@strawberry.input
class EnrichedEncodeCollectionInput:
    experiment_target: list[str] | None = None
    project: list[str] | None = None
    platform: list[str] | None = None
    dbxrefs: list[str] | None = None
    rbns_protein_concentration: list[str] | None = None
    annotation_type: list[str] | None = None
    software_used: list[str] | None = None
    encyclopedia_version: list[str] | None = None


@strawberry.input
class EnrichedHubmapCollectionInput:
    dataset_type: list[str] | None = None
    pipeline: list[str] | None = None
    processing: list[str] | None = None
    group_name: list[str] | None = None
    analyte_class: list[str] | None = None
    visualization: list[bool] | None = None


@strawberry.input
class EnrichedCollectionInput:
    fourdn: list[EnrichedFourdnCollectionInput] | None = None
    encode: list[EnrichedEncodeCollectionInput] | None = None
    hubmap: list[EnrichedHubmapCollectionInput] | None = None


@strawberry.input
class CollectionInput:
    biosamples: list[BiosampleInput] | None = None
    id_namespace: list[str] | None = None
    local_id: list[str] | None = None
    accession_id: list[str] | None = None
    persistent_id: list[str] | None = None
    creation_time: list[str] | None = None
    abbreviation: list[str] | None = None
    name: list[str] | None = None
    description: list[str] | None = None
    lab: list[str] | None = None
    experiment_type: list[str] | None = None
    experiment_target: list[str] | None = None
    analyte_class: list[str] | None = None
    anatomy: list[AnatomyInput] | None = None
    subjects: list[SubjectInput] | None = None
    extra: list[EnrichedCollectionInput] | None = None


@strawberry.input
class DataTypeInput:
    id: list[str] | None = None
    name: list[str] | None = None
    description: list[str] | None = None


@strawberry.input
class DccInput:
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
class EnrichedFourdnFileInput:
    enriched_file_format: list[str] | None = None
    genome_assembly: list[str] | None = None
    file_type: list[str] | None = None
    file_type_detailed: list[str] | None = None
    condition: list[str] | None = None
    biosource_name: list[str] | None = None
    dataset: list[str] | None = None
    replicate_info: list[str] | None = None
    cell_line_tier: list[str] | None = None


@strawberry.input
class EnrichedEncodeFileInput:
    assembly: list[str] | None = None
    annotation_type: list[str] | None = None
    organism: list[str] | None = None
    file_format_type: list[str] | None = None
    output_type: list[str] | None = None
    genome_annotation: list[str] | None = None
    controlled_by: list[str] | None = None
    s3_uri: list[str] | None = None
    azure_url: list[str] | None = None
    file_analysis_title: list[str] | None = None
    file_analysis_status: list[str] | None = None
    read_length: list[str] | None = None
    mapped_read_length: list[str] | None = None
    run_type: list[str] | None = None
    paired_end: list[str] | None = None
    paired_with: list[str] | None = None
    index_of: list[str] | None = None
    derived_from: list[str] | None = None
    audit_warning: list[str] | None = None
    audit_not_compliant: list[str] | None = None
    audit_error: list[str] | None = None


@strawberry.input
class EnrichedHubmapFileInput:
    genome_assembly: list[str] | None = None
    rel_path: list[str] | None = None
    is_data_product: list[bool] | None = None


@strawberry.input
class EnrichedFileInput:
    fourdn: list[EnrichedFourdnFileInput] | None = None
    encode: list[EnrichedEncodeFileInput] | None = None
    hubmap: list[EnrichedHubmapFileInput] | None = None


@strawberry.input
class FileMetadataInput:
    dcc: list[DccInput] | None = None
    collections: list[CollectionInput] | None = None
    project: list[ProjectInput] | None = None
    id_namespace: list[str] | None = None
    local_id: list[str] | None = None
    accession_id: list[str] | None = None
    project_id_namespace: list[str] | None = None
    project_local_id: list[str] | None = None
    persistent_id: list[str] | None = None
    creation_time: list[str] | None = None
    # ``BigInt``, not ``int``: a 32-bit ``Int`` filter cannot name the size of
    # a file over ~2 GB, which would leave exactly the files the widened
    # output field exposes unfilterable.
    size_in_bytes: list[BigInt] | None = None
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
    genome_assembly: list[str] | None = None
    genome_annotation: list[str] | None = None
    output_type: list[str] | None = None
    output_type_detail: list[str] | None = None
    biological_replicates: list[str] | None = None
    technical_replicates: list[str] | None = None
    assay_info: list[str] | None = None
    condition: list[str] | None = None
    extra: list[EnrichedFileInput] | None = None


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


def _predicate(key, value):
    """Build a single equality predicate, folding normalized fields.

    Every leaf of a filter becomes a bare equality match, so a value that
    does not match the stored form byte-for-byte matches nothing. For
    ``accession_id`` the stored form is
    :func:`cfdb.accessions.normalize_accession`'s output, so the filter
    value is folded the same way here -- the one place a scalar becomes a
    predicate -- rather than at each ``to_query`` call site, where a later
    caller could bypass it.

    An accession that folds to ``None`` (blank or whitespace only)
    contributes no clause: the empty dict returned here is dropped by
    ``to_query``. Emitting ``{field: None}`` instead would match documents
    whose accession is null *or absent* -- every HuBMAP file, every 4DN
    file whose accession did not parse, and the whole corpus before the
    first post-deploy sync. That made a blank value the only filter in the
    schema that *widens* the result set, where every sibling string field
    matches nothing; a search box wired straight to the variable returned
    a page of unrelated files rather than no results.
    """
    if key in _NORMALIZED_PATHS and isinstance(value, str):
        folded = normalize_accession(value)
        return {key: folded} if folded is not None else {}
    return {key: value}


def to_query(obj, prefix=""):
    """
    Convert a nested dict/list structure into a flattened MongoDB query.

    A branch that contributes no constraint -- an unset field, an accession
    that folds away, or an empty list -- yields ``{}`` and is dropped by its
    parent rather than left in place. MongoDB rejects ``{"$and": []}`` and
    ``{"$or": []}`` outright, so an empty clause list has to collapse to
    ``{}`` (match everything) rather than be emitted; without that, a filter
    whose only value folded away would 500 instead of returning rows.
    """
    if isinstance(obj, dict):
        and_clause = []
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, (dict, list)):
                flattened = to_query(v, key)
                if isinstance(flattened, dict) and "$and" in flattened:
                    and_clause.extend(flattened["$and"])
                elif flattened:
                    and_clause.append(flattened)
            elif v is not None:
                predicate = _predicate(key, v)
                if predicate:
                    and_clause.append(predicate)
        if not and_clause:
            return {}
        if len(and_clause) == 1:
            return and_clause[0]
        return {"$and": and_clause}
    elif isinstance(obj, list):
        or_clause = []
        for item in obj:
            flattened = to_query(item, prefix)
            if isinstance(flattened, dict) and "$or" in flattened:
                or_clause.extend(flattened["$or"])
            elif flattened:
                or_clause.append(flattened)
        if not or_clause:
            return {}
        if len(or_clause) == 1:
            return or_clause[0]
        return {"$or": or_clause}
    else:
        return _predicate(prefix, obj) if prefix else obj
