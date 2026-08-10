from __future__ import annotations

from typing import Any, List, Optional

from pydantic import BaseModel, field_validator


def coerce_4dn_cv_token(value: Any) -> Any:
    """Coerce a 4DN controlled-vocabulary object to its string token.

    The 4DN portal API represents fields like ``extra_files[].file_format``
    as an embedded CV object carrying the human token under
    ``display_title`` (e.g. ``{"display_title": "pairs_px2", ...}``).
    Return that token when ``value`` is such a dict (``None`` if the dict
    lacks ``display_title``); pass strings, ``None``, and any other type
    through unchanged.

    Single source of truth for this normalization, shared by the
    ``ExtraFile`` read validator, the sync write path
    (``services.fourdn.parse_extra_files``), and the ``/index`` sidecar
    resolver (``api.routers.index``).
    """
    if isinstance(value, dict):
        return value.get("display_title")
    return value


# The numeric 4DN experiment protocol fields the EnrichedFourdnCollection read
# validator and the sync write path (services.fourdn.parse_experiment_metadata)
# coerce from JSON numbers to their string form. This is the single source of
# truth — both the validator registration below and the write path import it.
#
# Deliberately observation-scoped per issue #53: it lists only the fields the
# 4DN portal API has been observed to send as JSON numbers. Sibling
# Optional[str] protocol fields are intentionally excluded. ``fragment_size_range``
# is a genuine range string upstream (e.g. "200-400") and must NOT be stringified;
# ``biotin_removed`` is the most plausible next instance (likely a JSON boolean)
# but has not been observed, so it stays out until it is. Widening this set would
# remove the fail-loud behavior that the negative-scope tests assert.
NUMERIC_PROTOCOL_FIELDS = (
    "crosslinking_temperature",
    "crosslinking_time",
    "ligation_temperature",
    "ligation_volume",
    "ligation_time",
    "digestion_temperature",
    "digestion_time",
    "average_fragment_size",
)


def coerce_scalar_to_str(value: Any) -> Any:
    """Coerce a non-None scalar to its string representation.

    Several 4DN experiment protocol fields (e.g. ``crosslinking_temperature``)
    are declared ``Optional[str]`` but the 4DN portal API sends them as JSON
    numbers (``25.0``). Stringify ``int`` / ``float`` / ``str`` so the field
    deserializes instead of raising a ``string_type`` validation error; pass
    ``None`` and already-string values through unchanged (a string is its own
    ``str``).

    Only scalar types are stringified. A non-scalar value (e.g. a ``list`` or
    ``dict``) is returned UNCHANGED so pydantic rejects it with a clean
    ``string_type`` error rather than having it masked as a Python ``repr``.
    Note ``bool`` is an ``int`` subclass, so ``True``/``False`` stringify to
    ``"True"``/``"False"`` — intended.

    Single source of truth for this normalization, shared by the
    ``EnrichedFourdnCollection`` read validator and the sync write path
    (``services.fourdn.parse_experiment_metadata``).
    """
    if value is None:
        return None
    if isinstance(value, (int, float, str)):
        return str(value)
    return value


class ExtraFile(BaseModel):
    """
    An associated index or auxiliary file from 4DN.

    Stored in EnrichedFourdnFile.extra_files for files that have companion index
    files (e.g., .px2, .bai).

    Attributes:
        href:
            Relative URL path to the file on the 4DN data portal.

        md5sum:
            MD5 checksum of the file.

        file_size:
            Size of the file in bytes.

        file_format:
            Format identifier (e.g., "pairs_px2", "bai").
    """

    href: Optional[str] = None
    md5sum: Optional[str] = None
    file_size: Optional[int] = None
    file_format: Optional[str] = None

    @field_validator("file_format", mode="before")
    @classmethod
    def _coerce_file_format_token(cls, value):
        """Coerce a dict-shaped 4DN ``file_format`` to its string token.

        Documents persisted by the sync may hold the 4DN CV object rather
        than a bare string; normalize on read so the field deserializes
        instead of raising. See :func:`coerce_4dn_cv_token`.
        """
        return coerce_4dn_cv_token(value)


class EnrichedFourdnFile(BaseModel):
    """4DN file-level metadata from the materializer and Search API."""

    enriched_file_format: Optional[str] = None
    genome_assembly: Optional[str] = None
    file_type: Optional[str] = None
    file_type_detailed: Optional[str] = None
    condition: Optional[str] = None
    biosource_name: Optional[str] = None
    dataset: Optional[str] = None
    replicate_info: Optional[str] = None
    cell_line_tier: Optional[str] = None
    extra_files: Optional[List[ExtraFile]] = None


class EnrichedEncodeFile(BaseModel):
    """ENCODE file-level metadata from metadata TSV."""

    assembly: Optional[str] = None
    file_format_type: Optional[str] = None
    output_type: Optional[str] = None
    genome_annotation: Optional[str] = None
    controlled_by: Optional[str] = None
    s3_uri: Optional[str] = None
    azure_url: Optional[str] = None
    file_analysis_title: Optional[str] = None
    file_analysis_status: Optional[str] = None
    biological_replicates: Optional[str] = None
    technical_replicates: Optional[str] = None
    read_length: Optional[str] = None
    mapped_read_length: Optional[str] = None
    run_type: Optional[str] = None
    paired_end: Optional[str] = None
    paired_with: Optional[str] = None
    index_of: Optional[str] = None
    derived_from: Optional[str] = None
    audit_warning: Optional[str] = None
    audit_not_compliant: Optional[str] = None
    audit_error: Optional[str] = None


class EnrichedHubmapFile(BaseModel):
    """HuBMAP file-level metadata from Search API."""

    genome_assembly: Optional[str] = None
    rel_path: Optional[str] = None
    is_data_product: Optional[bool] = None


class EnrichedFile(BaseModel):
    """
    DCC-specific file-level metadata that supplements C2M2 fields.

    Each DCC's extra fields are namespaced under a dedicated submodel.
    """

    fourdn: Optional[EnrichedFourdnFile] = None
    encode: Optional[EnrichedEncodeFile] = None
    hubmap: Optional[EnrichedHubmapFile] = None

    @field_validator("fourdn", "encode", "hubmap", mode="before")
    @classmethod
    def empty_string_to_none(cls, v):
        if v == "":
            return None
        return v


class EnrichedHubmapCollection(BaseModel):
    """HuBMAP dataset-level metadata from Search API."""

    pipeline: Optional[str] = None
    processing: Optional[str] = None
    group_name: Optional[str] = None
    visualization: Optional[bool] = None
    vitessce_hints: Optional[List[str]] = None
    metadata: Optional[dict] = None


class EnrichedHubmapSubject(BaseModel):
    """HuBMAP donor demographics from Search API."""

    age_value: Optional[float] = None
    age_unit: Optional[str] = None
    sex: Optional[str] = None
    race: Optional[str] = None
    body_mass_index_value: Optional[float] = None
    body_mass_index_unit: Optional[str] = None
    cause_of_death: Optional[str] = None
    death_event: Optional[str] = None
    mechanism_of_injury: Optional[str] = None
    medical_history: Optional[List[str]] = None
    social_history: Optional[List[str]] = None
    height_value: Optional[float] = None
    height_unit: Optional[str] = None
    weight_value: Optional[float] = None
    weight_unit: Optional[str] = None


class EnrichedSubject(BaseModel):
    """DCC-specific subject-level metadata."""

    hubmap: Optional[EnrichedHubmapSubject] = None

    @field_validator("hubmap", mode="before")
    @classmethod
    def empty_string_to_none(cls, v):
        if v == "":
            return None
        return v


class EnrichedEncodeCollection(BaseModel):
    """ENCODE experiment-level metadata from metadata TSV."""

    project: Optional[str] = None
    platform: Optional[str] = None
    dbxrefs: Optional[str] = None
    rbns_protein_concentration: Optional[str] = None


class EnrichedFourdnCollection(BaseModel):
    """4DN experiment-level metadata from Search API."""

    display_title: Optional[str] = None
    targeted_factor: Optional[List[str]] = None
    digestion_enzyme: Optional[str] = None
    crosslinking_method: Optional[str] = None
    crosslinking_temperature: Optional[str] = None
    crosslinking_time: Optional[str] = None
    ligation_temperature: Optional[str] = None
    ligation_volume: Optional[str] = None
    ligation_time: Optional[str] = None
    digestion_temperature: Optional[str] = None
    digestion_time: Optional[str] = None
    tagging_method: Optional[str] = None
    fragmentation_method: Optional[str] = None
    biotin_removed: Optional[str] = None
    library_prep_kit: Optional[str] = None
    average_fragment_size: Optional[str] = None
    fragment_size_range: Optional[str] = None
    status: Optional[str] = None
    date_created: Optional[str] = None

    @field_validator(*NUMERIC_PROTOCOL_FIELDS, mode="before")
    @classmethod
    def _coerce_numeric_protocol_field(cls, value):
        """Coerce a numeric 4DN protocol value to its string form.

        These experiment protocol fields are typed ``Optional[str]`` but the
        4DN portal API sends them as JSON numbers (e.g. ``25.0``), and
        documents persisted by the sync may hold those floats. Stringify on
        read so the field deserializes instead of raising — letting
        already-persisted data self-heal without a re-sync. ``None`` and
        existing strings pass through unchanged. See
        :func:`coerce_scalar_to_str`.
        """
        return coerce_scalar_to_str(value)


class EnrichedCollection(BaseModel):
    """
    DCC-specific collection-level metadata from DCC APIs.

    Each DCC's extra fields are namespaced under a dedicated submodel.
    """

    fourdn: Optional[EnrichedFourdnCollection] = None
    hubmap: Optional[EnrichedHubmapCollection] = None
    encode: Optional[EnrichedEncodeCollection] = None

    @field_validator("fourdn", "hubmap", "encode", mode="before")
    @classmethod
    def empty_string_to_none(cls, v):
        if v == "":
            return None
        return v


class EnrichedEncodeBiosample(BaseModel):
    """
    ENCODE biosample-level metadata from metadata TSV.

    Contains biosample classification, treatment, and library information.
    """

    biosample_type: Optional[str] = None
    biosample_treatments: Optional[str] = None
    biosample_treatments_amount: Optional[str] = None
    biosample_treatments_duration: Optional[str] = None
    biosample_genetic_modifications: Optional[str] = None
    library_made_from: Optional[str] = None
    library_depleted_in: Optional[str] = None
    library_extraction_method: Optional[str] = None
    library_lysis_method: Optional[str] = None
    library_crosslinking_method: Optional[str] = None
    library_strand_specific: Optional[str] = None
    library_fragmentation_method: Optional[str] = None
    library_size_range: Optional[str] = None


class EnrichedBiosample(BaseModel):
    """
    DCC-specific biosample-level metadata.

    Each DCC's extra fields are namespaced under a dedicated submodel.
    """

    encode: Optional[EnrichedEncodeBiosample] = None

    @field_validator("encode", mode="before")
    @classmethod
    def empty_string_to_none(cls, v):
        if v == "":
            return None
        return v


class FileMetadataModel(BaseModel):
    """
    A stable digital asset in the C2M2 data model.

    Represents a file with associated metadata including provenance,
    checksums, format information, and access URLs.

    Attributes:
        dcc:
            The Data Coordinating Center that produced this file.

        collections:
            Collections containing this file.

        id_namespace:
            A CFDE-cleared identifier representing the top-level data space
            containing this file. Part 1 of 2-component composite primary key.

        local_id:
            An identifier representing this file, unique within this id_namespace.
            Part 2 of 2-component composite primary key.

        project_id_namespace:
            The id_namespace of the primary project within which this file was
            created. Part 1 of 2-component composite foreign key.

        project_local_id:
            The local_id of the primary project within which this file was created.
            Part 2 of 2-component composite foreign key.

        persistent_id:
            A persistent, resolvable (not necessarily retrievable) URI or compact
            ID permanently attached to this file.

        creation_time:
            An ISO 8601/RFC 3339 compliant timestamp documenting this file's
            creation time (YYYY-MM-DDTHH:MM:SS±NN:NN).

        size_in_bytes:
            The size of this file in bytes.

        sha256:
            SHA-256 checksum for this file (preferred).

        md5:
            MD5 checksum for this file (allowed if sha256 unavailable).

        filename:
            A filename with no prepended PATH information.

        file_format:
            An EDAM CV term identifying the digital format of this file
            (e.g., TSV or FASTQ). If compressed, this is the uncompressed format.

        compression_format:
            An EDAM CV term ID identifying compression that is extrinsic to
            file_format (e.g., "format:3989" for gzip). The empty string means
            no compression was recorded or recognized -- NOT that the bytes are
            uncompressed, since a BAM or bigWig is internally compressed and
            carries it, and the C2M2-sourced DCCs leave the upstream column
            blank on gzipped files. None means no determination was possible;
            neither value licenses skipping a byte-level check.

        data_type:
            An EDAM CV term ID identifying the type of information stored in this
            file (e.g., RNA sequence reads).

        assay_type:
            An OBI CV term ID describing the type of experiment that generated the
            results summarized by this file.

        analysis_type:
            An OBI CV term ID describing the type of analytic operation that
            generated this file.

        mime_type:
            A MIME type describing this file.

        bundle_collection_id_namespace:
            If this file is a bundle, the id_namespace of a collection listing the
            bundle's sub-file contents.

        bundle_collection_local_id:
            If this file is a bundle, the local_id of a collection listing the
            bundle's sub-file contents.

        dbgap_study_id:
            The name of a dbGaP study ID governing access control, compatible for
            comparison to RAS user-level access control metadata.

        access_url:
            A DRS URI or publicly accessible DRS-compatible URL.

        status:
            HuBMAP dataset status (e.g., "Published", "QA") cached from HuBMAP
            Search API.

        data_access_level:
            HuBMAP data access level ("public", "consortium", "protected") cached
            from HuBMAP Search API.

        extra:
            DCC-specific file metadata. See EnrichedFile for available fields.

        project:
            The primary project within which this file was created.
    """

    class Config:
        arbitrary_types_allowed = True

    dcc: Dcc
    collections: List[Collection]
    project: Optional[Project] = None
    id_namespace: str = str()
    local_id: str = str()
    project_id_namespace: str = str()
    project_local_id: str = str()
    persistent_id: Optional[str] = None
    creation_time: Optional[str] = None
    size_in_bytes: Optional[int] = None
    sha256: Optional[str] = None
    md5: Optional[str] = None
    filename: str = str()
    file_format: Optional[FileFormat] = None
    compression_format: Optional[str] = None
    data_type: Optional[DataType] = None
    assay_type: Optional[AssayType] = None
    analysis_type: Optional[str] = None
    mime_type: Optional[str] = None
    bundle_collection_id_namespace: Optional[str] = None
    bundle_collection_local_id: Optional[str] = None
    dbgap_study_id: Optional[str] = None
    access_url: Optional[str] = None
    status: Optional[str] = None
    data_access_level: Optional[str] = None
    genome_assembly: Optional[str] = None
    genome_annotation: Optional[str] = None
    output_type: Optional[str] = None
    output_type_detail: Optional[str] = None
    biological_replicates: Optional[str] = None
    technical_replicates: Optional[str] = None
    assay_info: Optional[str] = None
    condition: Optional[str] = None
    extra: Optional[EnrichedFile] = None

    @field_validator(
        "file_format", "data_type", "assay_type", "project", "extra",
        mode="before",
    )
    @classmethod
    def empty_string_to_none(cls, v):
        if v == "":
            return None
        return v


class Dcc(BaseModel):
    """
    A Common Fund program or Data Coordinating Center.

    Represents the DCC that produced a C2M2 instance, identified by the
    given project foreign key.

    Attributes:
        id:
            The identifier for this DCC, issued by the CFDE-CC.

        dcc_name:
            A short, human-readable, machine-read-friendly label for this DCC.

        dcc_abbreviation:
            A very short display label for this DCC.

        dcc_description:
            A human-readable description of this DCC.

        contact_email:
            Email address of this DCC's primary technical contact.

        contact_name:
            Name of this DCC's primary technical contact.

        dcc_url:
            URL of the front page of the website for this DCC.

        project_id_namespace:
            ID of the identifier namespace for the project record representing
            the C2M2 submission produced by this DCC.

        project_local_id:
            Foreign key identifying the project record representing the C2M2
            submission produced by this DCC.
    """

    id: str = str()
    dcc_name: str = str()
    dcc_abbreviation: str = str()
    dcc_description: Optional[str] = None
    contact_email: str = str()
    contact_name: str = str()
    dcc_url: str = str()
    project_id_namespace: str = str()
    project_local_id: str = str()


class AssayType(BaseModel):
    """
    An Ontology for Biomedical Investigations (OBI) CV term.

    Describes types of experiments that generate results stored in C2M2 files.

    Attributes:
        id:
            An OBI CV term identifier.
        name:
            A short, human-readable, machine-read-friendly label for this OBI term.
        description:
            A human-readable description of this OBI term.
    """

    id: str = str()
    name: str = str()
    description: Optional[str] = None


class FileFormat(BaseModel):
    """
    An EDAM CV 'format:' term.

    Describes the digital format of C2M2 files.

    Attributes:
        id:
            An EDAM CV format term identifier.

        name:
            A short, human-readable, machine-read-friendly label for this EDAM
            format term.

        description:
            A human-readable description of this EDAM format term.
    """

    id: str = str()
    name: str = str()
    description: Optional[str] = None


class DataType(BaseModel):
    """
    An EDAM CV 'data:' term.

    Describes the type of data stored in C2M2 files.

    Attributes:
        id:
            An EDAM CV data term identifier.
        name:
            A short, human-readable, machine-read-friendly label for this EDAM
            data term.
        description:
            A human-readable description of this EDAM data term.
    """

    id: str = str()
    name: str = str()
    description: Optional[str] = None


class Collection(BaseModel):
    """
    A grouping of C2M2 files, biosamples, and/or subjects.

    Attributes:
        biosamples:
            Biosamples contained in this collection.

        id_namespace:
            A CFDE-cleared identifier representing the top-level data space
            containing this collection. Part 1 of 2-component composite primary key.

        local_id:
            An identifier representing this collection, unique within this
            id_namespace. Part 2 of 2-component composite primary key.

        persistent_id:
            A persistent, resolvable (not necessarily retrievable) URI or compact
            ID permanently attached to this collection.

        creation_time:
            An ISO 8601/RFC 3339 compliant timestamp documenting this collection's
            creation time (YYYY-MM-DDTHH:MM:SS±NN:NN).

        abbreviation:
            A very short display label for this collection.

        name:
            A short, human-readable, machine-read-friendly label for this collection.

        description:
            A human-readable description of this collection.

        lab:
            Lab or PI name associated with the experiment. Shared across 4DN
            and ENCODE.

        anatomy:
            Anatomy terms associated with this collection. Populated from the
            collection_anatomy junction table.

        subjects:
            Subjects (donors) directly associated with this collection. Populated
            from the subject_in_collection junction table.
    """

    biosamples: List[Biosample]
    id_namespace: str = str()
    local_id: str = str()
    persistent_id: Optional[str] = None
    creation_time: Optional[str] = None
    abbreviation: Optional[str] = None
    name: str = str()
    description: Optional[str] = None
    lab: Optional[str] = None
    experiment_type: Optional[str] = None
    experiment_target: Optional[str] = None
    analyte_class: Optional[str] = None
    anatomy: List[Anatomy] = []
    subjects: List[Subject] = []
    extra: Optional[EnrichedCollection] = None

    @field_validator("extra", mode="before")
    @classmethod
    def empty_string_to_none(cls, v):
        if v == "":
            return None
        return v


class Biosample(BaseModel):
    """
    A tissue sample or other physical specimen.

    Attributes:
        id_namespace:
            A CFDE-cleared identifier representing the top-level data space
            containing this biosample. Part 1 of 2-component composite primary key.

        local_id:
            An identifier representing this biosample, unique within this
            id_namespace. Part 2 of 2-component composite primary key.

        project_id_namespace:
            The id_namespace of the primary project within which this biosample
            was created. Part 1 of 2-component composite foreign key.

        project_local_id:
            The local_id of the primary project within which this biosample was
            created. Part 2 of 2-component composite foreign key.

        persistent_id:
            A persistent, resolvable (not necessarily retrievable) URI or compact
            ID permanently attached to this biosample.

        creation_time:
            An ISO 8601/RFC 3339 compliant timestamp documenting this biosample's
            creation time (YYYY-MM-DDTHH:MM:SS±NN:NN).

        sample_prep_method:
            An OBI CV term ID (from the 'planned process' branch, excluding 'assay'
            subtree) describing the preparation method that produced this biosample.

        anatomy:
            An UBERON CV term used to locate the origin of this biosample within
            the physiology of its source or host organism.

        biofluid:
            An UBERON CV term or InterLex term used to locate the origin of this
            biosample within the fluid compartment of its source or host organism.

        subjects:
            The subjects (donors) from which this biosample was derived. Linked
            via the biosample_from_subject junction table.
    """

    id_namespace: str = str()
    local_id: str = str()
    project_id_namespace: str = str()
    project_local_id: str = str()
    persistent_id: Optional[str] = None
    creation_time: Optional[str] = None
    sample_prep_method: Optional[str] = None
    anatomy: Optional[Anatomy] = None
    biofluid: Optional[str] = None
    subjects: List[Subject] = []
    extra: Optional[EnrichedBiosample] = None

    @field_validator("anatomy", "extra", mode="before")
    @classmethod
    def empty_string_to_none(cls, v):
        if v == "":
            return None
        return v


class Anatomy(BaseModel):
    """
    An Uber-anatomy ontology (UBERON) CV term.

    Used to locate the origin of a C2M2 biosample within the physiology
    of its source or host organism.

    Attributes:
        id:
            An UBERON CV term identifier.

        name:
            A short, human-readable, machine-read-friendly label for this
            UBERON term.

        description:
            A human-readable description of this UBERON term.
    """

    id: str = str()
    name: str = str()
    description: Optional[str] = None


class NcbiTaxonomy(BaseModel):
    """
    An NCBI Taxonomy term for organism classification.

    Used to identify the species or organism associated with a C2M2 subject.

    Attributes:
        id:
            An NCBI Taxonomy Database ID (e.g., NCBI:txid9606 for human).

        name:
            A short, human-readable label for this taxon (e.g., 'Homo sapiens').

        clade:
            The phylogenetic level assigned to this taxon (e.g., species, genus).

        description:
            A human-readable description of this taxon.
    """

    id: str = str()
    name: str = str()
    clade: Optional[str] = None
    description: Optional[str] = None


class Project(BaseModel):
    """
    A node in the C2M2 project hierarchy.

    Represents a project that subdivides resources described by a DCC's C2M2
    metadata.

    Attributes:
        id_namespace:
            A CFDE-cleared identifier representing the top-level data space
            containing this project. Part 1 of 2-component composite primary key.

        local_id:
            An identifier representing this project, unique within this
            id_namespace. Part 2 of 2-component composite primary key.

        name:
            A short, human-readable, machine-read-friendly label for this project.

        abbreviation:
            A very short display label for this project.

        description:
            A human-readable description of this project.

        persistent_id:
            A persistent, resolvable (not necessarily retrievable) URI or compact
            ID permanently attached to this project.
    """

    id_namespace: str = str()
    local_id: str = str()
    name: str = str()
    abbreviation: Optional[str] = None
    description: Optional[str] = None
    persistent_id: Optional[str] = None


class Subject(BaseModel):
    """
    A human or organism from which biosamples are derived.

    Represents an experimental subject (e.g., donor) in the C2M2 data model.

    Attributes:
        id_namespace:
            A CFDE-cleared identifier representing the top-level data space
            containing this subject. Part 1 of 2-component composite primary key.

        local_id:
            An identifier representing this subject, unique within this
            id_namespace. Part 2 of 2-component composite primary key.

        project_id_namespace:
            The id_namespace of the primary project within which this subject
            was enrolled. Part 1 of 2-component composite foreign key.

        project_local_id:
            The local_id of the primary project within which this subject was
            enrolled. Part 2 of 2-component composite foreign key.

        persistent_id:
            A persistent, resolvable (not necessarily retrievable) URI or compact
            ID permanently attached to this subject.

        creation_time:
            An ISO 8601/RFC 3339 compliant timestamp documenting this subject's
            record creation time (YYYY-MM-DDTHH:MM:SS±NN:NN).

        granularity:
            A CFDE CV term categorizing the subject by granularity (e.g.,
            single organism, cell line, microbiome).

        sex:
            An NCIT CV term ID describing the biological sex of this subject.

        ethnicity:
            An NCIT CV term ID describing the self-reported ethnicity of this
            subject.

        age_at_enrollment:
            The age in years (decimal) of this subject when first enrolled in
            the primary project.

        age_at_sampling:
            The age in years (decimal) of this subject when the associated
            biosample was taken. Populated from the biosample_from_subject
            junction table.

        race:
            Self-identified race(s) of this subject. A list of CFDE CV term IDs
            since subjects can identify with multiple races. Populated from the
            subject_race junction table.

        taxonomy:
            NCBI taxonomy information for this subject's organism. Populated from
            the subject_role_taxonomy junction table.
    """

    id_namespace: str = str()
    local_id: str = str()
    project_id_namespace: str = str()
    project_local_id: str = str()
    persistent_id: Optional[str] = None
    creation_time: Optional[str] = None
    granularity: Optional[str] = None
    sex: Optional[str] = None
    ethnicity: Optional[str] = None
    age_at_enrollment: Optional[float] = None
    age_at_sampling: Optional[float] = None
    race: List[str] = []
    taxonomy: Optional[NcbiTaxonomy] = None
    extra: Optional[EnrichedSubject] = None

    @field_validator(
        "age_at_enrollment", "age_at_sampling", "taxonomy", "extra",
        mode="before",
    )
    @classmethod
    def empty_string_to_none(cls, v):
        if v == "":
            return None
        return v
