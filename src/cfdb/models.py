from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, field_validator


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
            An EDAM CV term ID identifying the compression format (e.g., gzip or
            bzip2). None if file is not compressed.

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
            DCC-specific metadata that doesn't map to C2M2 fields. For ENCODE:
            assembly, output_type, platform, etc.

        project:
            The primary project within which this file was created.
    """

    class Config:
        arbitrary_types_allowed = True

    dcc: DCC
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
    extra: Optional[Dict[str, Any]] = None


class DCC(BaseModel):
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
    anatomy: List[Anatomy] = []
    subjects: List[Subject] = []


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
    extra: Optional[Dict[str, Any]] = None


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


class NCBITaxonomy(BaseModel):
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
    taxonomy: Optional[NCBITaxonomy] = None

    @field_validator("age_at_enrollment", "age_at_sampling", mode="before")
    @classmethod
    def empty_string_to_none(cls, v):
        if v == "":
            return None
        return v
