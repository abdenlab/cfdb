-- cfdb DuckDB dump schema (issue #123).
--
-- schema_version: 2
--
-- The schema of record for the flattened relational dump of the
-- materialized ``files`` collection. The design rules, the per-consortium
-- source mapping for every promoted common column, and the loader's dedup
-- and coercion policies are documented in the issue and in the README's
-- "DuckDB dump" section. Bump the schema_version header above whenever
-- this file changes shape; ``cfdb.dump`` parses it at import.
--
-- DuckDB has no ALTER TABLE ... ADD CONSTRAINT, so every foreign key is
-- declared here and the loader inserts parents before children:
-- dcc -> project -> ontology tables -> entities -> junctions -> extensions.

-- ─── provenance ────────────────────────────────────────────────────────────

CREATE TABLE dump_info (
    schema_version    INTEGER   NOT NULL,
    generated_at      TIMESTAMP NOT NULL,
    cfdb_version      VARCHAR,
    cfdb_commit       VARCHAR,
    source_database   VARCHAR   NOT NULL,
    source_collection VARCHAR   NOT NULL,
    submissions       VARCHAR[] NOT NULL,   -- submissions included in this dump
    file_count        BIGINT    NOT NULL
);

-- Row counts per table, plus coercion-failure counters
-- (size_in_bytes_uncoercible, creation_time_unparseable, ...).
CREATE TABLE dump_stat (
    name  VARCHAR PRIMARY KEY,
    value BIGINT  NOT NULL
);

-- Sampled divergences where repeated copies of one entity disagreed.
CREATE TABLE dump_conflict (
    table_name      VARCHAR NOT NULL,
    natural_key     VARCHAR NOT NULL,
    column_name     VARCHAR NOT NULL,
    kept_value      VARCHAR,
    discarded_value VARCHAR
);

-- ─── reference ─────────────────────────────────────────────────────────────

CREATE TABLE dcc (
    id                   VARCHAR PRIMARY KEY,      -- CFDE-issued, e.g. cfde_registry_dcc:4dn
    submission           VARCHAR NOT NULL UNIQUE,  -- 4dn | encode | hubmap
    dcc_name             VARCHAR NOT NULL,
    dcc_abbreviation     VARCHAR NOT NULL,
    dcc_description      VARCHAR,
    contact_email        VARCHAR,
    contact_name         VARCHAR,
    dcc_url              VARCHAR,
    project_id_namespace VARCHAR,
    project_local_id     VARCHAR
);

CREATE TABLE project (
    id_namespace  VARCHAR NOT NULL,
    local_id      VARCHAR NOT NULL,
    submission    VARCHAR NOT NULL REFERENCES dcc (submission),
    name          VARCHAR,
    abbreviation  VARCHAR,
    description   VARCHAR,
    persistent_id VARCHAR,
    PRIMARY KEY (id_namespace, local_id)
);

-- Ontology tables are deliberately submission-free: a term is the same term in
-- every consortium, and a dump exists to be joined across them. The raw Mongo
-- collections are scoped UNIQUE (submission, id), so the loader dedupes on id
-- and records any name/description divergence in dump_conflict.

CREATE TABLE file_format (        -- EDAM format: terms, or minted cfdb: tokens
    id          VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL,
    description VARCHAR
);

CREATE TABLE data_type (          -- EDAM data: terms
    id          VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL,
    description VARCHAR
);

CREATE TABLE assay_type (         -- OBI terms
    id          VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL,
    description VARCHAR
);

CREATE TABLE anatomy (            -- UBERON terms
    id          VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL,
    description VARCHAR
);

CREATE TABLE ncbi_taxonomy (
    id          VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL,
    clade       VARCHAR,
    description VARCHAR
);

-- ─── entities ──────────────────────────────────────────────────────────────

CREATE TABLE file (
    id_namespace                   VARCHAR NOT NULL,
    local_id                       VARCHAR NOT NULL,
    submission                     VARCHAR NOT NULL REFERENCES dcc (submission),
    accession_id                   VARCHAR,          -- case-folded; NULL for HuBMAP
    filename                       VARCHAR,
    persistent_id                  VARCHAR,
    creation_time                  TIMESTAMP,
    size_in_bytes                  BIGINT,
    sha256                         VARCHAR,
    md5                            VARCHAR,
    file_format                    VARCHAR REFERENCES file_format (id),
    compression_format             VARCHAR,          -- '' means "nothing recorded"
    data_type                      VARCHAR REFERENCES data_type (id),
    assay_type                     VARCHAR REFERENCES assay_type (id),
    analysis_type                  VARCHAR,
    mime_type                      VARCHAR,
    project_id_namespace           VARCHAR,
    project_local_id               VARCHAR,
    bundle_collection_id_namespace VARCHAR,
    bundle_collection_local_id     VARCHAR,
    dbgap_study_id                 VARCHAR,
    access_url                     VARCHAR,
    status                         VARCHAR,
    data_access_level              VARCHAR,          -- public | consortium | protected
    -- promoted common enrichment
    genome_assembly                VARCHAR,
    genome_annotation              VARCHAR,
    output_type                    VARCHAR,
    output_type_detail             VARCHAR,
    biological_replicates          VARCHAR,
    technical_replicates           VARCHAR,
    assay_info                     VARCHAR,
    "condition"                    VARCHAR,
    PRIMARY KEY (id_namespace, local_id),
    FOREIGN KEY (project_id_namespace, project_local_id)
        REFERENCES project (id_namespace, local_id)
);

CREATE TABLE collection (
    id_namespace      VARCHAR NOT NULL,
    local_id          VARCHAR NOT NULL,
    submission        VARCHAR NOT NULL REFERENCES dcc (submission),
    accession_id      VARCHAR,
    persistent_id     VARCHAR,
    creation_time     TIMESTAMP,
    abbreviation      VARCHAR,
    name              VARCHAR,
    description       VARCHAR,
    -- promoted common enrichment
    lab               VARCHAR,
    experiment_type   VARCHAR,
    experiment_target VARCHAR,
    analyte_class     VARCHAR,
    PRIMARY KEY (id_namespace, local_id)
);

CREATE TABLE biosample (
    id_namespace         VARCHAR NOT NULL,
    local_id             VARCHAR NOT NULL,
    submission           VARCHAR NOT NULL REFERENCES dcc (submission),
    project_id_namespace VARCHAR,
    project_local_id     VARCHAR,
    persistent_id        VARCHAR,
    creation_time        TIMESTAMP,
    sample_prep_method   VARCHAR,
    anatomy              VARCHAR REFERENCES anatomy (id),
    biofluid             VARCHAR,
    PRIMARY KEY (id_namespace, local_id),
    FOREIGN KEY (project_id_namespace, project_local_id)
        REFERENCES project (id_namespace, local_id)
);

CREATE TABLE subject (
    id_namespace         VARCHAR NOT NULL,
    local_id             VARCHAR NOT NULL,
    submission           VARCHAR NOT NULL REFERENCES dcc (submission),
    project_id_namespace VARCHAR,
    project_local_id     VARCHAR,
    persistent_id        VARCHAR,
    creation_time        TIMESTAMP,
    granularity          VARCHAR,
    sex                  VARCHAR,
    ethnicity            VARCHAR,
    age_at_enrollment    DOUBLE,
    age_at_sampling      DOUBLE,
    -- The files projection collapses C2M2's subject_role_taxonomy junction to a
    -- single term, so the role is not recoverable from this source.
    taxonomy             VARCHAR REFERENCES ncbi_taxonomy (id),
    PRIMARY KEY (id_namespace, local_id),
    FOREIGN KEY (project_id_namespace, project_local_id)
        REFERENCES project (id_namespace, local_id)
);

-- ─── C2M2 junctions ────────────────────────────────────────────────────────

CREATE TABLE file_in_collection (
    file_id_namespace       VARCHAR NOT NULL,
    file_local_id           VARCHAR NOT NULL,
    collection_id_namespace VARCHAR NOT NULL,
    collection_local_id     VARCHAR NOT NULL,
    PRIMARY KEY (file_id_namespace, file_local_id,
                 collection_id_namespace, collection_local_id),
    FOREIGN KEY (file_id_namespace, file_local_id)
        REFERENCES file (id_namespace, local_id),
    FOREIGN KEY (collection_id_namespace, collection_local_id)
        REFERENCES collection (id_namespace, local_id)
);

CREATE TABLE biosample_in_collection (
    biosample_id_namespace  VARCHAR NOT NULL,
    biosample_local_id      VARCHAR NOT NULL,
    collection_id_namespace VARCHAR NOT NULL,
    collection_local_id     VARCHAR NOT NULL,
    PRIMARY KEY (biosample_id_namespace, biosample_local_id,
                 collection_id_namespace, collection_local_id),
    FOREIGN KEY (biosample_id_namespace, biosample_local_id)
        REFERENCES biosample (id_namespace, local_id),
    FOREIGN KEY (collection_id_namespace, collection_local_id)
        REFERENCES collection (id_namespace, local_id)
);

CREATE TABLE subject_in_collection (
    subject_id_namespace    VARCHAR NOT NULL,
    subject_local_id        VARCHAR NOT NULL,
    collection_id_namespace VARCHAR NOT NULL,
    collection_local_id     VARCHAR NOT NULL,
    PRIMARY KEY (subject_id_namespace, subject_local_id,
                 collection_id_namespace, collection_local_id),
    FOREIGN KEY (subject_id_namespace, subject_local_id)
        REFERENCES subject (id_namespace, local_id),
    FOREIGN KEY (collection_id_namespace, collection_local_id)
        REFERENCES collection (id_namespace, local_id)
);

CREATE TABLE biosample_from_subject (
    biosample_id_namespace VARCHAR NOT NULL,
    biosample_local_id     VARCHAR NOT NULL,
    subject_id_namespace   VARCHAR NOT NULL,
    subject_local_id       VARCHAR NOT NULL,
    PRIMARY KEY (biosample_id_namespace, biosample_local_id,
                 subject_id_namespace, subject_local_id),
    FOREIGN KEY (biosample_id_namespace, biosample_local_id)
        REFERENCES biosample (id_namespace, local_id),
    FOREIGN KEY (subject_id_namespace, subject_local_id)
        REFERENCES subject (id_namespace, local_id)
);

CREATE TABLE collection_anatomy (
    collection_id_namespace VARCHAR NOT NULL,
    collection_local_id     VARCHAR NOT NULL,
    anatomy                 VARCHAR NOT NULL REFERENCES anatomy (id),
    PRIMARY KEY (collection_id_namespace, collection_local_id, anatomy),
    FOREIGN KEY (collection_id_namespace, collection_local_id)
        REFERENCES collection (id_namespace, local_id)
);

CREATE TABLE subject_race (
    subject_id_namespace VARCHAR NOT NULL,
    subject_local_id     VARCHAR NOT NULL,
    race                 VARCHAR NOT NULL,
    PRIMARY KEY (subject_id_namespace, subject_local_id, race),
    FOREIGN KEY (subject_id_namespace, subject_local_id)
        REFERENCES subject (id_namespace, local_id)
);

-- ─── per-consortium extensions (1:1 with parent) ───────────────────────────

-- genome_assembly, condition, assay_info, file_type and file_type_detailed are
-- promoted verbatim and omitted here; replicate_info is kept because the
-- replicate columns are regex-extracted from it.
CREATE TABLE file_extra_fourdn (
    id_namespace         VARCHAR NOT NULL,
    local_id             VARCHAR NOT NULL,
    enriched_file_format VARCHAR,
    dataset              VARCHAR,
    biosource_name       VARCHAR,
    replicate_info       VARCHAR,
    cell_line_tier       VARCHAR,
    PRIMARY KEY (id_namespace, local_id),
    FOREIGN KEY (id_namespace, local_id) REFERENCES file (id_namespace, local_id)
);

CREATE TABLE file_extra_fourdn_extra_file (
    id_namespace VARCHAR NOT NULL,
    local_id     VARCHAR NOT NULL,
    ordinal      INTEGER NOT NULL,   -- position in extra_files[]
    href         VARCHAR,
    md5sum       VARCHAR,
    file_size    BIGINT,
    file_format  VARCHAR,            -- 4DN token ("pairs_px2", "bai"), not EDAM
    PRIMARY KEY (id_namespace, local_id, ordinal),
    FOREIGN KEY (id_namespace, local_id) REFERENCES file (id_namespace, local_id)
);

-- assembly, genome_annotation, output_type, file_format_type and both replicate
-- columns are promoted verbatim and omitted here.
CREATE TABLE file_extra_encode (
    id_namespace         VARCHAR NOT NULL,
    local_id             VARCHAR NOT NULL,
    annotation_type      VARCHAR,
    organism             VARCHAR,
    controlled_by        VARCHAR,
    s3_uri               VARCHAR,
    azure_url            VARCHAR,
    file_analysis_title  VARCHAR,
    file_analysis_status VARCHAR,
    read_length          VARCHAR,
    mapped_read_length   VARCHAR,
    run_type             VARCHAR,
    paired_end           VARCHAR,
    paired_with          VARCHAR,
    index_of             VARCHAR,
    derived_from         VARCHAR,
    audit_warning        VARCHAR,
    audit_not_compliant  VARCHAR,
    audit_error          VARCHAR,
    PRIMARY KEY (id_namespace, local_id),
    FOREIGN KEY (id_namespace, local_id) REFERENCES file (id_namespace, local_id)
);

CREATE TABLE file_extra_hubmap (
    id_namespace    VARCHAR NOT NULL,
    local_id        VARCHAR NOT NULL,
    rel_path        VARCHAR,
    is_data_product BOOLEAN,
    PRIMARY KEY (id_namespace, local_id),
    FOREIGN KEY (id_namespace, local_id) REFERENCES file (id_namespace, local_id)
);

-- experiment_type and lab are promoted and omitted here.
CREATE TABLE collection_extra_fourdn (
    id_namespace             VARCHAR NOT NULL,
    local_id                 VARCHAR NOT NULL,
    display_title            VARCHAR,
    targeted_factor          VARCHAR[],
    digestion_enzyme         VARCHAR,
    crosslinking_method      VARCHAR,
    crosslinking_temperature VARCHAR,   -- numeric upstream, stringified on ingest
    crosslinking_time        VARCHAR,
    ligation_temperature     VARCHAR,
    ligation_volume          VARCHAR,
    ligation_time            VARCHAR,
    digestion_temperature    VARCHAR,
    digestion_time           VARCHAR,
    tagging_method           VARCHAR,
    fragmentation_method     VARCHAR,
    biotin_removed           VARCHAR,
    library_prep_kit         VARCHAR,
    average_fragment_size    VARCHAR,
    fragment_size_range      VARCHAR,   -- a genuine range string, e.g. "200-400"
    status                   VARCHAR,
    date_created             TIMESTAMP,
    PRIMARY KEY (id_namespace, local_id),
    FOREIGN KEY (id_namespace, local_id)
        REFERENCES collection (id_namespace, local_id)
);

-- experiment_target and analyte_class are promoted and omitted here.
CREATE TABLE collection_extra_encode (
    id_namespace               VARCHAR NOT NULL,
    local_id                   VARCHAR NOT NULL,
    project                    VARCHAR,
    platform                   VARCHAR,
    dbxrefs                    VARCHAR,
    rbns_protein_concentration VARCHAR,
    annotation_type            VARCHAR,
    software_used              VARCHAR,
    encyclopedia_version       VARCHAR,
    PRIMARY KEY (id_namespace, local_id),
    FOREIGN KEY (id_namespace, local_id)
        REFERENCES collection (id_namespace, local_id)
);

-- dataset_type (-> experiment_type) and analyte_class are promoted and omitted.
CREATE TABLE collection_extra_hubmap (
    id_namespace    VARCHAR NOT NULL,
    local_id        VARCHAR NOT NULL,
    pipeline        VARCHAR,
    processing      VARCHAR,
    group_name      VARCHAR,
    visualization   BOOLEAN,
    vitessce_hints  VARCHAR[],
    metadata        JSON,     -- open-ended assay-specific dict; no fixed columns
    PRIMARY KEY (id_namespace, local_id),
    FOREIGN KEY (id_namespace, local_id)
        REFERENCES collection (id_namespace, local_id)
);

CREATE TABLE biosample_extra_encode (
    id_namespace                    VARCHAR NOT NULL,
    local_id                        VARCHAR NOT NULL,
    biosample_type                  VARCHAR,
    life_stage                      VARCHAR,
    age                             VARCHAR,   -- free text upstream ("unknown")
    age_units                       VARCHAR,
    biosample_treatments            VARCHAR,
    biosample_treatments_amount     VARCHAR,
    biosample_treatments_duration   VARCHAR,
    biosample_genetic_modifications VARCHAR,
    library_made_from               VARCHAR,
    library_depleted_in             VARCHAR,
    library_extraction_method       VARCHAR,
    library_lysis_method            VARCHAR,
    library_crosslinking_method     VARCHAR,
    library_strand_specific         VARCHAR,
    library_fragmentation_method    VARCHAR,
    library_size_range              VARCHAR,
    PRIMARY KEY (id_namespace, local_id),
    FOREIGN KEY (id_namespace, local_id)
        REFERENCES biosample (id_namespace, local_id)
);

CREATE TABLE subject_extra_hubmap (
    id_namespace           VARCHAR NOT NULL,
    local_id               VARCHAR NOT NULL,
    age_value              DOUBLE,
    age_unit               VARCHAR,
    sex                    VARCHAR,
    race                   VARCHAR,
    body_mass_index_value  DOUBLE,
    body_mass_index_unit   VARCHAR,
    cause_of_death         VARCHAR,
    death_event            VARCHAR,
    mechanism_of_injury    VARCHAR,
    medical_history        VARCHAR[],
    social_history         VARCHAR[],
    height_value           DOUBLE,
    height_unit            VARCHAR,
    weight_value           DOUBLE,
    weight_unit            VARCHAR,
    PRIMARY KEY (id_namespace, local_id),
    FOREIGN KEY (id_namespace, local_id)
        REFERENCES subject (id_namespace, local_id)
);

-- ─── convenience views ─────────────────────────────────────────────────────

-- One row per file with the ontology term names and every per-consortium
-- file extension column inlined, so the common read needs no joins.
CREATE VIEW v_file AS
SELECT
    f.*,
    d.dcc_abbreviation,
    ff.name                 AS file_format_name,
    dt.name                 AS data_type_name,
    aty.name                AS assay_type_name,
    x4.enriched_file_format,
    x4.dataset,
    x4.biosource_name,
    x4.replicate_info,
    x4.cell_line_tier,
    xe.annotation_type,
    xe.organism,
    xe.controlled_by,
    xe.s3_uri,
    xe.azure_url,
    xe.file_analysis_title,
    xe.file_analysis_status,
    xe.read_length,
    xe.mapped_read_length,
    xe.run_type,
    xe.paired_end,
    xe.paired_with,
    xe.index_of,
    xe.derived_from,
    xe.audit_warning,
    xe.audit_not_compliant,
    xe.audit_error,
    xh.rel_path,
    xh.is_data_product
FROM file AS f
JOIN dcc AS d ON d.submission = f.submission
LEFT JOIN file_format AS ff ON ff.id = f.file_format
LEFT JOIN data_type   AS dt ON dt.id = f.data_type
LEFT JOIN assay_type  AS aty ON aty.id = f.assay_type
LEFT JOIN file_extra_fourdn AS x4
    ON x4.id_namespace = f.id_namespace AND x4.local_id = f.local_id
LEFT JOIN file_extra_encode AS xe
    ON xe.id_namespace = f.id_namespace AND xe.local_id = f.local_id
LEFT JOIN file_extra_hubmap AS xh
    ON xh.id_namespace = f.id_namespace AND xh.local_id = f.local_id;

-- The file x collection x biosample x subject grain -- "which files came from
-- heart tissue". Explicitly a fan-out view: a file row repeats once per
-- biosample/subject path, so never SUM(size_in_bytes) over it.
CREATE VIEW v_file_biosample AS
SELECT
    f.submission,
    f.id_namespace          AS file_id_namespace,
    f.local_id              AS file_local_id,
    f.filename,
    f.accession_id          AS file_accession_id,
    c.id_namespace          AS collection_id_namespace,
    c.local_id              AS collection_local_id,
    c.name                  AS collection_name,
    c.experiment_type,
    b.id_namespace          AS biosample_id_namespace,
    b.local_id              AS biosample_local_id,
    a.id                    AS anatomy_id,
    a.name                  AS anatomy_name,
    s.id_namespace          AS subject_id_namespace,
    s.local_id              AS subject_local_id,
    t.name                  AS taxonomy_name
FROM file AS f
JOIN file_in_collection AS fic
    ON fic.file_id_namespace = f.id_namespace AND fic.file_local_id = f.local_id
JOIN collection AS c
    ON c.id_namespace = fic.collection_id_namespace
    AND c.local_id = fic.collection_local_id
LEFT JOIN biosample_in_collection AS bic
    ON bic.collection_id_namespace = c.id_namespace
    AND bic.collection_local_id = c.local_id
LEFT JOIN biosample AS b
    ON b.id_namespace = bic.biosample_id_namespace
    AND b.local_id = bic.biosample_local_id
LEFT JOIN anatomy AS a ON a.id = b.anatomy
LEFT JOIN biosample_from_subject AS bfs
    ON bfs.biosample_id_namespace = b.id_namespace
    AND bfs.biosample_local_id = b.local_id
LEFT JOIN subject AS s
    ON s.id_namespace = bfs.subject_id_namespace
    AND s.local_id = bfs.subject_local_id
LEFT JOIN ncbi_taxonomy AS t ON t.id = s.taxonomy;

-- Subject demographics coalesced across the C2M2 columns and HuBMAP's
-- parallel enrichment, with a *_source column per value so provenance
-- survives the coalesce. The loader never invents values; this view is
-- where the triplicated facts are reconciled, visibly.
CREATE VIEW v_subject_demographics AS
SELECT
    s.id_namespace,
    s.local_id,
    s.submission,
    COALESCE(s.sex, xh.sex) AS sex,
    CASE
        WHEN s.sex IS NOT NULL THEN 'subject'
        WHEN xh.sex IS NOT NULL THEN 'subject_extra_hubmap'
    END AS sex_source,
    COALESCE(s.age_at_enrollment, s.age_at_sampling, xh.age_value) AS age,
    CASE
        WHEN s.age_at_enrollment IS NOT NULL THEN 'subject.age_at_enrollment'
        WHEN s.age_at_sampling IS NOT NULL THEN 'subject.age_at_sampling'
        WHEN xh.age_value IS NOT NULL THEN 'subject_extra_hubmap.age_value'
    END AS age_source,
    -- The unit belongs to HuBMAP's age_value alone; a C2M2-sourced age
    -- (years by definition) must not be paired with it.
    CASE
        WHEN s.age_at_enrollment IS NULL AND s.age_at_sampling IS NULL
        THEN xh.age_unit
    END AS age_unit,
    COALESCE(r.races, CASE WHEN xh.race IS NOT NULL THEN [xh.race] END) AS race,
    CASE
        WHEN r.races IS NOT NULL THEN 'subject_race'
        WHEN xh.race IS NOT NULL THEN 'subject_extra_hubmap'
    END AS race_source,
    s.ethnicity
FROM subject AS s
LEFT JOIN subject_extra_hubmap AS xh
    ON xh.id_namespace = s.id_namespace AND xh.local_id = s.local_id
LEFT JOIN (
    SELECT subject_id_namespace, subject_local_id,
           list(race ORDER BY race) AS races
    FROM subject_race
    GROUP BY subject_id_namespace, subject_local_id
) AS r
    ON r.subject_id_namespace = s.id_namespace
    AND r.subject_local_id = s.local_id;
