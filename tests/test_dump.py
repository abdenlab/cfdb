"""Tests for the relational DuckDB dump of the ``files`` collection."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime

import duckdb
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from cfdb.dump import SCHEMA_VERSION, DumpError, DumpReport, dump_files

#: Every table schema.sql creates, by tier.
PROVENANCE_TABLES = {"dump_info", "dump_stat", "dump_conflict"}
ENTITY_TABLES = {
    "dcc",
    "project",
    "file_format",
    "data_type",
    "assay_type",
    "anatomy",
    "ncbi_taxonomy",
    "file",
    "collection",
    "biosample",
    "subject",
}
JUNCTION_TABLES = {
    "file_in_collection",
    "biosample_in_collection",
    "subject_in_collection",
    "biosample_from_subject",
    "collection_anatomy",
    "subject_race",
}
EXTENSION_TABLES = {
    "file_extra_fourdn",
    "file_extra_fourdn_extra_file",
    "file_extra_encode",
    "file_extra_hubmap",
    "collection_extra_fourdn",
    "collection_extra_encode",
    "collection_extra_hubmap",
    "biosample_extra_encode",
    "subject_extra_hubmap",
}
ALL_TABLES = PROVENANCE_TABLES | ENTITY_TABLES | JUNCTION_TABLES | EXTENSION_TABLES
ALL_VIEWS = {"v_file", "v_file_biosample", "v_subject_demographics"}

FOURDN_NS = "tag:4dn.org,2015:"
ENCODE_NS = "tag:encode.org,2020:"
HUBMAP_NS = "tag:hubmapconsortium.org,2023:"


def make_dcc(submission: str) -> dict:
    """Build the embedded dcc object for a document."""
    return {
        "id": f"cfde_registry_dcc:{submission}",
        "dcc_name": f"{submission} name",
        "dcc_abbreviation": submission.upper(),
        "contact_email": "contact@example.org",
        "contact_name": "Contact",
        "dcc_url": f"https://{submission}.example.org",
        "project_id_namespace": "",
        "project_local_id": "",
    }


def make_subject(ns: str, local_id: str, **overrides) -> dict:
    """Build a nested subject object."""
    subject = {
        "id_namespace": ns,
        "local_id": local_id,
        "sex": "female",
        "race": ["asian"],
        "taxonomy": {
            "id": "NCBI:txid9606",
            "name": "Homo sapiens",
            "clade": "species",
        },
    }
    subject.update(overrides)
    return subject


def make_collection(ns: str, local_id: str, **overrides) -> dict:
    """Build a nested collection object with one biosample and one subject."""
    collection = {
        "id_namespace": ns,
        "local_id": local_id,
        "name": f"Collection {local_id}",
        "lab": "Lab A",
        "experiment_type": "in situ Hi-C",
        "anatomy": [{"id": "UBERON:0000948", "name": "heart"}],
        "subjects": [make_subject(ns, f"{local_id}-subj")],
        "biosamples": [
            {
                "id_namespace": ns,
                "local_id": f"{local_id}-bio",
                "anatomy": {"id": "UBERON:0000948", "name": "heart"},
                "subjects": [make_subject(ns, f"{local_id}-subj")],
            }
        ],
    }
    collection.update(overrides)
    return collection


def make_fourdn_doc(local_id: str = "file-1", **overrides) -> dict:
    """Build a complete 4DN files document."""
    doc = {
        "submission": "4dn",
        "id_namespace": FOURDN_NS,
        "local_id": local_id,
        "accession_id": "4dnfiabc123",
        "filename": f"{local_id}.mcool",
        "creation_time": "2021-03-04T05:06:07+00:00",
        "size_in_bytes": "123456",
        "md5": "aa",
        "project_id_namespace": FOURDN_NS,
        "project_local_id": "proj-1",
        "genome_assembly": "GRCh38",
        "output_type": "contact matrix",
        "output_type_detail": "contact matrix (mcool)",
        "assay_info": "Dilution Hi-C",
        "condition": "control",
        "dcc": make_dcc("4dn"),
        "project": {
            "id_namespace": FOURDN_NS,
            "local_id": "proj-1",
            "name": "Project One",
        },
        "file_format": {"id": "format:3590", "name": "HDF5", "description": "d"},
        "extra": {
            "fourdn": {
                "enriched_file_format": "mcool",
                "file_type": "contact matrix",
                "file_type_detailed": "contact matrix (mcool)",
                "genome_assembly": "GRCh38",
                "replicate_info": "Biorep 1, Techrep 1",
                "extra_files": [
                    {
                        "href": "/x",
                        "md5sum": "bb",
                        "file_size": "42",
                        "file_format": {"display_title": "pairs_px2"},
                    }
                ],
            }
        },
        "collections": [
            make_collection(
                FOURDN_NS,
                "exp-1",
                accession_id="4dnexabc",
                extra={
                    "fourdn": {
                        "display_title": "Exp 1",
                        "targeted_factor": ["CTCF protein"],
                        "digestion_enzyme": "DpnII",
                        "date_created": "2020-01-02T03:04:05+00:00",
                    }
                },
            )
        ],
    }
    doc.update(overrides)
    return doc


def make_encode_doc(local_id: str = "ENCFF001AAA", **overrides) -> dict:
    """Build a complete ENCODE files document."""
    doc = {
        "submission": "encode",
        "id_namespace": ENCODE_NS,
        "local_id": local_id,
        "accession_id": local_id.lower(),
        "filename": f"{local_id}.bed.gz",
        "size_in_bytes": 6262125716,
        "sha256": "cc",
        "genome_assembly": "GRCh38",
        "genome_annotation": "V29",
        "output_type": "peaks",
        "output_type_detail": "narrowPeak",
        "biological_replicates": "1",
        "technical_replicates": "1_1",
        "dcc": make_dcc("encode"),
        "file_format": {"id": "format:3003", "name": "BED"},
        "extra": {
            "encode": {
                "assembly": "GRCh38",
                "output_type": "peaks",
                "s3_uri": "s3://encode-public/x.bed.gz",
                "run_type": "single-ended",
                "audit_warning": "mild to moderate bottlenecking",
            }
        },
        "collections": [
            make_collection(
                ENCODE_NS,
                "ENCSR001AAA",
                experiment_type="TF ChIP-seq",
                experiment_target="CTCF",
                analyte_class="DNA",
                extra={
                    "encode": {
                        "project": "ENCODE",
                        "platform": "Illumina NovaSeq 6000",
                    }
                },
                biosamples=[
                    {
                        "id_namespace": ENCODE_NS,
                        "local_id": "ENCBS001AAA",
                        "anatomy": {"id": "UBERON:0002107", "name": "liver"},
                        "subjects": [make_subject(ENCODE_NS, "ENCDO001AAA")],
                        "extra": {
                            "encode": {
                                "biosample_type": "tissue",
                                "life_stage": "adult",
                                "library_made_from": "DNA",
                            }
                        },
                    }
                ],
            )
        ],
    }
    doc.update(overrides)
    return doc


def make_hubmap_doc(local_id: str = "hm-file-1", **overrides) -> dict:
    """Build a complete HuBMAP files document."""
    doc = {
        "submission": "hubmap",
        "id_namespace": HUBMAP_NS,
        "local_id": local_id,
        "filename": f"{local_id}.tsv",
        "size_in_bytes": "999",
        "md5": "dd",
        "status": "Published",
        "data_access_level": "public",
        "genome_assembly": "GRCh38",
        "dcc": make_dcc("hubmap"),
        "extra": {
            "hubmap": {
                "rel_path": "expr/some.tsv",
                "is_data_product": True,
            }
        },
        "collections": [
            make_collection(
                HUBMAP_NS,
                "HBM123.ABCD.456",
                experiment_type="RNAseq",
                analyte_class="RNA",
                extra={
                    "hubmap": {
                        "pipeline": "salmon-rnaseq",
                        "processing": "processed",
                        "visualization": True,
                        "vitessce_hints": ["is_sc", "rna"],
                        "metadata": {"assay_category": "sequence"},
                    }
                },
                subjects=[
                    make_subject(
                        HUBMAP_NS,
                        "donor-1",
                        sex=None,
                        race=[],
                        extra={
                            "hubmap": {
                                "sex": "Male",
                                "race": "White",
                                "age_value": 61.0,
                                "age_unit": "years",
                            }
                        },
                    )
                ],
                biosamples=[
                    {
                        "id_namespace": HUBMAP_NS,
                        "local_id": "HBM123.ABCD.456-bio",
                        "anatomy": {"id": "UBERON:0002113", "name": "kidney"},
                        "subjects": [
                            make_subject(
                                HUBMAP_NS,
                                "donor-1",
                                sex=None,
                                race=[],
                                extra={
                                    "hubmap": {
                                        "sex": "Male",
                                        "race": "White",
                                        "age_value": 61.0,
                                        "age_unit": "years",
                                    }
                                },
                            )
                        ],
                    }
                ],
            )
        ],
    }
    doc.update(overrides)
    return doc


def run_dump(docs: list[dict], **kwargs) -> tuple[duckdb.DuckDBPyConnection, DumpReport]:
    """Dump documents into a fresh in-memory DuckDB and return it."""
    con = duckdb.connect(":memory:")
    report = dump_files(docs, con, source_database="cfdb", **kwargs)
    return con, report


class TestDumpReport:
    def test_summary_should_render_sections_when_conflicts_and_counters_present(self):
        """Test the populated summary rendering.

        Given:
            A DumpReport with row counts, a non-empty conflict tally, and a
            non-zero counter.
        When:
            summary() is called.
        Then:
            It should render the per-table row lines plus the divergence and
            counter sections with their values.
        """
        # Arrange
        report = DumpReport(
            file_count=2,
            rows={"file": 2, "collection": 1},
            conflicts={"collection": 1},
            stats={"documents_invalid": 3},
        )

        # Act
        summary = report.summary()

        # Assert
        assert "files dumped: 2" in summary
        assert "file: 2 rows" in summary
        assert "divergent entity copies (first write kept):" in summary
        assert "collection: 1" in summary
        assert "counters:" in summary
        assert "documents_invalid: 3" in summary

    def test_summary_should_omit_sections_when_no_conflicts_or_counters(self):
        """Test empty-section suppression in the summary.

        Given:
            A DumpReport with no conflicts and only zero-valued counters.
        When:
            summary() is called.
        Then:
            It should omit both the divergence and counter section headers.
        """
        # Arrange
        report = DumpReport(
            file_count=1,
            rows={"file": 1},
            conflicts={},
            stats={"documents_invalid": 0},
        )

        # Act
        summary = report.summary()

        # Assert
        assert "divergent" not in summary
        assert "counters:" not in summary


def test_dump_files_should_create_all_tables():
    """Test schema bootstrap.

    Given:
        No documents at all.
    When:
        dump_files runs against a fresh in-memory DuckDB.
    Then:
        It should create every table and view the schema of record defines.
    """
    # Act
    con, _ = run_dump([])

    # Assert
    tables = {row[0] for row in con.execute("SELECT table_name FROM duckdb_tables()").fetchall()}
    views = {
        row[0]
        for row in con.execute(
            "SELECT view_name FROM duckdb_views() WHERE NOT internal"
        ).fetchall()
    }
    assert tables == ALL_TABLES
    assert ALL_VIEWS <= views


def test_dump_files_should_flatten_nested_entities_when_doc_spans_all_levels():
    """Test flattening of one fully nested document.

    Given:
        One 4DN document carrying a collection with anatomy, a biosample,
        and a subject shared between the collection and the biosample.
    When:
        The document is dumped.
    Then:
        It should recover one row per entity and one row per C2M2 junction.
    """
    # Arrange
    doc = make_fourdn_doc()

    # Act
    con, report = run_dump([doc])

    # Assert
    assert report.file_count == 1
    for table in (
        "file",
        "collection",
        "biosample",
        "subject",
        "file_in_collection",
        "biosample_in_collection",
        "subject_in_collection",
        "biosample_from_subject",
        "collection_anatomy",
        "subject_race",
        "file_extra_fourdn",
        "file_extra_fourdn_extra_file",
        "collection_extra_fourdn",
    ):
        assert report.rows[table] == 1, table


def test_dump_files_should_collapse_repeated_entities_when_shared_across_files():
    """Test entity dedup across documents.

    Given:
        Two 4DN documents embedding byte-identical copies of the same
        collection.
    When:
        Both documents are dumped.
    Then:
        It should write one collection row, two file rows, and two
        file_in_collection rows, with no conflicts recorded.
    """
    # Arrange
    docs = [make_fourdn_doc("file-1"), make_fourdn_doc("file-2")]

    # Act
    con, report = run_dump(docs)

    # Assert
    assert report.rows["file"] == 2
    assert report.rows["collection"] == 1
    assert report.rows["file_in_collection"] == 2
    assert report.conflicts == {}


def test_dump_files_should_record_conflict_when_repeated_entity_diverges():
    """Test divergence reporting on repeated entities.

    Given:
        Two documents embedding the same collection key with different
        lab values.
    When:
        Both documents are dumped.
    Then:
        It should keep the first copy's value and record the divergence
        in dump_conflict rather than silently resolving it.
    """
    # Arrange
    first = make_fourdn_doc("file-1")
    second = make_fourdn_doc("file-2")
    second["collections"][0]["lab"] = "Lab B"

    # Act
    con, report = run_dump([first, second])

    # Assert
    assert report.conflicts["collection"] == 1
    assert con.execute("SELECT lab FROM collection").fetchone()[0] == "Lab A"
    conflict = con.execute(
        "SELECT table_name, column_name, kept_value, discarded_value FROM dump_conflict"
    ).fetchone()
    assert conflict == ("collection", "lab", "Lab A", "Lab B")
    assert con.execute(
        "SELECT value FROM dump_stat WHERE name = 'conflicts_collection'"
    ).fetchone()[0] == 1


def test_dump_files_should_union_junction_rows_when_copies_disagree():
    """Test union semantics for junction rows.

    Given:
        Two documents whose copies of one collection carry different
        biosample lists.
    When:
        Both documents are dumped.
    Then:
        It should keep the union of biosample_in_collection rows, so a
        relationship present in only one copy is not silently dropped.
    """
    # Arrange
    first = make_fourdn_doc("file-1")
    second = make_fourdn_doc("file-2")
    second["collections"][0]["biosamples"].append(
        {
            "id_namespace": FOURDN_NS,
            "local_id": "exp-1-bio-extra",
            "anatomy": {"id": "UBERON:0000948", "name": "heart"},
            "subjects": [],
        }
    )

    # Act
    con, report = run_dump([first, second])

    # Assert
    assert report.rows["biosample"] == 2
    assert report.rows["biosample_in_collection"] == 2


def test_dump_files_should_raise_when_key_maps_to_two_submissions():
    """Test the key-uniqueness assertion.

    Given:
        Two documents embedding the same collection key under two
        different submissions.
    When:
        Both documents are dumped.
    Then:
        It should raise DumpError naming both submissions, because the
        schema's global natural keys no longer hold.
    """
    # Arrange
    first = make_fourdn_doc("file-1")
    second = make_encode_doc("ENCFF001AAA")
    second["collections"][0]["id_namespace"] = FOURDN_NS
    second["collections"][0]["local_id"] = "exp-1"

    # Act & assert
    with pytest.raises(DumpError, match="two submissions"):
        run_dump([first, second])


def test_dump_files_should_raise_when_duplicate_file_keys():
    """Test the duplicate-file-key assertion.

    Given:
        Two documents carrying the same (id_namespace, local_id) file key.
    When:
        Both documents are dumped.
    Then:
        It should raise DumpError rather than write an ambiguous file row.
    """
    # Arrange
    docs = [make_fourdn_doc("file-1"), make_fourdn_doc("file-1")]

    # Act & assert
    with pytest.raises(DumpError, match="duplicate file keys"):
        run_dump(docs)


def test_dump_files_should_enforce_foreign_keys_when_junction_names_absent_parent():
    """Test that the schema's declared foreign keys are enforced.

    Given:
        A completed dump and a file_in_collection row naming a collection
        no document carried.
    When:
        The row is inserted directly.
    Then:
        It should raise ConstraintException, because the fails-at-write-time
        promise rests on enforcement rather than declaration.
    """
    # Arrange
    con, _ = run_dump([make_fourdn_doc()])

    # Act & assert
    with pytest.raises(duckdb.ConstraintException):
        con.execute(
            "INSERT INTO file_in_collection VALUES (?, ?, ?, ?)",
            [FOURDN_NS, "file-1", FOURDN_NS, "no-such-collection"],
        )


def test_dump_files_should_raise_when_stub_upgraded_across_submissions():
    """Test the key-uniqueness assertion on the stub-upgrade path.

    Given:
        A document stubbing a project via a biosample FK, followed by a
        document from another submission carrying the real project object
        for the same key.
    When:
        Both documents are dumped.
    Then:
        It should raise DumpError naming both submissions instead of
        silently upgrading the stub across consortia.
    """
    # Arrange
    first = make_fourdn_doc(project=None, project_id_namespace="", project_local_id="")
    first["collections"][0]["biosamples"][0]["project_id_namespace"] = FOURDN_NS
    first["collections"][0]["biosamples"][0]["project_local_id"] = "proj-x"
    second = make_encode_doc(
        project={"id_namespace": FOURDN_NS, "local_id": "proj-x", "name": "P"},
        project_id_namespace=FOURDN_NS,
        project_local_id="proj-x",
    )

    # Act & assert
    with pytest.raises(DumpError, match="two submissions"):
        run_dump([first, second])


def test_dump_files_should_raise_when_stub_references_real_row_across_submissions():
    """Test the key-uniqueness assertion on the incoming-stub path.

    Given:
        A document carrying a real project object, followed by a document
        from another submission whose biosample FK stubs the same key.
    When:
        Both documents are dumped.
    Then:
        It should raise DumpError naming both submissions instead of
        silently dropping the cross-consortium stub.
    """
    # Arrange
    first = make_fourdn_doc()
    second = make_encode_doc()
    second["collections"][0]["biosamples"][0]["project_id_namespace"] = FOURDN_NS
    second["collections"][0]["biosamples"][0]["project_local_id"] = "proj-1"

    # Act & assert
    with pytest.raises(DumpError, match="two submissions"):
        run_dump([first, second])


def test_dump_files_should_coerce_string_size_when_source_is_tsv_typed():
    """Test numeric coercion of string-typed sizes.

    Given:
        A 4DN document whose size_in_bytes and sidecar file_size are
        strings, as the C2M2 TSV path loads them.
    When:
        The document is dumped.
    Then:
        It should store both as integers with no coercion failures
        counted.
    """
    # Arrange
    doc = make_fourdn_doc()

    # Act
    con, report = run_dump([doc])

    # Assert
    assert con.execute("SELECT size_in_bytes FROM file").fetchone()[0] == 123456
    assert (
        con.execute("SELECT file_size FROM file_extra_fourdn_extra_file").fetchone()[0]
        == 42
    )
    assert "size_in_bytes_uncoercible" not in report.stats


def test_dump_files_should_count_failure_when_size_uncoercible():
    """Test coercion-failure accounting.

    Given:
        A document whose size_in_bytes is a non-numeric string.
    When:
        The document is dumped.
    Then:
        It should store NULL, keep the document, and count the failure in
        dump_stat.
    """
    # Arrange
    doc = make_fourdn_doc(size_in_bytes="N/A")

    # Act
    con, report = run_dump([doc])

    # Assert
    assert report.file_count == 1
    assert con.execute("SELECT size_in_bytes FROM file").fetchone()[0] is None
    assert report.stats["size_in_bytes_uncoercible"] == 1
    assert con.execute(
        "SELECT value FROM dump_stat WHERE name = 'size_in_bytes_uncoercible'"
    ).fetchone()[0] == 1


def test_dump_files_should_null_term_fk_when_term_id_empty():
    """Test the term-id guard on the file-level FK columns.

    Given:
        A document whose file_format term carries an empty id.
    When:
        The document is dumped.
    Then:
        It should complete with file.file_format NULL, no file_format row,
        and the skip counted in dump_stat.
    """
    # Arrange
    doc = make_fourdn_doc(file_format={"id": "", "name": "HDF5"})

    # Act
    con, report = run_dump([doc])

    # Assert
    assert report.file_count == 1
    assert con.execute("SELECT file_format FROM file").fetchone()[0] is None
    assert con.execute("SELECT count(*) FROM file_format").fetchone()[0] == 0
    assert report.stats["term_id_missing"] == 1


def test_dump_files_should_skip_term_and_null_fk_when_term_name_empty():
    """Test the nameless-term guard.

    Given:
        A document whose file_format term carries an id but a blank name.
    When:
        The document is dumped.
    Then:
        It should complete with the term skipped, file.file_format NULL,
        and the skip counted in dump_stat, because a nameless term cannot
        satisfy the NOT NULL name column.
    """
    # Arrange
    doc = make_fourdn_doc(file_format={"id": "format:3590", "name": ""})

    # Act
    con, report = run_dump([doc])

    # Assert
    assert report.file_count == 1
    assert con.execute("SELECT file_format FROM file").fetchone()[0] is None
    assert con.execute("SELECT count(*) FROM file_format").fetchone()[0] == 0
    assert report.stats["term_name_missing"] == 1


def test_dump_files_should_populate_promoted_columns_when_consortium_fills_them():
    """Test the promoted cross-consortium common columns.

    Given:
        One document per consortium, each filling the common slots its
        ingest promotes.
    When:
        All three are dumped together.
    Then:
        It should populate each file and collection common column from
        that consortium's source field.
    """
    # Arrange
    docs = [make_fourdn_doc(), make_encode_doc(), make_hubmap_doc()]

    # Act
    con, report = run_dump(docs)

    # Assert
    rows = {
        submission: (assembly, annotation, output_type, condition)
        for submission, assembly, annotation, output_type, condition in con.execute(
            'SELECT submission, genome_assembly, genome_annotation, output_type, "condition" FROM file'
        ).fetchall()
    }
    assert rows["4dn"] == ("GRCh38", None, "contact matrix", "control")
    assert rows["encode"] == ("GRCh38", "V29", "peaks", None)
    assert rows["hubmap"] == ("GRCh38", None, None, None)
    collections = {
        submission: (experiment_type, experiment_target, analyte_class)
        for submission, experiment_type, experiment_target, analyte_class in con.execute(
            "SELECT submission, experiment_type, experiment_target, analyte_class FROM collection"
        ).fetchall()
    }
    assert collections["4dn"] == ("in situ Hi-C", None, None)
    assert collections["encode"] == ("TF ChIP-seq", "CTCF", "DNA")
    assert collections["hubmap"] == ("RNAseq", None, "RNA")


def test_dump_files_should_omit_promoted_fields_from_extension_tables():
    """Test that extension tables carry only the unpromoted residue.

    Given:
        The dumped schema.
    When:
        The extension tables' columns are inspected.
    Then:
        It should omit verbatim-promoted fields and keep replicate_info,
        whose promotion is derived rather than verbatim.
    """
    # Arrange
    con, _ = run_dump([make_fourdn_doc()])

    # Act
    columns = {
        row[0]
        for row in con.execute(
            "SELECT column_name FROM duckdb_columns() WHERE table_name = 'file_extra_fourdn'"
        ).fetchall()
    }

    # Assert
    assert "replicate_info" in columns
    assert "file_type" not in columns
    assert "file_type_detailed" not in columns
    assert "genome_assembly" not in columns


def test_dump_files_should_skip_and_count_invalid_documents():
    """Test invalid-document handling.

    Given:
        One valid document and one document with no dcc.
    When:
        Both are dumped.
    Then:
        It should load the valid document, skip the invalid one, and
        count the skip in dump_stat.
    """
    # Arrange
    invalid = make_fourdn_doc("file-2")
    del invalid["dcc"]

    # Act
    con, report = run_dump([make_fourdn_doc("file-1"), invalid])

    # Assert
    assert report.file_count == 1
    assert report.stats["documents_invalid"] == 1
    assert con.execute(
        "SELECT value FROM dump_stat WHERE name = 'documents_invalid'"
    ).fetchone()[0] == 1


def test_dump_files_should_write_provenance():
    """Test the dump_info provenance row.

    Given:
        One document and an explicit submissions filter.
    When:
        The document is dumped.
    Then:
        It should write one dump_info row carrying the schema version,
        the filter, and the file count.
    """
    # Arrange
    doc = make_fourdn_doc()

    # Act
    con, _ = run_dump([doc], submissions=["4dn"])

    # Assert
    row = con.execute(
        "SELECT schema_version, source_database, source_collection, submissions, file_count"
        " FROM dump_info"
    ).fetchone()
    assert row == (SCHEMA_VERSION, "cfdb", "files", ["4dn"], 1)


def test_dump_files_should_not_record_foreign_repo_commit_when_run_from_one(
    tmp_path, monkeypatch
):
    """Test the cfdb_commit provenance anchor.

    Given:
        The process CWD pointed at an unrelated git repository with a
        known HEAD.
    When:
        A document is dumped.
    Then:
        It should record either cfdb's own commit or NULL in
        dump_info.cfdb_commit, never the unrelated repository's HEAD.
    """
    # Arrange
    env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "commit", "-q", "--allow-empty", "-m", "scratch"],
        cwd=tmp_path,
        check=True,
        env={**env, "HOME": str(tmp_path), "GIT_CONFIG_GLOBAL": "/dev/null"},
    )
    scratch_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=tmp_path, capture_output=True, text=True
    ).stdout.strip()
    monkeypatch.chdir(tmp_path)

    # Act
    con, _ = run_dump([make_fourdn_doc()])

    # Assert
    recorded = con.execute("SELECT cfdb_commit FROM dump_info").fetchone()[0]
    assert recorded != scratch_sha


def test_v_subject_demographics_should_coalesce_with_source_when_hubmap_extra_present():
    """Test provenance-carrying coalescing of subject demographics.

    Given:
        A HuBMAP subject whose C2M2 sex and race are empty but whose
        enrichment carries both.
    When:
        v_subject_demographics is queried.
    Then:
        It should surface the enrichment values with source columns
        naming subject_extra_hubmap.
    """
    # Arrange
    con, _ = run_dump([make_hubmap_doc()])

    # Act
    row = con.execute(
        "SELECT sex, sex_source, race, race_source, age, age_source"
        " FROM v_subject_demographics"
    ).fetchone()

    # Assert
    assert row == (
        "Male",
        "subject_extra_hubmap",
        ["White"],
        "subject_extra_hubmap",
        61.0,
        "subject_extra_hubmap.age_value",
    )


def test_v_subject_demographics_should_null_age_unit_when_age_from_c2m2_column():
    """Test the source-consistency of the age_unit column.

    Given:
        A subject carrying both a C2M2 age_at_enrollment and divergent
        HuBMAP enrichment age_value and age_unit.
    When:
        v_subject_demographics is queried.
    Then:
        It should pair the C2M2 age with a NULL age_unit, because the
        unit belongs to HuBMAP's age_value alone.
    """
    # Arrange
    doc = make_hubmap_doc()
    for subject in (
        doc["collections"][0]["subjects"][0],
        doc["collections"][0]["biosamples"][0]["subjects"][0],
    ):
        subject["age_at_enrollment"] = 30.0
        subject["extra"]["hubmap"]["age_value"] = 360.0
        subject["extra"]["hubmap"]["age_unit"] = "months"
    con, _ = run_dump([doc])

    # Act
    row = con.execute(
        "SELECT age, age_source, age_unit FROM v_subject_demographics"
    ).fetchone()

    # Assert
    assert row == (30.0, "subject.age_at_enrollment", None)


@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    memberships=st.lists(
        st.sets(st.integers(min_value=0, max_value=3), min_size=1, max_size=3),
        min_size=1,
        max_size=8,
    )
)
def test_dump_files_should_preserve_cardinalities_with_arbitrary_sharing(memberships):
    """Test flattening invariants over arbitrary file/collection sharing.

    Given:
        Generated batches of files, each a member of an arbitrary
        non-empty subset of a shared collection pool.
    When:
        The batch is dumped.
    Then:
        It should write exactly one row per distinct file and collection,
        and one file_in_collection row per membership.
    """
    # Arrange
    docs = []
    for index, collections in enumerate(memberships):
        doc = make_fourdn_doc(f"file-{index}")
        doc["collections"] = [
            make_collection(FOURDN_NS, f"exp-{c}") for c in sorted(collections)
        ]
        docs.append(doc)
    expected_collections = {c for collections in memberships for c in collections}
    expected_memberships = sum(len(collections) for collections in memberships)

    # Act
    con, report = run_dump(docs)

    # Assert
    assert report.rows["file"] == len(memberships)
    assert report.rows["collection"] == len(expected_collections)
    assert report.rows["file_in_collection"] == expected_memberships


def test_dump_files_should_null_and_count_when_timestamps_unparseable():
    """Test the timestamp coercion-failure policy.

    Given:
        A 4DN document whose file creation_time and collection-extra
        date_created are non-ISO strings.
    When:
        The document is dumped.
    Then:
        It should store NULL in both columns, keep the document, and count
        each failure in the report stats and dump_stat.
    """
    # Arrange
    doc = make_fourdn_doc(creation_time="not-a-date")
    doc["collections"][0]["extra"]["fourdn"]["date_created"] = "also-not-a-date"

    # Act
    con, report = run_dump([doc])

    # Assert
    assert report.file_count == 1
    assert con.execute("SELECT creation_time FROM file").fetchone()[0] is None
    assert (
        con.execute("SELECT date_created FROM collection_extra_fourdn").fetchone()[0]
        is None
    )
    assert report.stats["creation_time_unparseable"] == 1
    assert report.stats["date_created_unparseable"] == 1
    counters = dict(
        con.execute(
            "SELECT name, value FROM dump_stat WHERE name LIKE '%_unparseable'"
        ).fetchall()
    )
    assert counters == {
        "creation_time_unparseable": 1,
        "date_created_unparseable": 1,
    }


def test_dump_files_should_normalize_to_utc_when_creation_time_offset():
    """Test timezone normalization of TIMESTAMP columns.

    Given:
        A document whose creation_time carries a +05:00 offset.
    When:
        The document is dumped.
    Then:
        It should store the UTC-equivalent naive timestamp.
    """
    # Arrange
    doc = make_fourdn_doc(creation_time="2021-03-04T10:06:07+05:00")

    # Act
    con, _ = run_dump([doc])

    # Assert
    stored = con.execute("SELECT creation_time FROM file").fetchone()[0]
    assert stored == datetime(2021, 3, 4, 5, 6, 7)


def test_dump_files_should_dedupe_terms_globally_when_descriptions_diverge():
    """Test ontology global dedup and divergence reporting.

    Given:
        A 4DN and an ENCODE document sharing one file_format id with
        divergent descriptions.
    When:
        Both documents are dumped.
    Then:
        It should write one globally deduped term row keeping the first
        description, and record the divergence in dump_conflict rather
        than raising.
    """
    # Arrange
    first = make_fourdn_doc(
        file_format={"id": "format:3003", "name": "BED", "description": "A"}
    )
    second = make_encode_doc()
    second["file_format"] = {"id": "format:3003", "name": "BED", "description": "B"}

    # Act
    con, report = run_dump([first, second])

    # Assert
    assert report.rows["file_format"] == 1
    assert con.execute("SELECT description FROM file_format").fetchone()[0] == "A"
    assert report.conflicts["file_format"] == 1
    conflict = con.execute(
        "SELECT table_name, natural_key, column_name, kept_value, discarded_value"
        " FROM dump_conflict"
    ).fetchone()
    assert conflict == ("file_format", "format:3003", "description", "A", "B")


def test_dump_files_should_write_project_stub_when_fk_has_no_object():
    """Test the project stub-row policy.

    Given:
        A document with no joined project object whose biosample carries a
        project foreign key.
    When:
        The document is dumped.
    Then:
        It should write an all-NULL-attribute stub project row keyed by the
        FK, count it, and keep foreign-key integrity.
    """
    # Arrange
    doc = make_fourdn_doc(project=None, project_id_namespace="", project_local_id="")
    doc["collections"][0]["biosamples"][0]["project_id_namespace"] = FOURDN_NS
    doc["collections"][0]["biosamples"][0]["project_local_id"] = "proj-stub"

    # Act
    con, report = run_dump([doc])

    # Assert
    assert report.stats["project_stubs_created"] == 1
    assert con.execute("SELECT local_id, name FROM project").fetchall() == [
        ("proj-stub", None)
    ]
    assert (
        con.execute("SELECT project_local_id FROM biosample").fetchone()[0]
        == "proj-stub"
    )
    assert con.execute("SELECT project_local_id FROM file").fetchone()[0] is None


def test_dump_files_should_upgrade_project_stub_when_real_object_arrives():
    """Test the stub-upgrade path.

    Given:
        A stub-creating document followed by a document carrying the real
        project object for the same key.
    When:
        Both documents are dumped in that order.
    Then:
        It should upgrade the stub to the real row with no conflict
        recorded.
    """
    # Arrange
    first = make_fourdn_doc("file-1", project=None)
    second = make_fourdn_doc("file-2")

    # Act
    con, report = run_dump([first, second])

    # Assert
    assert report.rows["project"] == 1
    assert con.execute("SELECT name FROM project").fetchone()[0] == "Project One"
    assert report.conflicts == {}
    assert report.stats["project_stubs_created"] == 1


def test_dump_files_should_bound_conflict_samples_when_cap_reached():
    """Test the conflict sample cap.

    Given:
        Three documents embedding one collection whose lab diverges in each
        copy, dumped with conflict_sample_cap=1.
    When:
        The documents are dumped.
    Then:
        It should write exactly one dump_conflict row while the conflict
        tally keeps counting both divergent copies.
    """
    # Arrange
    docs = [make_fourdn_doc(f"file-{i}") for i in range(3)]
    docs[1]["collections"][0]["lab"] = "Lab B"
    docs[2]["collections"][0]["lab"] = "Lab C"

    # Act
    con, report = run_dump(docs, conflict_sample_cap=1)

    # Assert
    assert report.conflicts["collection"] == 2
    assert con.execute("SELECT count(*) FROM dump_conflict").fetchone()[0] == 1


def test_dump_files_should_record_seen_submissions_when_filter_omitted():
    """Test the dump_info submissions fallback.

    Given:
        One document per consortium and no submissions argument.
    When:
        The documents are dumped.
    Then:
        It should record the sorted seen submissions in dump_info.
    """
    # Arrange
    docs = [make_hubmap_doc(), make_fourdn_doc(), make_encode_doc()]

    # Act
    con, _ = run_dump(docs)

    # Assert
    assert con.execute("SELECT submissions FROM dump_info").fetchone()[0] == [
        "4dn",
        "encode",
        "hubmap",
    ]


def test_dump_files_should_skip_extension_rows_when_all_fields_none():
    """Test all-None extension-row suppression.

    Given:
        A document whose file-level and collection-level extra submodels
        carry no values at all.
    When:
        The document is dumped.
    Then:
        It should write no extension rows for either level.
    """
    # Arrange
    doc = make_fourdn_doc(extra={"fourdn": {}})
    doc["collections"][0]["extra"] = {"fourdn": {}}

    # Act
    _, report = run_dump([doc])

    # Assert
    assert report.rows["file_extra_fourdn"] == 0
    assert report.rows["file_extra_fourdn_extra_file"] == 0
    assert report.rows["collection_extra_fourdn"] == 0


def test_dump_files_should_roundtrip_json_and_list_columns():
    """Test JSON and list column fidelity.

    Given:
        A HuBMAP document whose collection enrichment carries an assay
        metadata dict and a vitessce_hints list.
    When:
        The document is dumped.
    Then:
        It should round-trip the dict through the JSON column and the list
        through the VARCHAR[] column unchanged.
    """
    # Arrange
    doc = make_hubmap_doc()

    # Act
    con, _ = run_dump([doc])

    # Assert
    metadata, hints = con.execute(
        "SELECT metadata, vitessce_hints FROM collection_extra_hubmap"
    ).fetchone()
    assert json.loads(metadata) == {"assay_category": "sequence"}
    assert hints == ["is_sc", "rna"]


def test_dump_files_should_order_extra_files_by_source_position():
    """Test sidecar child-table ordinal ordering.

    Given:
        A 4DN document with two extra_files sidecars.
    When:
        The document is dumped.
    Then:
        It should write one row per sidecar with ordinals matching source
        order.
    """
    # Arrange
    doc = make_fourdn_doc()
    doc["extra"]["fourdn"]["extra_files"].append(
        {
            "href": "/y",
            "md5sum": "cc",
            "file_size": "43",
            "file_format": {"display_title": "bai"},
        }
    )

    # Act
    con, _ = run_dump([doc])

    # Assert
    rows = con.execute(
        "SELECT ordinal, href FROM file_extra_fourdn_extra_file ORDER BY ordinal"
    ).fetchall()
    assert rows == [(0, "/x"), (1, "/y")]


def test_dump_files_should_preserve_counts_when_batch_size_forces_flushes():
    """Test the staging flush path.

    Given:
        Three documents sharing one collection, dumped with batch_size=1.
    When:
        The documents are dumped.
    Then:
        It should produce the same row counts as the default batch size.
    """
    # Arrange
    docs = [make_fourdn_doc(f"file-{i}") for i in range(3)]

    # Act
    _, report = run_dump(docs, batch_size=1)

    # Assert
    assert report.rows["file"] == 3
    assert report.rows["collection"] == 1
    assert report.rows["file_in_collection"] == 3


def test_dump_files_should_keep_empty_compression_format():
    """Test compression_format empty-string semantics.

    Given:
        A document whose compression_format is the empty string, meaning
        "nothing recorded" rather than "uncompressed".
    When:
        The document is dumped.
    Then:
        It should store the empty string, not NULL.
    """
    # Arrange
    doc = make_fourdn_doc(compression_format="")

    # Act
    con, _ = run_dump([doc])

    # Assert
    assert con.execute("SELECT compression_format FROM file").fetchone()[0] == ""


def test_v_file_should_inline_extension_columns_per_consortium():
    """Test v_file extension and term inlining.

    Given:
        One ENCODE and one HuBMAP document.
    When:
        v_file is queried.
    Then:
        It should inline each row's own extension columns, term names, and
        dcc abbreviation, with the other consortium's extension columns
        NULL.
    """
    # Arrange
    con, _ = run_dump([make_encode_doc(), make_hubmap_doc()])

    # Act
    rows = {
        submission: columns
        for submission, *columns in con.execute(
            "SELECT submission, s3_uri, rel_path, file_format_name, dcc_abbreviation"
            " FROM v_file"
        ).fetchall()
    }

    # Assert
    assert rows["encode"] == ["s3://encode-public/x.bed.gz", None, "BED", "ENCODE"]
    assert rows["hubmap"] == [None, "expr/some.tsv", None, "HUBMAP"]


def test_v_file_biosample_should_resolve_names_at_fanout_grain():
    """Test the v_file_biosample join grain.

    Given:
        One fully nested 4DN document.
    When:
        v_file_biosample is queried.
    Then:
        It should return the file-collection-biosample-subject grain with
        anatomy and taxonomy names resolved.
    """
    # Arrange
    con, _ = run_dump([make_fourdn_doc()])

    # Act
    rows = con.execute(
        "SELECT filename, collection_name, anatomy_name, taxonomy_name"
        " FROM v_file_biosample"
    ).fetchall()

    # Assert
    assert rows == [("file-1.mcool", "Collection exp-1", "heart", "Homo sapiens")]


@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    values=st.tuples(
        *(
            [
                st.text(
                    alphabet=st.characters(
                        min_codepoint=1, exclude_categories=("Cs",)
                    ),
                    min_size=1,
                    max_size=50,
                )
            ]
            * 4
        )
    )
)
def test_dump_files_should_roundtrip_text_with_arbitrary_values(values):
    """Test text fidelity across the document-to-DuckDB path.

    Given:
        Arbitrary non-empty surrogate-free text assigned to the
        pass-through file columns status, mime_type, analysis_type, and
        condition.
    When:
        The document is dumped.
    Then:
        It should round-trip every value byte-exact through model
        validation and parameterized insert.
    """
    # Arrange
    status, mime_type, analysis_type, condition = values
    doc = make_fourdn_doc(
        status=status,
        mime_type=mime_type,
        analysis_type=analysis_type,
        condition=condition,
    )

    # Act
    con, _ = run_dump([doc])

    # Assert
    row = con.execute(
        'SELECT status, mime_type, analysis_type, "condition" FROM file'
    ).fetchone()
    assert row == values
