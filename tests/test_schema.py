"""Tests for simplified GraphQL files query."""

from __future__ import annotations

import pytest

from cfdb.api.gql.schema import schema
from cfdb.services import locks


def _make_file_doc(local_id: str, submission: str = "hubmap") -> dict:
    """Return a minimal file document that satisfies FileMetadataModel."""
    return {
        "id_namespace": "ns",
        "local_id": local_id,
        "project_id_namespace": "ns",
        "project_local_id": "proj",
        "filename": f"{local_id}.bam",
        "submission": submission,
        "data_access_level": "public",
        "dcc": {
            "dcc_name": submission.upper(),
            "dcc_abbreviation": submission,
        },
        "collections": [],
    }


def _make_distinct_doc(local_id: str, dcc_name: str, submission: str = "hubmap") -> dict:
    """Return a file document with a configurable dcc_name for distinct-values tests."""
    return {
        "id_namespace": "ns",
        "local_id": local_id,
        "project_id_namespace": "ns",
        "project_local_id": "proj",
        "filename": f"{local_id}.bam",
        "submission": submission,
        "data_access_level": "public",
        "dcc": {
            "dcc_name": dcc_name,
            "dcc_abbreviation": submission,
        },
        "collections": [],
    }


class TestFilesQuery:
    @pytest.fixture(autouse=True)
    def _patch_cutover(self, mocker):
        """No-op ``locks.wait_for_cutover`` for every test in this class."""
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

    @pytest.mark.asyncio
    async def test_returns_files_via_simple_pagination(self, mock_db):
        """Test pagination cap is applied to the files query.

        Given:
            Three files in the database
        When:
            The GraphQL files query is executed with page=0, page_size=2
        Then:
            Exactly 2 files are returned (no access-level over-fetch logic)
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("f1"),
            _make_file_doc("f2"),
            _make_file_doc("f3"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                files(page: 0, pageSize: 2) {
                    localId
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        assert len(result.data["files"]) == 2

    @pytest.mark.asyncio
    async def test_returns_4dn_file_with_dict_shaped_extra_file_format(self, mock_db):
        """Test the files query serializes 4DN extra_files with a CV-object format.

        Given:
            A 4DN file whose extra.fourdn.extra_files[0].file_format is a 4DN
            CV object (a dict carrying the token under display_title) — the
            shape that crashed the query.
        When:
            The GraphQL files query selects extra.fourdn.extraFiles.fileFormat.
        Then:
            It returns without errors and exposes the display_title token.
        """
        # Arrange
        doc = _make_file_doc("4DNFITEST", submission="4dn")
        doc["extra"] = {
            "fourdn": {
                "extra_files": [
                    {
                        "href": "/files/x.pairs_px2",
                        "file_format": {
                            "status": "released",
                            "display_title": "pairs_px2",
                        },
                    }
                ]
            }
        }
        mock_db.files.docs = [doc]

        # Act
        result = await schema.execute(
            """
            query {
                files {
                    localId
                    extra { fourdn { extraFiles { href fileFormat } } }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        extra_file = result.data["files"][0]["extra"]["fourdn"]["extraFiles"][0]
        assert extra_file["fileFormat"] == "pairs_px2"
        assert extra_file["href"] == "/files/x.pairs_px2"

    @pytest.mark.asyncio
    async def test_returns_all_submissions_without_filtering(self, mock_db):
        """Test the files query returns all DCCs when no filter is supplied.

        Given:
            Files from multiple DCCs including HuBMAP
        When:
            The GraphQL files query is executed with no input filter
        Then:
            Files from all DCCs are returned without access-level filtering
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("h1", "hubmap"),
            _make_file_doc("f1", "4dn"),
            _make_file_doc("e1", "encode"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                files(page: 0, pageSize: 10) {
                    localId
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        assert len(result.data["files"]) == 3


class TestDistinctValuesQuery:
    @pytest.fixture(autouse=True)
    def _patch_cutover(self, mocker):
        """No-op ``locks.wait_for_cutover`` for every test in this class."""
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

    @pytest.mark.asyncio
    async def test_distinct_values_returns_all_unique_values_for_single_field(
        self, mock_db
    ):
        """Test distinct values for a single nested field without filtering.

        Given:
            Three files with different dcc.dcc_name values
        When:
            The distinctValues query is executed with fields: ["dcc.dcc_name"]
        Then:
            It should return one entry containing all three distinct DCC names
        """
        # Arrange
        mock_db.files.docs = [
            _make_distinct_doc("f1", "HuBMAP", "hubmap"),
            _make_distinct_doc("f2", "4DN", "4dn"),
            _make_distinct_doc("f3", "ENCODE", "encode"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["dcc.dcc_name"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        assert len(result.data["distinctValues"]) == 1
        entry = result.data["distinctValues"][0]
        assert entry["field"] == "dcc.dcc_name"
        assert sorted(entry["values"]) == ["4DN", "ENCODE", "HuBMAP"]

    @pytest.mark.asyncio
    async def test_distinct_values_returns_entries_for_multiple_fields(
        self, mock_db
    ):
        """Test distinct values for multiple fields in one call.

        Given:
            Three files spanning two DCCs and two abbreviations
        When:
            The distinctValues query is executed with fields: ["dcc.dcc_name", "dcc.dcc_abbreviation"]
        Then:
            It should return two entries, each with the correct distinct values
        """
        # Arrange
        mock_db.files.docs = [
            _make_distinct_doc("f1", "HuBMAP", "hubmap"),
            _make_distinct_doc("f2", "HuBMAP", "hubmap"),
            _make_distinct_doc("f3", "4DN", "4dn"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["dcc.dcc_name", "dcc.dcc_abbreviation"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        entries = result.data["distinctValues"]
        assert len(entries) == 2
        by_field = {e["field"]: e["values"] for e in entries}
        assert sorted(by_field["dcc.dcc_name"]) == ["4DN", "HuBMAP"]
        assert sorted(by_field["dcc.dcc_abbreviation"]) == ["4dn", "hubmap"]

    @pytest.mark.asyncio
    async def test_distinct_values_applies_input_filter(self, mock_db):
        """Test distinct values with a DCC filter applied.

        Given:
            Three files, two from HuBMAP and one from 4DN
        When:
            The distinctValues query is executed with a DCC filter for HuBMAP and fields: ["dcc.dcc_abbreviation"]
        Then:
            It should return only the abbreviations from HuBMAP files
        """
        # Arrange
        mock_db.files.docs = [
            _make_distinct_doc("h1", "HuBMAP", "hubmap"),
            _make_distinct_doc("h2", "HuBMAP", "hubmap"),
            _make_distinct_doc("f1", "4DN", "4dn"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(
                    fields: ["dcc.dcc_abbreviation"]
                    input: [{ dcc: [{ dccName: ["HuBMAP"] }] }]
                ) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        entry = result.data["distinctValues"][0]
        assert entry["values"] == ["hubmap"]

    @pytest.mark.asyncio
    async def test_distinct_values_returns_empty_list_for_missing_field(
        self, mock_db
    ):
        """Test distinct values for a field absent from all documents.

        Given:
            Three files, none with a compression_format value
        When:
            The distinctValues query is executed with fields: ["compression_format"]
        Then:
            It should return one entry with an empty values list
        """
        # Arrange
        mock_db.files.docs = [
            _make_distinct_doc("f1", "HuBMAP"),
            _make_distinct_doc("f2", "4DN"),
            _make_distinct_doc("f3", "ENCODE"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["compression_format"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        entry = result.data["distinctValues"][0]
        assert entry["values"] == []

    @pytest.mark.asyncio
    async def test_distinct_values_returns_empty_list_for_empty_database(
        self, mock_db
    ):
        """Test distinct values against an empty collection.

        Given:
            An empty database
        When:
            The distinctValues query is executed with fields: ["dcc.dcc_name"]
        Then:
            It should return one entry with an empty values list
        """
        # Arrange
        mock_db.files.docs = []

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["dcc.dcc_name"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        entry = result.data["distinctValues"][0]
        assert entry["values"] == []

    @pytest.mark.asyncio
    async def test_distinct_values_deduplicates_values(self, mock_db):
        """Test that duplicate values are collapsed to unique entries.

        Given:
            Three files where two share the same dcc.dcc_name
        When:
            The distinctValues query is executed with fields: ["dcc.dcc_name"]
        Then:
            It should return only the deduplicated values
        """
        # Arrange
        mock_db.files.docs = [
            _make_distinct_doc("f1", "HuBMAP", "hubmap"),
            _make_distinct_doc("f2", "HuBMAP", "hubmap"),
            _make_distinct_doc("f3", "4DN", "4dn"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["dcc.dcc_name"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        entry = result.data["distinctValues"][0]
        assert sorted(entry["values"]) == ["4DN", "HuBMAP"]

    @pytest.mark.asyncio
    async def test_distinct_values_rejects_disallowed_field(self, mock_db):
        """Test that fields outside the allowlist are rejected.

        Given:
            A request for a field not in ALLOWED_DISTINCT_FIELDS
        When:
            The distinctValues query is executed with fields: ["secret_field"]
        Then:
            It should return an error naming the disallowed field
        """
        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["secret_field"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is not None
        assert "secret_field" in result.errors[0].message

    @pytest.mark.asyncio
    async def test_distinct_values_returns_unique_subdoc_names(self, mock_db):
        """Test distinct values for an EDAM subdocument sub-path.

        Given:
            Three files whose file_format subdocuments carry two distinct names
        When:
            The distinctValues query is executed with fields: ["file_format.name"]
        Then:
            It should return one entry containing the two distinct format names
        """
        # Arrange
        def doc_with_format(local_id: str, fmt_id: str, fmt_name: str) -> dict:
            doc = _make_distinct_doc(local_id, "ENCODE", "encode")
            doc["file_format"] = {"id": fmt_id, "name": fmt_name}
            return doc

        mock_db.files.docs = [
            doc_with_format("f1", "format:3003", "BED"),
            doc_with_format("f2", "format:3003", "BED"),
            doc_with_format("f3", "format:3004", "bigBed"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["file_format.name"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        entry = result.data["distinctValues"][0]
        assert entry["field"] == "file_format.name"
        assert sorted(entry["values"]) == ["BED", "bigBed"]

    @pytest.mark.asyncio
    async def test_distinct_values_rejects_bare_subdocument_field(self, mock_db):
        """Test that the bare top-level subdocument name is no longer allowlisted.

        Given:
            The bare ``file_format`` field returned subdocuments rather than scalar
            values, so it has been removed from ALLOWED_DISTINCT_FIELDS in favor of
            the indexed ``file_format.id`` and ``file_format.name`` sub-paths.
        When:
            The distinctValues query is executed with fields: ["file_format"]
        Then:
            It should return an error naming the disallowed field
        """
        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["file_format"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is not None
        assert "file_format" in result.errors[0].message
