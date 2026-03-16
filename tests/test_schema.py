"""Tests for simplified GraphQL files query."""

from __future__ import annotations

import pytest

from cfdb.api.gql.schema import schema


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
    @pytest.mark.asyncio
    async def test_returns_files_via_simple_pagination(self, mock_db, mocker):
        """
        GIVEN 3 files in the database
        WHEN the GraphQL files query is executed with page=0, page_size=2
        THEN exactly 2 files are returned (no access-level over-fetch logic)
        """
        mocker.patch("cfdb.services.locks.wait_for_cutover", return_value=None)

        mock_db.files.docs = [
            _make_file_doc("f1"),
            _make_file_doc("f2"),
            _make_file_doc("f3"),
        ]

        result = await schema.execute(
            """
            query {
                files(page: 0, pageSize: 2) {
                    localId
                }
            }
            """
        )

        assert result.errors is None
        assert len(result.data["files"]) == 2

    @pytest.mark.asyncio
    async def test_returns_all_submissions_without_filtering(self, mock_db, mocker):
        """
        GIVEN files from multiple DCCs including HuBMAP
        WHEN the GraphQL files query is executed with no input filter
        THEN files from all DCCs are returned without access-level filtering
        """
        mocker.patch("cfdb.services.locks.wait_for_cutover", return_value=None)

        mock_db.files.docs = [
            _make_file_doc("h1", "hubmap"),
            _make_file_doc("f1", "4dn"),
            _make_file_doc("e1", "encode"),
        ]

        result = await schema.execute(
            """
            query {
                files(page: 0, pageSize: 10) {
                    localId
                }
            }
            """
        )

        assert result.errors is None
        assert len(result.data["files"]) == 3


class TestDistinctValuesQuery:
    @pytest.mark.asyncio
    async def test_distinct_values_returns_all_unique_values_for_single_field(
        self, mock_db, mocker
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
        mocker.patch("cfdb.services.locks.wait_for_cutover", return_value=None)
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
        self, mock_db, mocker
    ):
        """Test distinct values for multiple fields in one call.

        Given:
            Three files spanning two DCCs and two submissions
        When:
            The distinctValues query is executed with fields: ["dcc.dcc_name", "submission"]
        Then:
            It should return two entries, each with the correct distinct values
        """
        # Arrange
        mocker.patch("cfdb.services.locks.wait_for_cutover", return_value=None)
        mock_db.files.docs = [
            _make_distinct_doc("f1", "HuBMAP", "hubmap"),
            _make_distinct_doc("f2", "HuBMAP", "hubmap"),
            _make_distinct_doc("f3", "4DN", "4dn"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["dcc.dcc_name", "submission"]) {
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
        assert sorted(by_field["submission"]) == ["4dn", "hubmap"]

    @pytest.mark.asyncio
    async def test_distinct_values_applies_input_filter(self, mock_db, mocker):
        """Test distinct values with a DCC filter applied.

        Given:
            Three files, two from HuBMAP and one from 4DN
        When:
            The distinctValues query is executed with a DCC filter for HuBMAP and fields: ["local_id"]
        Then:
            It should return only the local IDs from HuBMAP files
        """
        # Arrange
        mocker.patch("cfdb.services.locks.wait_for_cutover", return_value=None)
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
                    fields: ["local_id"]
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
        assert sorted(entry["values"]) == ["h1", "h2"]

    @pytest.mark.asyncio
    async def test_distinct_values_returns_empty_list_for_missing_field(
        self, mock_db, mocker
    ):
        """Test distinct values for a field absent from all documents.

        Given:
            Three files, none with a persistent_id value
        When:
            The distinctValues query is executed with fields: ["persistent_id"]
        Then:
            It should return one entry with an empty values list
        """
        # Arrange
        mocker.patch("cfdb.services.locks.wait_for_cutover", return_value=None)
        mock_db.files.docs = [
            _make_distinct_doc("f1", "HuBMAP"),
            _make_distinct_doc("f2", "4DN"),
            _make_distinct_doc("f3", "ENCODE"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["persistent_id"]) {
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
        self, mock_db, mocker
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
        mocker.patch("cfdb.services.locks.wait_for_cutover", return_value=None)
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
    async def test_distinct_values_deduplicates_values(self, mock_db, mocker):
        """Test that duplicate values are collapsed to unique entries.

        Given:
            Three files where two share the same dcc.dcc_name
        When:
            The distinctValues query is executed with fields: ["dcc.dcc_name"]
        Then:
            It should return only the deduplicated values
        """
        # Arrange
        mocker.patch("cfdb.services.locks.wait_for_cutover", return_value=None)
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
