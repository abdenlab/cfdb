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
