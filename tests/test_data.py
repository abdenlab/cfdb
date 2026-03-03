"""Tests for simplified REST access control in data router."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from cfdb.api.routers.data import stream_file


def _make_request(method: str = "GET"):
    """Return a minimal mock request object."""

    class FakeRequest:
        def __init__(self):
            self.method = method

    return FakeRequest()


def _make_dcc_doc() -> dict:
    return {
        "dcc_abbreviation": "HuBMAP",
        "project_id_namespace": "tag:hubmapconsortium.org,2023:",
    }


def _make_file_doc(*, access_level: str = "public") -> dict:
    return {
        "id_namespace": "tag:hubmapconsortium.org,2023:",
        "local_id": "file-1",
        "filename": "data.bam",
        "access_url": "drs://drs.hubmapconsortium.org/abc",
        "data_access_level": access_level,
        "submission": "hubmap",
    }


class TestStreamFile:
    @pytest.mark.asyncio
    async def test_non_public_file_returns_403(self, mock_db, mocker):
        """
        GIVEN a HuBMAP file with data_access_level="consortium" in the database
        WHEN stream_file is called for that file
        THEN a 403 HTTPException is raised without any Search API calls
        """
        mocker.patch("cfdb.services.locks.wait_for_cutover", return_value=None)

        mock_db.dcc.docs = [_make_dcc_doc()]
        mock_db.file.docs = [_make_file_doc(access_level="consortium")]

        with pytest.raises(HTTPException) as exc_info:
            await stream_file("hubmap", "file-1", _make_request(), range=None)

        assert exc_info.value.status_code == 403
        assert "consortium" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_public_file_proceeds_past_access_check(self, mock_db, mocker):
        """
        GIVEN a public HuBMAP file in the database
        WHEN stream_file is called
        THEN the access check passes (no 403) and execution continues to DRS
        """
        mocker.patch("cfdb.services.locks.wait_for_cutover", return_value=None)

        mock_db.dcc.docs = [_make_dcc_doc()]
        mock_db.file.docs = [_make_file_doc(access_level="public")]

        # Let it fail at DRS resolution (proves it passed the access check)
        mocker.patch(
            "cfdb.services.drs.fetch_drs_object",
            side_effect=Exception("DRS unavailable"),
        )

        with pytest.raises(HTTPException) as exc_info:
            await stream_file("hubmap", "file-1", _make_request(), range=None)

        # Should fail at DRS, not at access control
        assert exc_info.value.status_code != 403
