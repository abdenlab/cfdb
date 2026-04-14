"""Tests for the /index/{dcc}/{local_id} streaming router."""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from cfdb.api.routers.index import stream_index_file
from cfdb.services import drs, locks


def _make_request(method: str = "HEAD"):
    """Return a minimal mock request object."""

    class FakeRequest:
        def __init__(self):
            self.method = method

    return FakeRequest()


def _make_4dn_file_doc(
    *,
    local_id: str = "4DNFI1234ABC",
    extra_files: list | None = None,
    extra_fourdn_extras_present: bool = True,
    extra_top_level_extras: list | None = None,
) -> dict:
    """Build a materialized 4DN file document.

    By default produces a document with a single sidecar entry under the
    DCC-namespaced path ``extra.fourdn.extra_files``.
    """
    doc: dict = {
        "submission": "4dn",
        "local_id": local_id,
        "filename": "4DNFI1234ABC.mcool",
    }
    extra: dict = {}
    if extra_fourdn_extras_present:
        extra["fourdn"] = {
            "extra_files": extra_files
            if extra_files is not None
            else [
                {
                    "href": "/files-processed/4DNFI1234ABC/@@download/4DNFI1234ABC.mcool.px2",
                    "file_size": 1024,
                    "file_format": "pairs_px2",
                    "md5sum": "deadbeef",
                }
            ]
        }
    if extra_top_level_extras is not None:
        extra["extra_files"] = extra_top_level_extras
    if extra:
        doc["extra"] = extra
    return doc


class TestStreamIndexFile:
    @pytest.fixture(autouse=True)
    def _patch_cutover(self, mocker):
        """No-op ``locks.wait_for_cutover`` for every test in this class."""
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

    @pytest.mark.asyncio
    async def test_stream_index_file_4dn_with_sidecar_head(self, mock_db):
        """Test HEAD request returns sidecar headers for a 4DN file.

        Given:
            A 4DN file whose materialized document has extra.fourdn.extra_files populated.
        When:
            stream_index_file is called with a HEAD request and no Range header.
        Then:
            It should return a 200 response with Content-Disposition, Accept-Ranges,
            and Content-Length headers sourced from the sidecar entry.
        """
        # Arrange
        mock_db.files.docs = [_make_4dn_file_doc()]

        # Act
        response = await stream_index_file(
            "4dn", "4DNFI1234ABC", _make_request("HEAD"), range=None
        )

        # Assert
        assert response.status_code == 200
        assert (
            response.headers["Content-Disposition"]
            == 'attachment; filename="4DNFI1234ABC.mcool.px2"'
        )
        assert response.headers["Accept-Ranges"] == "bytes"
        assert response.headers["Content-Length"] == "1024"

    @pytest.mark.asyncio
    async def test_stream_index_file_4dn_with_only_legacy_top_level_extras(
        self, mock_db
    ):
        """Test legacy top-level extras are ignored for 4DN dispatch.

        Given:
            A 4DN file with only the legacy top-level extra.extra_files shape and
            no extra.fourdn.extra_files.
        When:
            stream_index_file is called.
        Then:
            It should raise an HTTPException with status 404 because the router
            only reads the DCC-namespaced path.
        """
        # Arrange
        mock_db.files.docs = [
            _make_4dn_file_doc(
                extra_fourdn_extras_present=False,
                extra_top_level_extras=[
                    {
                        "href": "/should/not/be/used.px2",
                        "file_size": 99,
                        "file_format": "pairs_px2",
                    }
                ],
            )
        ]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file(
                "4dn", "4DNFI1234ABC", _make_request("HEAD"), range=None
            )
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "No index file available for this file"

    @pytest.mark.asyncio
    async def test_stream_index_file_4dn_with_range_header(self, mock_db):
        """Test valid Range header produces a 206 response with Content-Range.

        Given:
            A 4DN file with a sidecar entry in extra.fourdn.extra_files.
        When:
            stream_index_file is called with a HEAD request and a valid Range header.
        Then:
            It should return a 206 response with a Content-Range header matching
            the requested byte range.
        """
        # Arrange
        mock_db.files.docs = [_make_4dn_file_doc()]

        # Act
        response = await stream_index_file(
            "4dn", "4DNFI1234ABC", _make_request("HEAD"), range="bytes=0-99"
        )

        # Assert
        assert response.status_code == 206
        assert response.headers["Content-Range"] == "bytes 0-99/1024"
        assert response.headers["Content-Length"] == "100"

    @pytest.mark.asyncio
    async def test_stream_index_file_with_missing_file_document(self, mock_db):
        """Test missing file document raises 404.

        Given:
            No file document matching the requested 4DN local_id.
        When:
            stream_index_file is called.
        Then:
            It should raise an HTTPException with status 404 and detail
            "File not found".
        """
        # Arrange
        mock_db.files.docs = []

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file(
                "4dn", "4DNFI1234ABC", _make_request("HEAD"), range=None
            )
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "File not found"

    @pytest.mark.asyncio
    async def test_stream_index_file_with_unknown_dcc(self, mock_db):
        """Test an unknown DCC name raises 400.

        Given:
            A request with an unknown DCC name.
        When:
            stream_index_file is called.
        Then:
            It should raise an HTTPException with status 400 and the DB is
            not queried.
        """
        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file(
                "not-a-dcc", "whatever", _make_request("HEAD"), range=None
            )
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_stream_index_file_4dn_entry_without_href(self, mock_db):
        """Test sidecar entry without an href raises 404.

        Given:
            A 4DN file whose extra.fourdn.extra_files entry has no href field.
        When:
            stream_index_file is called.
        Then:
            It should raise an HTTPException with status 404 and detail
            "Index file has no download URL".
        """
        # Arrange
        mock_db.files.docs = [
            _make_4dn_file_doc(extra_files=[{"file_size": 123}])
        ]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file(
                "4dn", "4DNFI1234ABC", _make_request("HEAD"), range=None
            )
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "Index file has no download URL"

    @pytest.mark.asyncio
    async def test_stream_index_file_with_mixed_case_dcc(self, mock_db):
        """Test DCC dispatch is case-insensitive.

        Given:
            A 4DN file with a sidecar entry in extra.fourdn.extra_files.
        When:
            stream_index_file is called with DCC "4DN" (mixed case) via HEAD.
        Then:
            It should return a 200 response with the correct sidecar headers,
            confirming case-insensitive DCC normalization.
        """
        # Arrange
        mock_db.files.docs = [_make_4dn_file_doc()]

        # Act
        response = await stream_index_file(
            "4DN", "4DNFI1234ABC", _make_request("HEAD"), range=None
        )

        # Assert
        assert response.status_code == 200
        assert (
            response.headers["Content-Disposition"]
            == 'attachment; filename="4DNFI1234ABC.mcool.px2"'
        )
        assert response.headers["Accept-Ranges"] == "bytes"
        assert response.headers["Content-Length"] == "1024"

    @pytest.mark.asyncio
    async def test_stream_index_file_4dn_with_get_method(self, mock_db, mocker):
        """Test GET request returns a StreamingResponse with correct headers.

        Given:
            A 4DN file with a sidecar entry in extra.fourdn.extra_files.
        When:
            stream_index_file is called with a GET request (not HEAD).
        Then:
            It should return a StreamingResponse with status 200 and
            the expected sidecar headers.
        """
        # Arrange
        mocker.patch.object(drs, "stream_from_url", return_value=iter([b""]))
        mock_db.files.docs = [_make_4dn_file_doc()]

        # Act
        response = await stream_index_file(
            "4dn", "4DNFI1234ABC", _make_request("GET"), range=None
        )

        # Assert
        assert isinstance(response, StreamingResponse)
        assert response.status_code == 200
        assert (
            response.headers["Content-Disposition"]
            == 'attachment; filename="4DNFI1234ABC.mcool.px2"'
        )
        assert response.headers["Accept-Ranges"] == "bytes"
        assert response.headers["Content-Length"] == "1024"

    @pytest.mark.asyncio
    async def test_stream_index_file_4dn_range_when_entry_has_no_file_size(
        self, mock_db
    ):
        """Test Range request on sidecar without file_size raises 500.

        Given:
            A 4DN sidecar entry that is missing the file_size field.
        When:
            stream_index_file is called with a HEAD request and a Range header.
        Then:
            It should raise an HTTPException with status 500 and detail
            "Cannot process range request: index file size unavailable".
        """
        # Arrange
        mock_db.files.docs = [
            _make_4dn_file_doc(
                extra_files=[
                    {
                        "href": "/files-processed/4DNFI1234ABC/@@download/4DNFI1234ABC.mcool.px2",
                        "file_format": "pairs_px2",
                    }
                ]
            )
        ]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file(
                "4dn", "4DNFI1234ABC", _make_request("HEAD"), range="bytes=0-99"
            )
        assert exc_info.value.status_code == 500
        assert (
            exc_info.value.detail
            == "Cannot process range request: index file size unavailable"
        )

    @pytest.mark.asyncio
    async def test_stream_index_file_4dn_with_malformed_range_header(
        self, mock_db
    ):
        """Test malformed Range header raises 400.

        Given:
            A 4DN file with a sidecar entry in extra.fourdn.extra_files.
        When:
            stream_index_file is called with a HEAD request and a malformed
            Range header "bananas".
        Then:
            It should raise an HTTPException with status 400 and a detail
            starting with "Invalid Range header".
        """
        # Arrange
        mock_db.files.docs = [_make_4dn_file_doc()]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file(
                "4dn", "4DNFI1234ABC", _make_request("HEAD"), range="bananas"
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail.startswith("Invalid Range header")

    @pytest.mark.asyncio
    async def test_stream_index_file_4dn_with_unsatisfiable_range(self, mock_db):
        """Test unsatisfiable Range raises 416 with Content-Range header.

        Given:
            A 4DN sidecar entry with file_size=1024.
        When:
            stream_index_file is called with a HEAD request and Range
            "bytes=2000-3000".
        Then:
            It should raise an HTTPException with status 416 and a
            Content-Range header of "bytes */1024".
        """
        # Arrange
        mock_db.files.docs = [_make_4dn_file_doc()]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file(
                "4dn",
                "4DNFI1234ABC",
                _make_request("HEAD"),
                range="bytes=2000-3000",
            )
        assert exc_info.value.status_code == 416
        assert exc_info.value.headers["Content-Range"] == "bytes */1024"

    @pytest.mark.asyncio
    async def test_stream_index_file_for_non_4dn_dcc_returns_no_index(
        self, mock_db
    ):
        """Test non-4DN DCCs always return no index (router only unpacks 4DN).

        Given:
            A hubmap file document in the DB (with any extra shape).
        When:
            stream_index_file is called for the hubmap DCC.
        Then:
            It should raise an HTTPException with status 404 and detail
            "No index file available for this file", because the router's
            dispatch only unpacks the 4DN-namespaced path.
        """
        # Arrange
        mock_db.files.docs = [
            {
                "submission": "hubmap",
                "local_id": "HBM123.ABCD.456",
                "filename": "dataset.zip",
                "extra": {
                    "fourdn": {
                        "extra_files": [
                            {"href": "/x.px2", "file_size": 10}
                        ]
                    },
                    "extra_files": [
                        {"href": "/y.px2", "file_size": 20}
                    ],
                },
            }
        ]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file(
                "hubmap", "HBM123.ABCD.456", _make_request("HEAD"), range=None
            )
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "No index file available for this file"

    @pytest.mark.asyncio
    async def test_stream_index_file_4dn_with_empty_fourdn_extras_list(
        self, mock_db
    ):
        """Test empty fourdn extras list raises 404.

        Given:
            A 4DN file with extra.fourdn.extra_files set to an empty list.
        When:
            stream_index_file is called with a HEAD request.
        Then:
            It should raise an HTTPException with status 404 and detail
            "No index file available for this file".
        """
        # Arrange
        mock_db.files.docs = [_make_4dn_file_doc(extra_files=[])]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file(
                "4dn", "4DNFI1234ABC", _make_request("HEAD"), range=None
            )
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "No index file available for this file"
