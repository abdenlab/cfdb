"""REST API router for streaming index files (e.g., .px2, .bai) from DCCs."""

import logging
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import Response, StreamingResponse

from cfdb import api
from cfdb.dcc_registry import get_all_dcc_names, get_dcc_config, normalize_dcc_name
from cfdb.services import drs, locks

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/index", tags=["index"])


@router.head("/{dcc}/{local_id}")
@router.get("/{dcc}/{local_id}")
async def stream_index_file(
    dcc: str, local_id: str, request: Request, range: Optional[str] = Header(None)
):
    """
    Stream an index file (e.g., .px2, .bai) associated with a DCC file.

    Index files are discovered during 4DN API enrichment and stored in
    extra.extra_files on the materialized file document.

    Path Parameters:
        dcc: DCC abbreviation (e.g., 4dn) - case insensitive
        local_id: The file's unique ID within the DCC

    Headers:
        Range: Optional "bytes=start-end" for partial content requests

    Returns:
        StreamingResponse with index file contents

    Raises:
        400: Invalid DCC or Range header
        404: File not found or no index file available
        502: Upstream service error
    """
    await locks.wait_for_cutover()

    try:
        normalized_dcc = normalize_dcc_name(dcc)
        valid_dccs = get_all_dcc_names()

        if normalized_dcc not in valid_dccs:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown DCC '{dcc}'. Valid DCCs: {', '.join(valid_dccs)}",
            )

        if api.db is None:
            raise HTTPException(status_code=500, detail="Database not available")

        # Look up file in materialized collection
        file_doc = await api.db.files.find_one(
            {"submission": normalized_dcc, "local_id": local_id}
        )

        if not file_doc:
            raise HTTPException(status_code=404, detail="File not found")

        # Get extra_files from the extra field
        extra = file_doc.get("extra", {})
        extra_files = extra.get("extra_files", [])

        if not extra_files:
            raise HTTPException(
                status_code=404, detail="No index file available for this file"
            )

        # Take the first index file entry
        index_entry = extra_files[0]
        href = index_entry.get("href")

        if not href:
            raise HTTPException(
                status_code=404, detail="Index file has no download URL"
            )

        # Construct the full download URL
        config = get_dcc_config(normalized_dcc)
        download_url = config["api_base"] + href

        # Extract filename from href (last path segment)
        filename = href.rsplit("/", 1)[-1] if "/" in href else href
        file_size = index_entry.get("file_size")

        logger.info(f"Streaming index file: {filename} from {download_url}")

        # Prepare response headers
        response_headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Accept-Ranges": "bytes",
        }

        status_code = 200
        range_to_send = None

        # Handle Range request
        if range:
            if not file_size:
                raise HTTPException(
                    status_code=500,
                    detail="Cannot process range request: index file size unavailable",
                )

            try:
                start, end, content_length = drs.parse_range_header(range, file_size)

                range_to_send = range
                status_code = 206
                response_headers["Content-Range"] = (
                    f"bytes {start}-{end}/{file_size}"
                )
                response_headers["Content-Length"] = str(content_length)

            except drs.RangeNotSatisfiableError as e:
                raise HTTPException(
                    status_code=416,
                    headers={
                        "Content-Range": f"bytes */{e.file_size}",
                        "Accept-Ranges": "bytes",
                    },
                )

            except ValueError as e:
                raise HTTPException(
                    status_code=400, detail=f"Invalid Range header: {str(e)}"
                )

        # For full requests, include Content-Length if available
        if not range and file_size:
            response_headers["Content-Length"] = str(file_size)

        # HEAD request - return headers only
        if request.method == "HEAD":
            return Response(
                status_code=status_code,
                media_type="application/octet-stream",
                headers=response_headers,
            )

        # Stream index file
        chunk_gen = drs.stream_from_url(download_url, range_to_send)

        return StreamingResponse(
            chunk_gen,
            status_code=status_code,
            media_type="application/octet-stream",
            headers=response_headers,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in stream_index_file: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
