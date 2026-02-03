"""REST API router for streaming files from DCCs."""

import logging
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import Response, StreamingResponse

from cfdb import api
from cfdb.models import FileMetadataModel
from cfdb.services import drs, locks
from cfdb.services.hubmap import (
    extract_uuid_from_persistent_id,
    fetch_access_metadata,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])


@router.head("/{dcc}/{local_id}")
@router.get("/{dcc}/{local_id}")
async def stream_file(
    dcc: str, local_id: str, request: Request, range: Optional[str] = Header(None)
):
    """
    Stream file from DCC via HTTPS using file metadata from database.

    For 4DN files: Streams file contents directly via HTTPS.
    For HuBMAP files: Streams public file contents via HTTPS (Globus/protected files not supported).
    For ENCODE files: Streams directly from ENCODE servers via HTTPS.

    Path Parameters:
        dcc: DCC abbreviation (4dn, hubmap, encode) - case insensitive
        local_id: The file's unique ID within the DCC

    Headers:
        Range: Optional "bytes=start-end" for partial content requests
            - Supported for HTTPS-accessible files (4DN, public HuBMAP, ENCODE)
            - Not supported for Globus transfers

    Returns:
        StreamingResponse with file contents streamed as binary data

    Raises:
        404: File not found in database
        403: File requires authentication (consortium/protected)
        501: No supported access method available (e.g., Globus-only files)
        502: Upstream service error
        504: Service timeout
    """
    # Wait for any database cutover to complete before proceeding
    await locks.wait_for_cutover()

    try:
        # 1. Validate and normalize DCC name
        from cfdb.dcc_registry import get_all_dcc_names, normalize_dcc_name

        normalized_dcc = normalize_dcc_name(dcc)
        valid_dccs = get_all_dcc_names()

        if normalized_dcc not in valid_dccs:
            logger.warning(f"Invalid DCC requested: {dcc}")
            raise HTTPException(
                status_code=400,
                detail=f"Unknown DCC '{dcc}'. Valid DCCs: {', '.join(valid_dccs)}",
            )

        # 2. Look up DCC metadata to get id_namespace
        if api.db is None:
            logger.error("Database not initialized")
            raise HTTPException(status_code=500, detail="Database not available")

        # Look up DCC by normalized abbreviation (case-insensitive)
        dcc_doc = await api.db.dcc.find_one(
            {"dcc_abbreviation": {"$regex": f"^{normalized_dcc}", "$options": "i"}}
        )

        if not dcc_doc:
            logger.warning(f"DCC metadata not found: {dcc}")
            raise HTTPException(
                status_code=500, detail=f"DCC configuration not found: {dcc}"
            )

        id_namespace = dcc_doc.get("project_id_namespace")

        if not id_namespace:
            logger.error(f"DCC missing project_id_namespace: {dcc}")
            raise HTTPException(
                status_code=500, detail=f"DCC configuration incomplete: {dcc}"
            )

        # 3. Look up file in MongoDB by composite key
        logger.info(
            f"Looking up file: id_namespace={id_namespace}, local_id={local_id}"
        )

        # ENCODE files are stored in the pre-materialized 'files' collection
        # Other DCCs use the 'file' collection (raw C2M2 tables)
        if normalized_dcc == "encode":
            file_doc = await api.db.files.find_one(
                {"id_namespace": id_namespace, "local_id": local_id}
            )
        else:
            file_doc = await api.db.file.find_one(
                {"id_namespace": id_namespace, "local_id": local_id}
            )

        if not file_doc:
            logger.warning(f"File not found: {id_namespace}/{local_id}")
            raise HTTPException(status_code=404, detail="File not found")

        # 2. Parse file metadata
        try:
            # Extract only the fields needed for the API from the database document
            file_data = {
                k: v
                for k, v in file_doc.items()
                if k in FileMetadataModel.__fields__
                and k
                not in ("dcc", "collections")  # Skip required fields not in database
            }
            file_metadata = FileMetadataModel(**file_data)
        except Exception as e:
            logger.error(f"Failed to parse file metadata: {str(e)}")
            # Try to extract just the access_url if full parsing fails
            access_url = file_doc.get("access_url")
            if not access_url:
                raise HTTPException(
                    status_code=500, detail="Invalid file metadata in database"
                )
            # Create a minimal metadata object with just access_url
            from dataclasses import dataclass

            @dataclass
            class MinimalMetadata:
                access_url: str
                filename: str

            file_metadata = MinimalMetadata(
                access_url=access_url, filename=file_doc.get("filename", "file")
            )

        # 3. Check if file has access_url
        if not file_metadata.access_url:
            logger.warning(f"File has no access_url: {id_namespace}/{local_id}")
            raise HTTPException(status_code=501, detail="File has no access URL")

        logger.info(f"File access_url: {file_metadata.access_url}")

        # ENCODE files: Stream directly via HTTPS (bypass DRS)
        if normalized_dcc == "encode":
            return await _stream_encode_file(
                file_doc, file_metadata, request, range
            )

        # 5. Fetch DRS object metadata
        try:
            drs_object = await drs.fetch_drs_object(file_metadata.access_url)
        except ValueError as e:
            logger.warning(f"Invalid DRS URI: {file_metadata.access_url}")
            raise HTTPException(
                status_code=400, detail=f"Invalid file access URL: {str(e)}"
            )
        except Exception as e:
            logger.error(f"DRS metadata fetch failed: {str(e)}")
            if "not found" in str(e).lower():
                raise HTTPException(
                    status_code=404, detail="File not found in repository"
                )
            elif "authentication" in str(e).lower() or "forbidden" in str(e).lower():
                raise HTTPException(status_code=401, detail="Authentication required")
            elif "timeout" in str(e).lower():
                raise HTTPException(
                    status_code=504, detail="Repository service timeout"
                )
            else:
                raise HTTPException(
                    status_code=502, detail="Failed to fetch file metadata"
                )

        # 6. Check HuBMAP access level and enforce access control
        if normalized_dcc == "hubmap":
            data_access_level = file_doc.get("data_access_level")

            # If access level not cached, fetch from Search API and cache it
            if data_access_level is None:
                logger.info(
                    f"Access level not cached for {local_id}, querying HuBMAP Search API"
                )

                # Extract UUID from persistent_id
                persistent_id = file_doc.get("persistent_id")
                if persistent_id:
                    uuid = extract_uuid_from_persistent_id(persistent_id)

                    if uuid:
                        # Fetch access metadata from Search API
                        metadata = await fetch_access_metadata(uuid)

                        if metadata and metadata.data_access_level:
                            # Cache in MongoDB for future requests
                            logger.debug(
                                f"Caching access level '{metadata.data_access_level}' for {local_id}"
                            )

                            await api.db.file.update_one(
                                {"id_namespace": id_namespace, "local_id": local_id},
                                {
                                    "$set": {
                                        "status": metadata.status,
                                        "data_access_level": metadata.data_access_level,
                                    }
                                },
                            )

                            data_access_level = metadata.data_access_level
                        else:
                            logger.warning(
                                f"Could not fetch access level for {local_id} (UUID: {uuid})"
                            )
                    else:
                        logger.warning(
                            f"Could not extract UUID from persistent_id: {persistent_id}"
                        )

            # If still unknown after fetch attempt, allow request to proceed
            # Let downstream Globus/DRS handle access control
            if data_access_level is None:
                logger.info(
                    f"HuBMAP file {local_id} has unknown access level. "
                    "Allowing request to proceed - downstream access control will enforce permissions."
                )
                # Don't block the request - continue to streaming logic

            # Enforce access control based on data_access_level
            elif data_access_level in ["consortium", "protected"]:
                # This API only supports public files
                logger.info(
                    f"Blocked {data_access_level} file {local_id} - API only supports public files"
                )
                raise HTTPException(
                    status_code=403,
                    detail=f"This file requires {data_access_level} access and is not available through this API. "
                    "This API only serves publicly accessible files. "
                    "For access to HuBMAP data, please use the HuBMAP Portal at https://portal.hubmapconsortium.org/",
                )

            # Public files - continue normally (no auth required)

        # 7. Determine access method (HTTPS or Globus)
        has_globus = any(m.type == "globus" for m in drs_object.access_methods)
        has_https = any(m.type in ["https", "s3"] for m in drs_object.access_methods)

        logger.debug(
            f"Access methods available: HTTPS={has_https}, Globus={has_globus}"
        )

        # 6. Stream file using appropriate method
        if has_https:
            # Direct HTTPS streaming (4DN path)
            try:
                download_url = await drs.get_https_download_url(
                    drs_object.access_methods
                )
                logger.info(f"Streaming HTTPS file: {drs_object.name}")

                # Prepare response headers
                response_headers = {
                    "Content-Disposition": f'attachment; filename="{drs_object.name or "file"}"',
                    "Accept-Ranges": "bytes",
                }

                status_code = 200
                range_header_to_send = None

                # Handle Range request if present
                if range:
                    # Validate that file size is available
                    if not drs_object.size:
                        logger.warning(
                            f"Range request for file without size metadata: {local_id}"
                        )
                        raise HTTPException(
                            status_code=500,
                            detail="Cannot process range request: file size unavailable",
                        )

                    try:
                        # Parse and validate the Range header
                        start, end, content_length = drs.parse_range_header(
                            range, drs_object.size
                        )

                        logger.debug(
                            f"Range request: bytes {start}-{end}/{drs_object.size}"
                        )

                        # Set response for partial content
                        range_header_to_send = range
                        status_code = 206
                        response_headers["Content-Range"] = (
                            f"bytes {start}-{end}/{drs_object.size}"
                        )
                        response_headers["Content-Length"] = str(content_length)

                    except drs.RangeNotSatisfiableError as e:
                        # Range exceeds file bounds - return 416
                        logger.warning(
                            f"Range not satisfiable: {range} for file size {e.file_size}"
                        )
                        raise HTTPException(
                            status_code=416,
                            headers={
                                "Content-Range": f"bytes */{e.file_size}",
                                "Accept-Ranges": "bytes",
                            },
                        )

                    except ValueError as e:
                        # Invalid Range header syntax - return 400
                        logger.warning(f"Invalid Range header syntax: {range}")
                        raise HTTPException(
                            status_code=400, detail=f"Invalid Range header: {str(e)}"
                        )

                # Set Content-Type from DRS metadata
                media_type = drs_object.mime_type or "application/octet-stream"

                # For full file requests, include Content-Length from DRS metadata if available
                if not range and drs_object.size:
                    response_headers["Content-Length"] = str(drs_object.size)

                # HEAD request - return headers only, no body
                if request.method == "HEAD":
                    return Response(
                        status_code=status_code,
                        media_type=media_type,
                        headers=response_headers,
                    )

                # Stream file (with or without range)
                chunk_gen = drs.stream_from_url(download_url, range_header_to_send)

                return StreamingResponse(
                    chunk_gen,
                    status_code=status_code,
                    media_type=media_type,
                    headers=response_headers,
                )

            except HTTPException:
                # Re-raise HTTP exceptions (400, 416, 500, etc.)
                raise
            except Exception as e:
                logger.error(f"HTTPS streaming error: {str(e)}")
                raise HTTPException(status_code=502, detail="Failed to stream file")

        elif has_globus:
            # Globus transfer requires authentication - not supported in public-only API
            logger.warning(f"Globus-only file requested: {local_id}")
            raise HTTPException(
                status_code=501,
                detail="This file requires Globus transfer which is not supported by this API. "
                "This API only serves files accessible via HTTPS. "
                "For Globus access, please use the HuBMAP Portal at https://portal.hubmapconsortium.org/",
            )

        else:
            logger.warning(f"No supported access method for {dcc}/{local_id}")
            raise HTTPException(
                status_code=501,
                detail="No supported access method available for this file",
            )

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"Unexpected error in stream_file: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


async def _stream_encode_file(
    file_doc: dict,
    file_metadata,
    request: Request,
    range_header: Optional[str] = None,
):
    """
    Stream ENCODE file directly via HTTPS.

    ENCODE files are publicly accessible and don't require DRS resolution.
    We stream directly from the ENCODE download URL.

    Args:
        file_doc: MongoDB document for the file
        file_metadata: Parsed file metadata (FileMetadataModel or MinimalMetadata)
        request: FastAPI request object
        range_header: Optional Range header value

    Returns:
        StreamingResponse with file contents
    """
    download_url = file_metadata.access_url
    filename = getattr(file_metadata, "filename", None) or file_doc.get("filename", "file")
    file_size = file_doc.get("size_in_bytes")

    logger.info(f"Streaming ENCODE file: {filename} from {download_url}")

    # Prepare response headers
    response_headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Accept-Ranges": "bytes",
    }

    status_code = 200
    range_to_send = None

    # Handle Range request if present
    if range_header:
        if not file_size:
            logger.warning("Range request for ENCODE file without size metadata")
            raise HTTPException(
                status_code=500,
                detail="Cannot process range request: file size unavailable",
            )

        try:
            start, end, content_length = drs.parse_range_header(range_header, file_size)

            logger.debug(f"Range request: bytes {start}-{end}/{file_size}")

            range_to_send = range_header
            status_code = 206
            response_headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
            response_headers["Content-Length"] = str(content_length)

        except drs.RangeNotSatisfiableError as e:
            logger.warning(f"Range not satisfiable: {range_header} for file size {e.file_size}")
            raise HTTPException(
                status_code=416,
                headers={
                    "Content-Range": f"bytes */{e.file_size}",
                    "Accept-Ranges": "bytes",
                },
            )

        except ValueError as e:
            logger.warning(f"Invalid Range header syntax: {range_header}")
            raise HTTPException(status_code=400, detail=f"Invalid Range header: {str(e)}")

    # Determine media type from filename or default to binary
    media_type = "application/octet-stream"
    if filename:
        if filename.endswith(".gz"):
            media_type = "application/gzip"
        elif filename.endswith(".bam"):
            media_type = "application/octet-stream"
        elif filename.endswith(".fastq") or filename.endswith(".fq"):
            media_type = "text/plain"
        elif filename.endswith(".bed"):
            media_type = "text/plain"
        elif filename.endswith(".bigWig") or filename.endswith(".bw"):
            media_type = "application/octet-stream"
        elif filename.endswith(".bigBed") or filename.endswith(".bb"):
            media_type = "application/octet-stream"

    # For full file requests, include Content-Length if available
    if not range_header and file_size:
        response_headers["Content-Length"] = str(file_size)

    # HEAD request - return headers only, no body
    if request.method == "HEAD":
        return Response(
            status_code=status_code,
            media_type=media_type,
            headers=response_headers,
        )

    # Stream file content
    try:
        chunk_gen = drs.stream_from_url(download_url, range_to_send)

        return StreamingResponse(
            chunk_gen,
            status_code=status_code,
            media_type=media_type,
            headers=response_headers,
        )

    except Exception as e:
        logger.error(f"ENCODE streaming error: {str(e)}")
        raise HTTPException(status_code=502, detail="Failed to stream ENCODE file")
