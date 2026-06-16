"""REST API router for streaming files from DCCs."""

import logging
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Path, Request, status

from cfdb import api
from cfdb.api.routers._helpers import enforce_hubmap_access, lookup_file_doc
from cfdb.api.routers.cache_stream import (
    serve_workflow_artifact_or_dispatch,
    stream_upstream_url,
)
from cfdb.models import FileMetadataModel
from cfdb.services import drs, locks
from cfdb.services.drs import (
    DRSError,
    DRSForbidden,
    DRSNotFound,
    DRSRedirectBlocked,
    DRSTimeout,
    DRSUpstreamError,
)
from cfdb.workflows.models import ArtifactKind

#: Tight path-param constraint shared by /data and /index. DCC accessions
#: across ENCODE / 4DN / HuBMAP are all subsets of ``[A-Za-z0-9._-]``;
#: the length cap defends Mongo and log lines from unbounded input.
_PATH_PARAM_PATTERN = r"^[A-Za-z0-9._-]+$"
_PATH_PARAM_MAX_LEN = 256

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])


@router.head("/{dcc}/{local_id}")
@router.get("/{dcc}/{local_id}")
async def stream_file(
    dcc: str = Path(
        ..., max_length=_PATH_PARAM_MAX_LEN, pattern=_PATH_PARAM_PATTERN
    ),
    local_id: str = Path(
        ..., max_length=_PATH_PARAM_MAX_LEN, pattern=_PATH_PARAM_PATTERN
    ),
    request: Request = None,  # type: ignore[assignment]  # injected by FastAPI
    range: Optional[str] = Header(None),
    raw: bool = False,
):
    """
    Stream file from DCC via HTTPS using file metadata from database.

    When ``raw`` is False (the default), a request for a file whose
    format requires preprocessing returns the cached processed artifact
    (or ``202 Accepted`` with a ``Location`` header pointing to a
    workflow job on cache miss). Passing ``raw=true`` bypasses the
    workflow pipeline and streams the upstream file directly.

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
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown DCC '{dcc}'. Valid DCCs: {', '.join(valid_dccs)}",
            )

        # 2. Look up the file document via the shared helper so /data and
        #    /index converge on the same record for any given
        #    (normalized_dcc, local_id) pair. The helper uses
        #    FILE_DOC_PROJECTION, which strips _id and limits the doc to
        #    the fields routers + workflows actually read.
        if api.db is None:
            logger.error("Database not initialized")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database not available",
            )

        logger.info(
            f"Looking up file: submission={normalized_dcc}, local_id={local_id}"
        )
        file_doc = await lookup_file_doc(api.db, normalized_dcc, local_id)

        if not file_doc:
            logger.warning(f"File not found: {normalized_dcc}/{local_id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="File not found"
            )

        # 2. Parse file metadata
        try:
            # Extract only the fields needed for the API from the database document
            file_data = {
                k: v
                for k, v in file_doc.items()
                if k in FileMetadataModel.model_fields
                and k
                not in ("dcc", "collections")  # Skip required fields not in database
            }
            file_metadata = FileMetadataModel(**file_data)
        except Exception:
            logger.exception("Failed to parse file metadata")
            # Try to extract just the access_url if full parsing fails
            access_url = file_doc.get("access_url")
            if not access_url:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Invalid file metadata in database",
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
            logger.warning(f"File has no access_url: {normalized_dcc}/{local_id}")
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="File has no access URL",
            )

        # 4. Defence-in-depth: reject any non-public HuBMAP file that somehow
        #    survived pruning. Placed before the workflow branch so protected
        #    files never enter the preprocessing pipeline.
        #
        #    Log access_url AFTER the guard so signed/private URLs for
        #    non-public HuBMAP files don't leak to logs before the 403.
        enforce_hubmap_access(normalized_dcc, file_doc)

        logger.info(f"File access_url: {file_metadata.access_url}")

        # 5. Workflow path: if the client wants the preprocessed artifact
        #    (``raw=False``, the default) and a processor is registered
        #    for this format, serve the cached artifact or dispatch a
        #    workflow. When ``raw=True`` the workflow branch is skipped
        #    entirely and the request falls through to direct streaming
        #    from upstream. Passthrough formats (CSV/TSV/bigWig) return
        #    None either way and fall through to the existing streaming
        #    logic.
        workflow_response = await _try_serve_workflow_artifact(
            file_doc, request, range, artifact_kind=ArtifactKind.DATA, raw=raw
        )
        if workflow_response is not None:
            return workflow_response

        # ENCODE files: Stream directly via HTTPS (bypass DRS)
        if normalized_dcc == "encode":
            return await _stream_encode_file(file_doc, file_metadata, request, range)

        # 6. Fetch DRS object metadata
        try:
            drs_object = await drs.fetch_drs_object(file_metadata.access_url)
        except ValueError as e:
            logger.warning(f"Invalid DRS URI: {file_metadata.access_url}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid file access URL: {str(e)}",
            )
        except DRSNotFound:
            logger.exception("DRS metadata fetch failed: not found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File not found in repository",
            )
        except DRSForbidden:
            logger.exception("DRS metadata fetch failed: forbidden")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied by upstream repository",
            )
        except DRSTimeout:
            logger.exception("DRS metadata fetch failed: timeout")
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Repository service timeout",
            )
        except DRSRedirectBlocked:
            logger.exception("DRS metadata fetch failed: redirect blocked by allowlist")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Upstream redirect not allowed",
            )
        except (DRSUpstreamError, DRSError):
            logger.exception("DRS metadata fetch failed (upstream error)")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Failed to fetch file metadata",
            )

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

                return stream_upstream_url(
                    download_url,
                    drs_object.size,
                    drs_object.name or "file",
                    request,
                    range,
                    media_type=drs_object.mime_type or "application/octet-stream",
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
    except Exception:
        logger.exception("Unexpected error in stream_file")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        )


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
    filename = getattr(file_metadata, "filename", None) or file_doc.get(
        "filename", "file"
    )
    file_size = file_doc.get("size_in_bytes")

    logger.info(f"Streaming ENCODE file: {filename} from {download_url}")

    # Determine media type from filename; default to binary. (Branches
    # that resolve to the default are folded away.)
    media_type = "application/octet-stream"
    if filename:
        if filename.endswith(".gz"):
            media_type = "application/gzip"
        elif filename.endswith((".fastq", ".fq", ".bed")):
            media_type = "text/plain"

    try:
        return stream_upstream_url(
            download_url,
            file_size,
            filename,
            request,
            range_header,
            media_type=media_type,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"ENCODE streaming error: {str(e)}")
        raise HTTPException(status_code=502, detail="Failed to stream ENCODE file")


async def _try_serve_workflow_artifact(
    file_doc: dict,
    request: Request,
    range_header: Optional[str],
    artifact_kind: ArtifactKind,
    raw: bool,
):
    """Serve a processed artifact from cache, or dispatch a workflow.

    Thin wrapper over ``serve_workflow_artifact_or_dispatch`` that
    short-circuits when the client explicitly asked for ``?raw=true``.
    See the helper docstring for full behavior.
    """
    if raw:
        # Client explicitly asked for the upstream bytes — skip the
        # workflow branch and fall through to the direct-streaming path.
        return None
    return await serve_workflow_artifact_or_dispatch(
        file_doc,
        artifact_kind,
        request,
        range_header,
        head_404_detail=(
            "Processed artifact not yet available. Issue a GET to "
            "trigger preprocessing, or add ?raw=true for the upstream "
            "file."
        ),
    )
