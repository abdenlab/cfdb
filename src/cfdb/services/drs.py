"""Data Repository Service (DRS) integration for file streaming."""

import asyncio
import logging
import urllib.parse
from typing import AsyncGenerator, List, Optional

import aiohttp
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class RangeNotSatisfiableError(Exception):
    """Raised when requested byte range cannot be satisfied."""

    def __init__(self, file_size: int):
        self.file_size = file_size
        super().__init__(f"Range not satisfiable for file of size {file_size}")


class DRSError(Exception):
    """Base class for DRS resolution / streaming failures.

    Subclasses are mapped to HTTP status codes by the router catch
    block; callers should prefer ``except`` chains over inspecting the
    message string. Replaces the older "match the substring of
    ``str(exc).lower()``" pattern in ``routers/data.py`` which silently
    drifted on upstream wording changes.
    """


class DRSNotFound(DRSError):
    """Upstream returned a 404 or otherwise indicated the object does not exist."""


class DRSForbidden(DRSError):
    """Upstream rejected the request as unauthenticated/forbidden."""


class DRSTimeout(DRSError):
    """Connection or read timeout while streaming from upstream."""


class DRSUpstreamError(DRSError):
    """Any other upstream failure (HTTP 5xx, malformed response, network error)."""


class DRSRedirectBlocked(DRSError):
    """A 30x redirect pointed at a URL outside the allowlist."""


#: Bound on the number of redirect hops ``stream_from_url`` will follow
#: before failing. Each hop's ``Location`` must pass
#: ``validate_outbound_url``. Standard browsers default to 20 hops; we
#: pick a tighter bound since DRS resolvers should redirect at most
#: once (signed URL) or twice (CDN edge).
_MAX_REDIRECTS = 5


class DRSAccessMethod(BaseModel):
    """GA4GH DRS access method for retrieving object bytes."""

    type: str  # e.g., "https", "s3", "globus", "gs"
    access_url: Optional[str] = None  # Changed from HttpUrl to str for compatibility
    access_id: Optional[str] = None
    region: Optional[str] = None
    headers: Optional[dict] = None


class DRSObject(BaseModel):
    """GA4GH DRS object with metadata and access methods."""

    id: str
    name: Optional[str] = None
    size: Optional[int] = None
    checksums: Optional[List[dict]] = None
    access_methods: List[DRSAccessMethod]
    mime_type: Optional[str] = None


async def parse_drs_uri(drs_uri: str) -> tuple:
    """
    Parse DRS URI into hostname and object ID.

    Args:
        drs_uri: DRS URI (e.g., drs://drs.hubmapconsortium.org/abc123)

    Returns:
        Tuple of (hostname, object_id)

    Raises:
        ValueError: If URI is invalid or not a DRS URI
    """
    parsed = urllib.parse.urlparse(drs_uri)

    if parsed.scheme != "drs":
        raise ValueError(
            f"Invalid DRS URI: must start with drs://, got {parsed.scheme}://"
        )

    hostname = parsed.netloc
    object_id = parsed.path.lstrip("/")

    if not hostname or not object_id:
        raise ValueError(f"Invalid DRS URI format: {drs_uri}")

    return hostname, object_id


def parse_range_header(range_header: str, file_size: int) -> tuple[int, int, int]:
    """
    Parse and validate HTTP Range header against file size.

    Supports formats:
    - bytes=start-end (specific range)
    - bytes=start- (from start to end of file)
    - bytes=-suffix (last N bytes)

    Args:
        range_header: Range header value (e.g., "bytes=0-1023")
        file_size: Total file size in bytes

    Returns:
        Tuple of (start_byte, end_byte, content_length)

    Raises:
        ValueError: If range syntax is invalid
        RangeNotSatisfiableError: If range exceeds file bounds
    """
    # Validate format: must start with "bytes="
    if not range_header.startswith("bytes="):
        raise ValueError("Range header must start with 'bytes='")

    # Extract range part after "bytes="
    range_spec = range_header[6:].strip()

    # Reject multipart range requests (multiple ranges)
    if "," in range_spec:
        raise ValueError("Multipart range requests are not supported")

    # Parse start and end
    if "-" not in range_spec:
        raise ValueError("Invalid range format: missing '-'")

    parts = range_spec.split("-", 1)
    start_str, end_str = parts[0].strip(), parts[1].strip()

    # Handle suffix-length format: bytes=-500 (last 500 bytes)
    if not start_str and end_str:
        try:
            suffix_length = int(end_str)
        except ValueError:
            raise ValueError("Suffix length must be an integer")
        if suffix_length <= 0:
            raise ValueError("Suffix length must be positive")
        start = max(0, file_size - suffix_length)
        end = file_size - 1

    # Handle open-ended format: bytes=1000- (from byte 1000 to end)
    elif start_str and not end_str:
        try:
            start = int(start_str)
        except ValueError:
            raise ValueError("Start byte must be an integer")
        end = file_size - 1

    # Handle specific range: bytes=0-1023
    elif start_str and end_str:
        try:
            start = int(start_str)
            end = int(end_str)
        except ValueError:
            raise ValueError("Start and end bytes must be integers")

    else:
        raise ValueError("Invalid range format")

    # Validate bounds
    if start < 0 or end < 0:
        raise ValueError("Range values cannot be negative")

    if start > end:
        raise ValueError("Start byte must be <= end byte")

    if start >= file_size:
        raise RangeNotSatisfiableError(file_size)

    # Clamp end to file size
    end = min(end, file_size - 1)

    content_length = end - start + 1

    return start, end, content_length


async def fetch_drs_object(drs_uri: str, auth_token: Optional[str] = None) -> DRSObject:
    """
    Fetch DRS object metadata from GA4GH DRS API.

    Args:
        drs_uri: DRS URI (e.g., drs://drs.hubmapconsortium.org/abc123)
        auth_token: Optional Bearer token for authentication

    Returns:
        DRSObject with metadata and access methods

    Raises:
        ValueError: If DRS URI is invalid.
        DRSNotFound: Upstream returned 404 for the DRS object.
        DRSForbidden: Upstream returned 401 or 403.
        DRSTimeout: Network timeout while fetching DRS metadata.
        DRSUpstreamError: Any other upstream failure (5xx, network error,
            redirect attempt, malformed response).
    """
    from cfdb.workflows.urlsafe import validate_outbound_url

    hostname, object_id = await parse_drs_uri(drs_uri)

    # Construct GA4GH DRS API endpoint and gate it through the SSRF
    # allowlist before issuing the request. The hostname comes from a
    # DB-sourced ``access_url`` and is therefore untrusted by default.
    drs_api_url = f"https://{hostname}/ga4gh/drs/v1/objects/{object_id}"
    try:
        validate_outbound_url(drs_api_url)
    except ValueError as exc:
        raise DRSUpstreamError(
            f"DRS metadata host rejected by allowlist: {exc}"
        ) from exc

    logger.debug(f"Fetching DRS metadata from {drs_api_url}")

    async with aiohttp.ClientSession() as session:
        headers = {}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        try:
            # ``allow_redirects=False`` matches ``stream_from_url`` —
            # every 30x must be re-validated against the allowlist before
            # being followed. DRS metadata endpoints should not redirect
            # in normal operation, so we reject 3xx outright rather than
            # reimplementing the redirect loop for an exceptional path.
            async with session.get(
                drs_api_url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
                allow_redirects=False,
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Retrieved DRS object {object_id}")

                    # Parse access methods
                    access_methods = []
                    for method_data in data.get("access_methods", []):
                        # Normalize access_url - it can be a string or a dict with "url" key
                        method_copy = method_data.copy()
                        if isinstance(method_copy.get("access_url"), dict):
                            method_copy["access_url"] = method_copy["access_url"].get(
                                "url"
                            )
                        access_methods.append(DRSAccessMethod(**method_copy))

                    return DRSObject(
                        id=data.get("id", object_id),
                        name=data.get("name"),
                        size=data.get("size"),
                        checksums=data.get("checksums"),
                        access_methods=access_methods,
                        mime_type=data.get("mime_type"),
                    )

                if response.status == 404:
                    raise DRSNotFound(f"DRS object not found: {object_id}")

                if response.status in (401, 403):
                    raise DRSForbidden(
                        f"DRS object access denied ({response.status}): {object_id}"
                    )

                if response.status in (301, 302, 303, 307, 308):
                    raise DRSUpstreamError(
                        f"DRS metadata endpoint redirected unexpectedly "
                        f"(HTTP {response.status}); refusing to follow."
                    )

                raise DRSUpstreamError(
                    f"DRS API error: HTTP {response.status} for {object_id}"
                )

        except asyncio.TimeoutError as exc:
            raise DRSTimeout(
                f"DRS service timeout for {object_id}"
            ) from exc
        except aiohttp.ClientError as exc:
            raise DRSUpstreamError(
                f"Network error fetching DRS metadata: {exc}"
            ) from exc


async def get_https_download_url(access_methods: List[DRSAccessMethod]) -> str:
    """
    Extract HTTPS download URL from access methods.

    Args:
        access_methods: List of DRS access methods

    Returns:
        Download URL string

    Raises:
        ValueError: If no HTTPS/S3 access method found
    """
    for method in access_methods:
        if method.type in ["https", "s3"]:
            if not method.access_url:
                continue
            return str(method.access_url)

    raise ValueError("No HTTPS or S3 access method found")


async def stream_from_url(
    url: str, range_header: Optional[str] = None
) -> AsyncGenerator[bytes, None]:
    """
    Stream file bytes from HTTPS URL with optional Range request support.

    Args:
        url: HTTPS download URL
        auth_headers: Optional authentication headers
        range_header: Optional Range header value to forward to upstream

    Yields:
        Chunks of file bytes

    Raises:
        Exception: On download errors
    """
    from cfdb.workflows.urlsafe import validate_outbound_url

    # Validate the initial URL against the SSRF allowlist before the
    # first GET. Callers in the router layer (data.py) pass URLs that
    # came from MongoDB ``access_url`` or ``drs.get_https_download_url``
    # without prior validation; covering this here closes the gap
    # uniformly for every caller.
    try:
        validate_outbound_url(url)
    except ValueError as exc:
        raise DRSRedirectBlocked(
            f"Source URL rejected by allowlist: {exc}"
        ) from exc

    headers = {}
    if range_header:
        headers["Range"] = range_header

    # We disable aiohttp's automatic redirect-following because every
    # 30x must be re-checked against the SSRF allowlist before issuing
    # the follow-up GET. Otherwise an allowlisted host can 302 us to
    # ``http://169.254.169.254/`` (or any RFC1918 address) and aiohttp
    # would happily fetch it. We follow up to ``_MAX_REDIRECTS`` hops
    # manually, validating each ``Location`` first.
    current_url = url
    redirects_followed = 0
    timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=60)
    async with aiohttp.ClientSession() as session:
        try:
            while True:
                async with session.get(
                    current_url,
                    headers=headers,
                    timeout=timeout,
                    allow_redirects=False,
                ) as response:
                    if response.status in (301, 302, 303, 307, 308):
                        if redirects_followed >= _MAX_REDIRECTS:
                            raise DRSUpstreamError(
                                f"Exceeded {_MAX_REDIRECTS} redirects starting at {url}"
                            )
                        location = response.headers.get("Location")
                        if not location:
                            raise DRSUpstreamError(
                                f"{response.status} from {current_url} with no Location header"
                            )
                        next_url = urllib.parse.urljoin(current_url, location)
                        try:
                            validate_outbound_url(next_url)
                        except ValueError as exc:
                            raise DRSRedirectBlocked(
                                f"Redirect target rejected by allowlist: {exc}"
                            ) from exc
                        current_url = next_url
                        redirects_followed += 1
                        continue
                    if response.status == 404:
                        raise DRSNotFound(
                            f"Upstream returned 404 for {current_url}"
                        )
                    if response.status in (401, 403):
                        raise DRSForbidden(
                            f"Upstream returned {response.status} for {current_url}"
                        )
                    if response.status not in (200, 206):
                        raise DRSUpstreamError(
                            f"Failed to download file: HTTP {response.status}"
                        )
                    async for chunk in response.content.iter_chunked(8192):
                        if chunk:
                            yield chunk
                    return
        except asyncio.TimeoutError as exc:
            raise DRSTimeout(f"Timeout downloading file from {url}") from exc
        except aiohttp.ClientError as exc:
            raise DRSUpstreamError(f"Network error downloading file: {exc}") from exc
