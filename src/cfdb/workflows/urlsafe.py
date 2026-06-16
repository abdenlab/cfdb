"""Outbound URL allowlist for worker-side fetches.

Workers pull source bytes from DCC-controlled URLs (``access_url`` for
direct HTTPS / S3 fetches, ``drs://`` URIs that resolve to HTTPS, and
4DN sidecar ``href`` values joined onto a DCC ``api_base``). A poisoned
upstream record could redirect a worker at AWS IMDS
(``http://169.254.169.254/``), an internal control plane, or any
attacker-controlled host. The allowlist below caps the blast radius to
known-good production domains.

The list is conservative — adding a new DCC's URL space is an explicit
edit here so it's surfaced for review.
"""

from __future__ import annotations

import os
from typing import Final
from urllib.parse import urlparse

#: Schemes that may appear in any outbound URL the worker fetches.
ALLOWED_SCHEMES: Final[frozenset[str]] = frozenset({"https", "drs"})

#: When set to a truthy value, ``http://127.0.0.1`` and
#: ``http://localhost`` URLs are also accepted. Used by the integration
#: test suite to point workers at an in-process ``http.server`` serving
#: sample fixture bytes (aiohttp does not accept ``file://`` URIs and
#: production HTTPS infrastructure is overkill for an in-test fixture
#: server). MUST NOT be set in production.
_ALLOW_HTTP_LOOPBACK: Final[bool] = os.getenv(
    "CFDB_URLSAFE_ALLOW_HTTP_LOOPBACK", ""
).lower() in ("1", "true", "yes")

#: Allowed netloc suffixes. A leading dot (``".s3.amazonaws.com"``) means
#: "any subdomain of"; an entry without a leading dot is an exact match.
ALLOWED_NETLOC_SUFFIXES: Final[tuple[str, ...]] = (
    # AWS S3 (presigned URLs commonly used by 4DN / ENCODE / HuBMAP).
    ".s3.amazonaws.com",
    ".s3.us-east-1.amazonaws.com",
    ".s3.us-west-1.amazonaws.com",
    ".s3.us-west-2.amazonaws.com",
    "s3.amazonaws.com",
    # 4DN data portal.
    "data.4dnucleome.org",
    # ENCODE.
    "www.encodeproject.org",
    "encode-public.s3.amazonaws.com",
    # HuBMAP public assets.
    "assets.hubmapconsortium.org",
)


class UnsafeOutboundURL(ValueError):
    """Raised when an outbound URL is outside the allowlist."""


def validate_outbound_url(url: str) -> str:
    """Raise ``UnsafeOutboundURL`` if ``url`` is not in the outbound allowlist.

    Returns the unmodified URL on success so callers can chain
    ``validate_outbound_url(url)`` inline.
    """
    parsed = urlparse(url)
    # Loopback HTTP escape hatch for the integration test suite — see
    # ``CFDB_URLSAFE_ALLOW_HTTP_LOOPBACK`` above.
    if (
        _ALLOW_HTTP_LOOPBACK
        and parsed.scheme == "http"
        and parsed.hostname in ("127.0.0.1", "localhost")
    ):
        return url
    if parsed.scheme not in ALLOWED_SCHEMES:
        raise UnsafeOutboundURL(f"scheme not allowed: {parsed.scheme!r}")
    # ``drs://`` URIs identify a DRS broker that the workflow layer
    # resolves to an HTTPS download URL via ``drs.fetch_drs_object``;
    # the resolved URL is then re-validated against this allowlist
    # before bytes flow. Enforcing the HTTPS-host allowlist on the
    # raw DRS URI here would reject every legitimate DRS hostname
    # (none of the production DCC DRS brokers are HTTPS portals), so
    # short-circuit on scheme: the SSRF boundary is the resolved
    # HTTPS URL, not the DRS broker identifier itself.
    if parsed.scheme == "drs":
        if not parsed.netloc:
            raise UnsafeOutboundURL(f"missing DRS host: {url!r}")
        return url
    host = parsed.netloc.lower()
    if not host:
        raise UnsafeOutboundURL(f"missing netloc: {url!r}")
    # Strip user:password@ if present (defense in depth — shouldn't appear
    # in our URL space).
    if "@" in host:
        host = host.rsplit("@", 1)[1]
    # Strip port suffix.
    if ":" in host:
        host = host.rsplit(":", 1)[0]
    for suffix in ALLOWED_NETLOC_SUFFIXES:
        if suffix.startswith("."):
            if host.endswith(suffix) or host == suffix.lstrip("."):
                return url
        else:
            if host == suffix:
                return url
    raise UnsafeOutboundURL(f"host not in allowlist: {host!r}")
