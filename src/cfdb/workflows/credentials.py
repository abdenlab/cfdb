"""Build wool mutual-TLS credentials from cert-path configuration.

The API dispatches ``@wool.routine`` work to wool workers over a gRPC
channel. By default that channel is plaintext, gated only by the worker
security group. When the three cert-path env vars are configured, both
sides load a :class:`wool.WorkerCredentials` (CA + leaf cert + key,
``mutual=True``) so the server and the dispatching client authenticate
each other and the channel is encrypted.

This module is the single seam every process uses to turn cert paths
into credentials: the ECS worker entrypoint (:mod:`worker_main`), the
LAN worker pool (:mod:`worker_lan`), and the API lifespan all call
:func:`build_worker_credentials`. Keeping the validation in one place
means "all three set or none, and partial config fails fast" is
enforced identically everywhere.

wool's mTLS is peer-to-peer: each process holds *its own* leaf cert and
key (the worker's on workers, the API client's on the API) signed by a
*shared* CA. So ``CFDB_WORKER_TLS_CERT`` / ``CFDB_WORKER_TLS_KEY`` name
different files per process while ``CFDB_WORKER_TLS_CA`` is common.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import wool

__all__ = [
    "TLS_CA_ENV",
    "TLS_CERT_ENV",
    "TLS_KEY_ENV",
    "build_worker_credentials",
    "worker_credentials_from_env",
]

#: Env var naming the shared CA certificate every peer verifies against.
TLS_CA_ENV = "CFDB_WORKER_TLS_CA"

#: Env var naming this process's PEM certificate (worker cert on a
#: worker, API client cert on the API).
TLS_CERT_ENV = "CFDB_WORKER_TLS_CERT"

#: Env var naming this process's PEM private key.
TLS_KEY_ENV = "CFDB_WORKER_TLS_KEY"


def build_worker_credentials(
    ca_path: Optional[str],
    cert_path: Optional[str],
    key_path: Optional[str],
    *,
    mutual: bool = True,
) -> Optional[wool.WorkerCredentials]:
    """Build :class:`wool.WorkerCredentials` from cert paths, or ``None``.

    Returns ``None`` when all three paths are unset (empty string is
    treated as unset) — the caller stays on the plaintext gRPC path so
    local PoC dev without certs keeps working.

    Raises :class:`ValueError` when the configuration is *partial* (some
    paths set, some not): a half-configured channel is never intentional
    and would otherwise silently fall back to plaintext. The error names
    the missing env var(s).

    Raises :class:`ValueError` when a configured path does not exist on
    disk, naming the offending path, so the failure surfaces here rather
    than as an opaque error deep inside wool/grpc.

    When all three exist, returns the credentials from
    :meth:`wool.WorkerCredentials.from_files` with ``mutual`` (default
    ``True`` — both server and client authenticate, i.e. mTLS enforced).
    """
    # Normalize empty strings to None so an exported-but-empty env var
    # reads as "unset" rather than a path that fails the existence check.
    paths = {
        TLS_CA_ENV: ca_path or None,
        TLS_CERT_ENV: cert_path or None,
        TLS_KEY_ENV: key_path or None,
    }

    set_vars = {name for name, value in paths.items() if value is not None}
    if not set_vars:
        return None

    missing_vars = sorted(name for name in paths if name not in set_vars)
    if missing_vars:
        raise ValueError(
            "Partial worker mTLS configuration: "
            f"{', '.join(sorted(set_vars))} set but "
            f"{', '.join(missing_vars)} unset. Set all three "
            f"({TLS_CA_ENV}, {TLS_CERT_ENV}, {TLS_KEY_ENV}) to enable "
            "mTLS, or none to stay on the plaintext channel."
        )

    for name, value in paths.items():
        assert value is not None  # narrowed by the partial-config check
        if not Path(value).is_file():
            raise ValueError(
                f"Worker mTLS path {name}={value!r} does not exist or is "
                "not a file."
            )

    return wool.WorkerCredentials.from_files(
        ca_path=paths[TLS_CA_ENV],
        key_path=paths[TLS_KEY_ENV],
        cert_path=paths[TLS_CERT_ENV],
        mutual=mutual,
    )


def worker_credentials_from_env(
    *, mutual: bool = True
) -> Optional[wool.WorkerCredentials]:
    """Build credentials from the ``CFDB_WORKER_TLS_*`` env vars.

    Convenience wrapper over :func:`build_worker_credentials` that reads
    :data:`TLS_CA_ENV`, :data:`TLS_CERT_ENV`, and :data:`TLS_KEY_ENV`
    from the process environment. Used by the worker entrypoints; the
    API passes its own ``cfdb.api`` config constants in directly.
    """
    return build_worker_credentials(
        os.getenv(TLS_CA_ENV),
        os.getenv(TLS_CERT_ENV),
        os.getenv(TLS_KEY_ENV),
        mutual=mutual,
    )
