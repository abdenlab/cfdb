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

**Identity.** By default TLS verifies a server's certificate against the
address the client dialed, which no static cert can satisfy here: on ECS
``EcsDiscovery`` dials each worker at its dynamic awsvpc IP, and locally
a containerized worker answers on whatever bridge address it was given.
Setting an *identity* points the client at a fixed logical name instead
(wool passes it as gRPC's ``ssl_target_name_override``), so the worker
leaf carries one stable SAN rather than an enumeration of every address
it might be reached at. Chain and SAN verification still both happen —
only the name being matched changes.

Identity is a property of the *dialing* side. That is not the same as
saying it belongs to the API: the roles are properties of a connection,
not of a process. The API is the client on the dispatch channel, and the
worker is the server there — but ``wool.LocalWorker.stop`` opens one
outbound channel of its own, to the worker's subprocess, to ask it to
drain. On that connection the worker is the client, and it verifies its
subprocess's certificate exactly the way the API verifies the worker's.

So every process that holds credentials gets the identity, and it is
simply inert wherever the credentials are used to serve rather than to
dial. Withholding it from workers looks harmless — the shipped cert
carries loopback SANs, so the stop RPC verifies by address and succeeds
— but it breaks precisely the certificate this feature exists to enable:
mint a leaf whose only SAN is the logical identity, as the runbook says
to, and graceful drain fails its name check while dispatch keeps working.
The failure is a killed worker instead of a drained one, which surfaces
as lost in-flight work rather than as a TLS error.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
from typing import Union

import wool

# Re-exported so callers can keep importing the whole TLS vocabulary
# from this module; they live in the wool-free constants module because
# ``cfdb.api`` needs the default without importing wool.
from cfdb.workflows.constants import DEFAULT_TLS_IDENTITY, TLS_IDENTITY_ENV

__all__ = [
    "DEFAULT_TLS_IDENTITY",
    "TLS_CA_ENV",
    "TLS_CERT_ENV",
    "TLS_IDENTITY_ENV",
    "TLS_KEY_ENV",
    "WorkerCredentialsLike",
    "build_worker_credentials",
    "identity_from_env",
]

#: Env var naming the shared CA certificate every peer verifies against.
TLS_CA_ENV = "CFDB_WORKER_TLS_CA"

#: Env var naming this process's PEM certificate (worker cert on a
#: worker, API client cert on the API).
TLS_CERT_ENV = "CFDB_WORKER_TLS_CERT"

#: Env var naming this process's PEM private key.
TLS_KEY_ENV = "CFDB_WORKER_TLS_KEY"

#: What :func:`build_worker_credentials` hands back: a provider when an
#: identity applies, plain credentials otherwise, ``None`` for plaintext.
WorkerCredentialsLike = Union[
    wool.WorkerCredentials, wool.WorkerCredentialsProvider
]


def identity_from_env() -> Optional[str]:
    """Resolve the configured identity, or ``None`` to verify by address.

    Reads :data:`TLS_IDENTITY_ENV`, defaulting to
    :data:`DEFAULT_TLS_IDENTITY`. An exported-but-empty value is the
    documented opt-out and normalizes to ``None``, which
    :func:`build_worker_credentials` treats as "verify against the
    dialed address" — the pre-identity behaviour.
    """
    return os.getenv(TLS_IDENTITY_ENV, DEFAULT_TLS_IDENTITY) or None


def build_worker_credentials(
    ca_path: Optional[str],
    cert_path: Optional[str],
    key_path: Optional[str],
    *,
    identity: Optional[str] = None,
    mutual: bool = True,
) -> Optional[WorkerCredentialsLike]:
    """Build wool credentials from cert paths, or ``None``.

    Returns ``None`` when all three paths are unset (empty string is
    treated as unset) — the caller stays on the plaintext gRPC path so
    local PoC dev without certs keeps working. That holds regardless of
    ``identity``: an identity alone never turns mTLS on, because there is
    no certificate for it to constrain.

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

    When ``identity`` is also given, those credentials are adapted with
    :meth:`wool.WorkerCredentials.as_provider` so the peer certificate is
    verified against that logical name rather than the dialed address —
    see the module docstring. ``identity`` is deliberately outside the
    all-or-none check above: it refines a complete mTLS configuration
    rather than forming a fourth required part of one, and omitting it
    leaves verification exactly as it was.
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

    credentials = wool.WorkerCredentials.from_files(
        ca_path=paths[TLS_CA_ENV],
        key_path=paths[TLS_KEY_ENV],
        cert_path=paths[TLS_CERT_ENV],
        mutual=mutual,
    )
    if not identity:
        return credentials
    return credentials.as_provider(identity=identity)


# NOTE: there is deliberately no env-reading convenience wrapper here.
# The entrypoints resolve their cert paths through click options (where
# a CLI flag can override the env var) and call
# ``build_worker_credentials(..., identity=identity_from_env())``
# directly; a wrapper reading ``os.getenv`` would silently drop the
# CLI override and duplicate a three-line composition. Tests pin the
# ``identity=`` kwarg at each call site instead.
