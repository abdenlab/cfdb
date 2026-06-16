#!/bin/sh
# Materialize wool mTLS PEMs from env vars to files, then exec the command.
#
# ECS Fargate cannot mount a Secrets Manager secret as a file — it injects
# secret values as environment variables. But cfdb reads
# CFDB_WORKER_TLS_CA/CERT/KEY as filesystem *paths*
# (cfdb.workflows.credentials.build_worker_credentials). This shim bridges
# the two: for each X in CA/CERT/KEY, if CFDB_WORKER_TLS_<X>_PEM holds PEM
# content, write it to a private 0600 file and point CFDB_WORKER_TLS_<X> at
# that file, then exec the real command.
#
# It is INERT when the *_PEM vars are absent, so the plaintext path and the
# local mounted-cert path (CFDB_WORKER_TLS_* already set to mounted file
# paths) both pass straight through untouched. Used as the ENTRYPOINT of
# both Dockerfile.api and Dockerfile.wool.
set -eu

# Private dir for the materialized PEMs. Overridable for tests; defaults
# under /tmp, which is writable by the unprivileged ``app`` user in both
# images.
_dir="${CFDB_WORKER_TLS_DIR:-/tmp/cfdb-tls}"

# $1 = source env var name (PEM content), $2 = filename,
# $3 = path env var to export for build_worker_credentials.
_materialize() {
    _val="$(printenv "$1" 2>/dev/null || true)"
    [ -n "$_val" ] || return 0
    mkdir -p "$_dir"
    chmod 700 "$_dir"
    _path="$_dir/$2"
    printf '%s\n' "$_val" >"$_path"
    chmod 600 "$_path"
    export "$3=$_path"
}

_materialize CFDB_WORKER_TLS_CA_PEM ca.pem CFDB_WORKER_TLS_CA
_materialize CFDB_WORKER_TLS_CERT_PEM cert.pem CFDB_WORKER_TLS_CERT
_materialize CFDB_WORKER_TLS_KEY_PEM key.pem CFDB_WORKER_TLS_KEY

exec "$@"
