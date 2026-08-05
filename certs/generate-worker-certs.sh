#!/usr/bin/env bash
#
# Generate the mutual-TLS material for the wool worker gRPC channel.
#
# Mints a shared CA plus two CA-signed leaf certificates — one for the
# wool workers and one for the API client — so the API and the workers
# can authenticate each other over gRPC (wool's ``mutual=True``). Point
# each process at its own leaf cert/key and the shared CA via:
#
#   CFDB_WORKER_TLS_CA   -> certs/worker-ca/ca.pem
#   CFDB_WORKER_TLS_CERT -> certs/worker/worker-cert.pem  (on the worker)
#                           certs/api/api-cert.pem        (on the API)
#   CFDB_WORKER_TLS_KEY  -> certs/worker/worker-key.pem   (on the worker)
#                           certs/api/api-key.pem         (on the API)
#
# The CA lives under certs/worker-ca/ (distinct from any DocumentDB
# X.509 material) so the two cert stories never collide.
#
# This is local-dev tooling. The generated keys are git-ignored
# (see certs/.gitignore) and MUST NOT be committed or used in
# production; ECS distributes its own material out of band.
#
# The *worker* leaf carries a logical *identity* SAN (default
# ``cfdb-worker``). The API verifies worker certs against that name
# rather than the address it dialed — see CFDB_WORKER_TLS_IDENTITY —
# which is what lets one cert serve a worker no matter which address it
# comes up on. Without it the SAN list has to enumerate every address a
# worker might answer at, which is impossible on ECS and tedious for
# containerized local dev. The API leaf deliberately carries no SAN at
# all (and clientAuth only): a worker verifies its client by chain, not
# by name, and an identity SAN on the API leaf would make the API's own
# certificate a valid worker certificate.
#
# Usage:
#   ./certs/generate-worker-certs.sh [--force] [--identity NAME] [extra-SAN ...]
#
#   --force          Overwrite existing certificates instead of skipping.
#   --identity NAME  Logical identity SAN to embed (default cfdb-worker).
#                    Must match CFDB_WORKER_TLS_IDENTITY on the API.
#   extra-SAN ...    Additional subjectAltName entries to embed in the
#                    leaf certs (e.g. a worker's LAN hostname/IP). Each
#                    is auto-classified as IP:<x> or DNS:<x>. Rarely
#                    needed now that the identity covers addressing.
#
#   -h, --help       Show this help and exit.

set -euo pipefail

usage() {
    sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//; $d'
}

FORCE=0
IDENTITY="cfdb-worker"   # keep in step with credentials.DEFAULT_TLS_IDENTITY
EXTRA_SANS=()
while [ "$#" -gt 0 ]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        --force)
            FORCE=1
            ;;
        --identity)
            # Consumes the next argument, so guard against it being
            # absent — otherwise the identity would silently become
            # empty and every leaf would ship without its stable SAN.
            [ "$#" -ge 2 ] || { echo "error: --identity requires a name" >&2; exit 2; }
            IDENTITY="$2"
            shift
            ;;
        --identity=*)
            IDENTITY="${1#--identity=}"
            ;;
        *)
            EXTRA_SANS+=("$1")
            ;;
    esac
    shift
done

# The identity is interpolated straight into a subjectAltName list and
# matched by gRPC as a DNS name, so constrain it to what can survive
# both. A comma would silently inject a second SAN entry; a space would
# fail openssl's extension parser; a leading dash means the caller wrote
# "--identity --force" and lost their value to the flag that followed.
# An IP literal is rejected too: it would be minted as DNS:1.2.3.4,
# which gRPC never matches against an address.
if [ -z "$IDENTITY" ]; then
    echo "error: --identity must not be empty" >&2
    exit 2
elif [[ ! "$IDENTITY" =~ ^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$ ]]; then
    echo "error: --identity must be a DNS-safe name (got '$IDENTITY')" >&2
    exit 2
elif [[ "$IDENTITY" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "error: --identity must be a name, not an IP address" >&2
    exit 2
fi

# Resolve directories relative to this script so it works from any CWD.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CA_DIR="$SCRIPT_DIR/worker-ca"
WORKER_DIR="$SCRIPT_DIR/worker"
API_DIR="$SCRIPT_DIR/api"

CA_KEY="$CA_DIR/ca-key.pem"
CA_CERT="$CA_DIR/ca.pem"

DAYS=825          # < 825d keeps leaf certs within common TLS limits
KEY_BITS=4096

mkdir -p "$CA_DIR" "$WORKER_DIR" "$API_DIR"

if ! command -v openssl >/dev/null 2>&1; then
    echo "error: openssl not found on PATH" >&2
    exit 1
fi

# Build the subjectAltName list. The identity comes first because it is
# the entry the API actually matches on when CFDB_WORKER_TLS_IDENTITY is
# set; the rest are a convenience for address-based verification, which
# is what you fall back to with an empty identity. localhost / loopback
# / 0.0.0.0 cover the single-host LAN-dev case; callers can append the
# worker's actual advertised hostname/IP as extra args.
#
# These belong to the WORKER leaf alone. The API is only ever a client on
# this channel, and a worker authenticates its client by chain with no
# name check — so the API leaf needs no SAN at all. Giving it one, and in
# particular giving it the identity, would make the API's own certificate
# a valid worker server certificate: after this change the API's test of
# a worker is "CA-signed and bears the identity SAN", which its own leaf
# would then satisfy.
build_worker_san() {
    local sans="DNS:$IDENTITY,DNS:localhost,IP:127.0.0.1,IP:0.0.0.0"
    local host
    host="$(hostname 2>/dev/null || true)"
    if [ -n "$host" ]; then
        sans="$sans,DNS:$host"
    fi
    local extra
    for extra in "${EXTRA_SANS[@]:-}"; do
        [ -z "$extra" ] && continue
        if [[ "$extra" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            sans="$sans,IP:$extra"
        else
            sans="$sans,DNS:$extra"
        fi
    done
    printf '%s' "$sans"
}

WORKER_SAN="$(build_worker_san)"

# --- Certificate authority -------------------------------------------
if [ "$FORCE" -eq 0 ] && [ -f "$CA_CERT" ] && [ -f "$CA_KEY" ]; then
    echo "CA already exists at $CA_CERT (use --force to regenerate)"
else
    echo "Generating CA -> $CA_CERT"
    openssl req -x509 -newkey "rsa:$KEY_BITS" -nodes \
        -keyout "$CA_KEY" -out "$CA_CERT" \
        -days "$DAYS" -sha256 \
        -subj "/CN=cfdb-worker-ca"
fi

# --- Leaf certificate helper -----------------------------------------
# $1 = output dir, $2 = file stem, $3 = CN, $4 = extendedKeyUsage,
# $5 = subjectAltName list (empty for a leaf that needs none)
#
# The two leaves are deliberately NOT symmetric. The worker terminates
# the dispatch connection, so it needs serverAuth and a SAN the API can
# match. The API only ever dials, so it needs clientAuth and no SAN —
# and withholding serverAuth means the TLS stack itself refuses to let
# the API's certificate terminate a server side, which is a stronger
# guarantee than merely omitting the name.
mint_leaf() {
    local dir="$1" stem="$2" cn="$3" eku="$4" san="${5:-}"
    local key="$dir/${stem}-key.pem"
    local cert="$dir/${stem}-cert.pem"
    local csr="$dir/${stem}.csr"

    if [ "$FORCE" -eq 0 ] && [ -f "$cert" ] && [ -f "$key" ]; then
        echo "${stem} cert already exists at $cert (use --force to regenerate)"
        return
    fi

    # openssl rejects an empty subjectAltName, so the extension is
    # omitted entirely rather than emitted blank.
    local ext="extendedKeyUsage=$eku"
    if [ -n "$san" ]; then
        ext="subjectAltName=$san"$'\n'"$ext"
    fi

    echo "Generating ${stem} cert -> $cert (CN=$cn, EKU=$eku, SAN=${san:-<none>})"
    openssl req -newkey "rsa:$KEY_BITS" -nodes \
        -keyout "$key" -out "$csr" \
        -subj "/CN=$cn"
    openssl x509 -req -in "$csr" \
        -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial \
        -out "$cert" -days "$DAYS" -sha256 \
        -extfile <(printf '%s\n' "$ext")
    rm -f "$csr"
}

# The worker also dials one connection of its own — wool's graceful-stop
# RPC to its subprocess — so its leaf keeps clientAuth alongside
# serverAuth.
# CN follows the identity so `openssl x509 -text` reads consistently
# during a debugging session; verification itself uses only the SAN.
mint_leaf "$WORKER_DIR" "worker" "$IDENTITY" "serverAuth,clientAuth" "$WORKER_SAN"
mint_leaf "$API_DIR" "api" "cfdb-api" "clientAuth"

echo
echo "Done. Worker mTLS material written under $SCRIPT_DIR/"
echo "  CA:     $CA_CERT"
echo "  worker: $WORKER_DIR/worker-cert.pem  $WORKER_DIR/worker-key.pem"
echo "  api:    $API_DIR/api-cert.pem  $API_DIR/api-key.pem"
