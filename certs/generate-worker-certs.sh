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
# Usage:
#   ./certs/generate-worker-certs.sh [--force] [extra-SAN ...]
#
#   --force        Overwrite existing certificates instead of skipping.
#   extra-SAN ...  Additional subjectAltName entries to embed in the
#                  leaf certs (e.g. a worker's LAN hostname/IP). Each is
#                  auto-classified as IP:<x> or DNS:<x>.
#
#   -h, --help     Show this help and exit.

set -euo pipefail

usage() {
    sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//; $d'
}

FORCE=0
EXTRA_SANS=()
for arg in "$@"; do
    case "$arg" in
        -h|--help)
            usage
            exit 0
            ;;
        --force)
            FORCE=1
            ;;
        *)
            EXTRA_SANS+=("$arg")
            ;;
    esac
done

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

# Build the subjectAltName list. localhost / loopback / 0.0.0.0 cover
# the single-host LAN-dev case; callers can append the worker's actual
# advertised hostname/IP as extra args.
build_san() {
    local sans="DNS:localhost,IP:127.0.0.1,IP:0.0.0.0"
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

SAN="$(build_san)"

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
# $1 = output dir, $2 = file stem, $3 = CN
mint_leaf() {
    local dir="$1" stem="$2" cn="$3"
    local key="$dir/${stem}-key.pem"
    local cert="$dir/${stem}-cert.pem"
    local csr="$dir/${stem}.csr"

    if [ "$FORCE" -eq 0 ] && [ -f "$cert" ] && [ -f "$key" ]; then
        echo "${stem} cert already exists at $cert (use --force to regenerate)"
        return
    fi

    echo "Generating ${stem} cert -> $cert (CN=$cn, SAN=$SAN)"
    openssl req -newkey "rsa:$KEY_BITS" -nodes \
        -keyout "$key" -out "$csr" \
        -subj "/CN=$cn"
    openssl x509 -req -in "$csr" \
        -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial \
        -out "$cert" -days "$DAYS" -sha256 \
        -extfile <(printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\n' "$SAN")
    rm -f "$csr"
}

mint_leaf "$WORKER_DIR" "worker" "cfdb-worker"
mint_leaf "$API_DIR" "api" "cfdb-api"

echo
echo "Done. Worker mTLS material written under $SCRIPT_DIR/"
echo "  CA:     $CA_CERT"
echo "  worker: $WORKER_DIR/worker-cert.pem  $WORKER_DIR/worker-key.pem"
echo "  api:    $API_DIR/api-cert.pem  $API_DIR/api-key.pem"
