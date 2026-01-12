#!/bin/bash
# Certificate generation script for MongoDB X.509 authentication

set -e

usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS] [HOSTNAME] [IP_ADDRESS]

Generate TLS certificates for MongoDB X.509 authentication.

Arguments:
  HOSTNAME      MongoDB server hostname (default: cvh-backend)
  IP_ADDRESS    MongoDB server IP address (default: 127.0.0.1)

Options:
  -h, --help    Show this help message and exit

Environment variables (used if arguments not provided):
  MONGODB_HOSTNAME    MongoDB server hostname
  MONGODB_IP          MongoDB server IP address

Configuration precedence:
  1. Command-line arguments (highest)
  2. Environment variables
  3. Defaults (lowest)

Examples:
  $(basename "$0")                                    # Use defaults (local dev)
  $(basename "$0") mongodb.example.com 10.0.1.50     # Production with args
  MONGODB_HOSTNAME=db.example.com $(basename "$0")   # Production with env var

Output:
  certs/ca/ca.pem                            - CA certificate (deploy everywhere)
  certs/server/mongodb-server-bundle.pem     - Server certificate bundle
  certs/clients/cfdb-api-bundle.pem          - API client certificate
  certs/clients/cfdb-materializer-bundle.pem - Materializer client certificate
EOF
    exit 0
}

# Parse options
case "${1:-}" in
    -h|--help)
        usage
        ;;
esac

CERT_DIR="$(cd "$(dirname "$0")" && pwd)"
DAYS_CA=3650      # 10 years
DAYS_CERT=365     # 1 year

# Organization details (customize as needed)
ORG="Abdenlab"
COUNTRY="US"

# MongoDB server hostname: arg1 > env var > default
MONGODB_HOSTNAME="${1:-${MONGODB_HOSTNAME:-cvh-backend}}"

# MongoDB server IP: arg2 > env var > default
MONGODB_IP="${2:-${MONGODB_IP:-127.0.0.1}}"

echo "=== CFDB Certificate Generation ==="
echo "Output directory: ${CERT_DIR}"
echo "MongoDB hostname: ${MONGODB_HOSTNAME}"
echo "MongoDB IP:       ${MONGODB_IP}"
echo ""

# Create directories
mkdir -p "${CERT_DIR}/ca" "${CERT_DIR}/server" "${CERT_DIR}/clients"

# =============================================================================
# Generate Root CA
# =============================================================================
echo "=== Generating Root CA ==="
openssl genrsa -out "${CERT_DIR}/ca/ca.key" 4096
openssl req -new -x509 -days ${DAYS_CA} \
    -key "${CERT_DIR}/ca/ca.key" \
    -out "${CERT_DIR}/ca/ca.pem" \
    -subj "/CN=CFDB Root CA/O=${ORG}/C=${COUNTRY}"

echo "  Created: ca/ca.key (private key - keep secure!)"
echo "  Created: ca/ca.pem (certificate - deploy to all containers)"

# =============================================================================
# Generate MongoDB Server Certificate
# =============================================================================
echo ""
echo "=== Generating MongoDB Server Certificate ==="

# Generate key and CSR
openssl genrsa -out "${CERT_DIR}/server/mongodb-server.key" 2048
openssl req -new \
    -key "${CERT_DIR}/server/mongodb-server.key" \
    -out "${CERT_DIR}/server/mongodb-server.csr" \
    -subj "/CN=${MONGODB_HOSTNAME}/O=${ORG}/C=${COUNTRY}"

# Create SAN extension config with configured hostname and IP
cat > "${CERT_DIR}/server/san.cnf" << SANEOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = ${MONGODB_HOSTNAME}

[v3_req]
subjectAltName = @alt_names

[alt_names]
DNS.1 = ${MONGODB_HOSTNAME}
DNS.2 = localhost
DNS.3 = mongodb
DNS.4 = cvh-backend
IP.1 = ${MONGODB_IP}
IP.2 = 127.0.0.1
SANEOF

# Sign with CA including SAN
openssl x509 -req -days ${DAYS_CERT} \
    -in "${CERT_DIR}/server/mongodb-server.csr" \
    -CA "${CERT_DIR}/ca/ca.pem" \
    -CAkey "${CERT_DIR}/ca/ca.key" \
    -CAcreateserial \
    -out "${CERT_DIR}/server/mongodb-server.pem" \
    -sha256 \
    -extfile "${CERT_DIR}/server/san.cnf" \
    -extensions v3_req

# Create server bundle (MongoDB requires key + cert in one file)
cat "${CERT_DIR}/server/mongodb-server.key" \
    "${CERT_DIR}/server/mongodb-server.pem" > \
    "${CERT_DIR}/server/mongodb-server-bundle.pem"

# Also create symlink with old name for backward compatibility
ln -sf mongodb-server-bundle.pem "${CERT_DIR}/server/cvh-backend-bundle.pem"

echo "  Created: server/mongodb-server-bundle.pem (server key+cert bundle)"
echo "  Created: server/cvh-backend-bundle.pem -> mongodb-server-bundle.pem (symlink)"

# =============================================================================
# Generate API Client Certificate
# =============================================================================
echo ""
echo "=== Generating API Client Certificate ==="
# Note: Client certificates must use a DIFFERENT Organization than server cert
# to avoid MongoDB thinking they're cluster members

openssl genrsa -out "${CERT_DIR}/clients/cfdb-api.key" 2048
openssl req -new \
    -key "${CERT_DIR}/clients/cfdb-api.key" \
    -out "${CERT_DIR}/clients/cfdb-api.csr" \
    -subj "/CN=cfdb-api/OU=Clients/O=${ORG}-Clients/C=${COUNTRY}"

openssl x509 -req -days ${DAYS_CERT} \
    -in "${CERT_DIR}/clients/cfdb-api.csr" \
    -CA "${CERT_DIR}/ca/ca.pem" \
    -CAkey "${CERT_DIR}/ca/ca.key" \
    -CAcreateserial \
    -out "${CERT_DIR}/clients/cfdb-api.pem" \
    -sha256

# Create client bundle
cat "${CERT_DIR}/clients/cfdb-api.key" \
    "${CERT_DIR}/clients/cfdb-api.pem" > \
    "${CERT_DIR}/clients/cfdb-api-bundle.pem"

echo "  Created: clients/cfdb-api-bundle.pem"

# =============================================================================
# Generate Materializer Client Certificate
# =============================================================================
echo ""
echo "=== Generating Materializer Client Certificate ==="

openssl genrsa -out "${CERT_DIR}/clients/cfdb-materializer.key" 2048
openssl req -new \
    -key "${CERT_DIR}/clients/cfdb-materializer.key" \
    -out "${CERT_DIR}/clients/cfdb-materializer.csr" \
    -subj "/CN=cfdb-materializer/OU=Clients/O=${ORG}-Clients/C=${COUNTRY}"

openssl x509 -req -days ${DAYS_CERT} \
    -in "${CERT_DIR}/clients/cfdb-materializer.csr" \
    -CA "${CERT_DIR}/ca/ca.pem" \
    -CAkey "${CERT_DIR}/ca/ca.key" \
    -CAcreateserial \
    -out "${CERT_DIR}/clients/cfdb-materializer.pem" \
    -sha256

# Create client bundle
cat "${CERT_DIR}/clients/cfdb-materializer.key" \
    "${CERT_DIR}/clients/cfdb-materializer.pem" > \
    "${CERT_DIR}/clients/cfdb-materializer-bundle.pem"

echo "  Created: clients/cfdb-materializer-bundle.pem"

# =============================================================================
# Set Permissions
# =============================================================================
echo ""
echo "=== Setting Permissions ==="
chmod 400 "${CERT_DIR}/ca/ca.key"
chmod 400 "${CERT_DIR}/server/"*.key 2>/dev/null || true
chmod 400 "${CERT_DIR}/clients/"*.key
chmod 444 "${CERT_DIR}/ca/ca.pem"
chmod 444 "${CERT_DIR}/server/"*.pem 2>/dev/null || true
chmod 444 "${CERT_DIR}/clients/"*.pem
echo "  Private keys: 400 (owner read only)"
echo "  Certificates: 444 (read only)"

# =============================================================================
# Cleanup temporary files
# =============================================================================
rm -f "${CERT_DIR}/server/"*.csr "${CERT_DIR}/clients/"*.csr "${CERT_DIR}/server/san.cnf"

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "=== Certificate Generation Complete ==="
echo ""
echo "Configuration used:"
echo "  MongoDB hostname: ${MONGODB_HOSTNAME}"
echo "  MongoDB IP:       ${MONGODB_IP}"
echo ""
echo "Files created:"
echo "  ${CERT_DIR}/ca/ca.pem                            - CA certificate"
echo "  ${CERT_DIR}/server/mongodb-server-bundle.pem     - MongoDB server bundle"
echo "  ${CERT_DIR}/clients/cfdb-api-bundle.pem          - API client bundle"
echo "  ${CERT_DIR}/clients/cfdb-materializer-bundle.pem - Materializer client bundle"
echo ""
echo "Server certificate SANs:"
echo "  DNS: ${MONGODB_HOSTNAME}, localhost, mongodb, cvh-backend"
echo "  IP:  ${MONGODB_IP}, 127.0.0.1"
echo ""
echo "MongoDB X.509 usernames (Subject DNs - RFC 2253 order):"
echo "  API:          C=${COUNTRY},O=${ORG}-Clients,OU=Clients,CN=cfdb-api"
echo "  Materializer: C=${COUNTRY},O=${ORG}-Clients,OU=Clients,CN=cfdb-materializer"
echo ""
echo "Usage examples:"
echo "  Local dev:    ./generate-certs.sh"
echo "  With args:    ./generate-certs.sh mongodb.example.com 10.0.1.50"
echo "  With env:     MONGODB_HOSTNAME=db.example.com MONGODB_IP=10.0.1.50 ./generate-certs.sh"
echo ""
echo "IMPORTANT: Keep ca/ca.key secure and never commit certificates to git!"
