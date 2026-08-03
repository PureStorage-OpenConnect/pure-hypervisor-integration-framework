#!/usr/bin/env bash
# Generate a self-signed TLS cert/key for PHIF into ./certs/.
#
# Usage: deploy/gen-certs.sh [HOST]
#   HOST = the IP or DNS name clients will use (added to the cert SAN).
#          Defaults to "localhost".
#
# For a publicly-resolvable domain, use a real CA (e.g. Let's Encrypt) instead
# and drop the resulting fullchain/privkey in as certs/tls.crt and certs/tls.key.
set -euo pipefail

HOST="${1:-localhost}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$ROOT/certs"
mkdir -p "$DIR"

# Build the SAN: always include localhost + 127.0.0.1, plus the supplied host
# as either an IP or DNS entry depending on its form.
SAN="DNS:localhost,DNS:phif,IP:127.0.0.1"
if [[ "$HOST" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  SAN="$SAN,IP:$HOST"
elif [[ "$HOST" != "localhost" ]]; then
  SAN="$SAN,DNS:$HOST"
fi

openssl req -x509 -newkey rsa:2048 -nodes -days 825 \
  -keyout "$DIR/tls.key" -out "$DIR/tls.crt" \
  -subj "/CN=$HOST" \
  -addext "subjectAltName=$SAN"

chmod 600 "$DIR/tls.key"
chmod 644 "$DIR/tls.crt"
echo "Wrote $DIR/tls.crt and $DIR/tls.key (CN=$HOST, SAN=$SAN)"
