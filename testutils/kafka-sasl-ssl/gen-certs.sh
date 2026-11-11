#!/usr/bin/env bash
#
# Generates the test CA, the broker keystore and the client files.
# TEST ONLY: all keys and passwords in this folder are public.
#
# Output:
#   secrets/kafka-ca-1.crt           CA certificate (broker truststore)
#   secrets/kafka.keystore.p12       broker certificate and key
#   sigmoscan/kafka-ca-1.crt         CA certificate (client copy)
#   sigmoscan/sigmoscan.certificate.pem  client certificate
#   sigmoscan/sigmoscan.key          client key, encrypted with KEY_PASSWORD
#
# The client folder has the same layout as the one expected by
# example-new-kafka/kafka_config.py: <user>/kafka-ca-1.crt, <user>/<user>.certificate.pem, <user>/<user>.key
#
# Usage: ./gen-certs.sh   (overwrites the existing files)

set -euo pipefail

cd "$(dirname "$0")"

USER_NAME="sigmoscan"
KEY_PASSWORD="sigmoscan-key-password"
KEYSTORE_PASSWORD="broker-keystore-password"
# Names and IPs that clients can use in their bootstrap address (checked with ssl_check_hostname)
BROKER_SAN="DNS:localhost,DNS:kafka-sasl-ssl,IP:127.0.0.1"
DAYS=3650

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

mkdir -p secrets "$USER_NAME"

# --- CA ---------------------------------------------------------------------

cat > "$WORK/ca.cnf" <<EOF
[req]
distinguished_name = dn
prompt = no
[dn]
CN = SigmoScan test CA
[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always
EOF

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$WORK/ca.key" 2>/dev/null
openssl req -x509 -new -sha256 -days "$DAYS" \
    -key "$WORK/ca.key" -config "$WORK/ca.cnf" -extensions v3_ca \
    -out secrets/kafka-ca-1.crt

# --- Broker -----------------------------------------------------------------

cat > "$WORK/broker.ext" <<EOF
basicConstraints = CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = $BROKER_SAN
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid
EOF

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$WORK/broker.key" 2>/dev/null
openssl req -new -key "$WORK/broker.key" -subj "/CN=localhost" -out "$WORK/broker.csr"
openssl x509 -req -sha256 -days "$DAYS" -in "$WORK/broker.csr" \
    -CA secrets/kafka-ca-1.crt -CAkey "$WORK/ca.key" -CAcreateserial -CAserial "$WORK/ca.srl" \
    -extfile "$WORK/broker.ext" -out "$WORK/broker.crt" 2>/dev/null

openssl pkcs12 -export -name kafka \
    -in "$WORK/broker.crt" -inkey "$WORK/broker.key" -certfile secrets/kafka-ca-1.crt \
    -passout "pass:$KEYSTORE_PASSWORD" -out secrets/kafka.keystore.p12

# --- Client -----------------------------------------------------------------

cat > "$WORK/client.ext" <<EOF
basicConstraints = CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = clientAuth
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid
EOF

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
    -aes-256-cbc -pass "pass:$KEY_PASSWORD" -out "$USER_NAME/$USER_NAME.key" 2>/dev/null
openssl req -new -key "$USER_NAME/$USER_NAME.key" -passin "pass:$KEY_PASSWORD" \
    -subj "/CN=$USER_NAME" -out "$WORK/client.csr"
openssl x509 -req -sha256 -days "$DAYS" -in "$WORK/client.csr" \
    -CA secrets/kafka-ca-1.crt -CAkey "$WORK/ca.key" -CAserial "$WORK/ca.srl" \
    -extfile "$WORK/client.ext" -out "$USER_NAME/$USER_NAME.certificate.pem" 2>/dev/null

cp secrets/kafka-ca-1.crt "$USER_NAME/kafka-ca-1.crt"

# The CA key is not kept: run this script again to make new certificates.
echo "Done. Rebuild the broker image to use the new files: docker compose up --build"
