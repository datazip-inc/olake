#!/bin/bash

# Get script directory and project paths (resolve to absolute path)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CERTS_DIR="${SCRIPT_DIR}/certs"

# SAN config files from same directory
SERVER_SAN_CNF="${SCRIPT_DIR}/server-san.cnf"
CLIENT_SAN_CNF="${SCRIPT_DIR}/client-san.cnf"

# Validate SAN config files exist
if [[ ! -f "$SERVER_SAN_CNF" ]]; then
    echo "Error: Server SAN config not found at $SERVER_SAN_CNF"
    exit 1
fi

if [[ ! -f "$CLIENT_SAN_CNF" ]]; then
    echo "Error: Client SAN config not found at $CLIENT_SAN_CNF"
    exit 1
fi

# Clean up existing certificates
echo "Cleaning up existing certificates..."
rm -rf "$CERTS_DIR"
mkdir -p "$CERTS_DIR"
cd "$CERTS_DIR"

PASSWORD="testpassword"

echo "Generating CA..."
openssl req -new -x509 -keyout ca-key.pem -out ca-cert.pem -days 365 \
    -subj "/CN=test-ca" -passout pass:$PASSWORD

echo "Generating server keystore with SAN..."
keytool -genkey -noprompt -alias kafka-server -keyalg RSA -keystore kafka.server.keystore.jks \
    -storepass $PASSWORD -keypass $PASSWORD -dname "CN=localhost" \
    -ext SAN=dns:localhost,dns:kafka,ip:127.0.0.1

echo "Creating server CSR..."
keytool -keystore kafka.server.keystore.jks -alias kafka-server -certreq -file server.csr \
    -storepass $PASSWORD -keypass $PASSWORD

echo "Signing server certificate with SAN..."
openssl x509 -req -CA ca-cert.pem -CAkey ca-key.pem -in server.csr -out server-signed.pem \
    -days 365 -CAcreateserial -passin pass:$PASSWORD \
    -extfile "$SERVER_SAN_CNF" -extensions v3_req

echo "Importing CA into server keystore..."
keytool -keystore kafka.server.keystore.jks -alias ca-cert -import -file ca-cert.pem \
    -storepass $PASSWORD -noprompt

echo "Importing signed cert into server keystore..."
keytool -keystore kafka.server.keystore.jks -alias kafka-server -import -file server-signed.pem \
    -storepass $PASSWORD -noprompt

echo "Creating server truststore with CA..."
keytool -keystore kafka.server.truststore.jks -alias ca-cert -import -file ca-cert.pem \
    -storepass $PASSWORD -noprompt

echo "Generating client certificate with SAN..."
openssl genrsa -out client-key.pem 2048
openssl req -new -key client-key.pem -out client.csr -config "$CLIENT_SAN_CNF"
openssl x509 -req -CA ca-cert.pem -CAkey ca-key.pem -in client.csr -out client-cert.pem \
    -days 365 -CAcreateserial -passin pass:$PASSWORD \
    -extfile "$CLIENT_SAN_CNF" -extensions v3_req

# Create credential files for Kafka
echo "$PASSWORD" > keystore_creds
echo "$PASSWORD" > key_creds
echo "$PASSWORD" > truststore_creds

echo ""
echo "Certificates generated in $CERTS_DIR"
echo ""
echo "Using SAN configs from:"
echo "  Server: $SERVER_SAN_CNF"
echo "  Client: $CLIENT_SAN_CNF"
echo ""
echo "For Go client, use:"
echo "  CA cert:     $CERTS_DIR/ca-cert.pem"
echo "  Client cert: $CERTS_DIR/client-cert.pem"
echo "  Client key:  $CERTS_DIR/client-key.pem"
