#!/bin/sh
set -eu

BASE_URL="http://amoro:1630"
HDR_SOURCE="X-Request-Source: Web"

echo "Waiting for Amoro to be reachable..."
until curl -sf "${BASE_URL}/" >/dev/null 2>&1; do
  sleep 1
done

COOKIE_JAR="/tmp/amoro-cookies.txt"

echo "Logging into Amoro..."
curl -sf -c "${COOKIE_JAR}" \
  -X POST "${BASE_URL}/api/ams/v1/login" \
  -H "Content-Type: application/json" \
  -H "${HDR_SOURCE}" \
  -d '{"user":"admin","password":"admin"}' >/dev/null

echo "Checking if catalog 'olake_iceberg' already exists..."
if curl -sf -b "${COOKIE_JAR}" -H "${HDR_SOURCE}" "${BASE_URL}/api/ams/v1/catalogs" | grep -q '"catalogName":"olake_iceberg"'; then
  echo "Catalog already exists; skipping create."
  exit 0
fi

echo "Creating catalog 'olake_iceberg' (Iceberg JdbcCatalog on Postgres + MinIO warehouse)..."
curl -sf -b "${COOKIE_JAR}" \
  -X POST "${BASE_URL}/api/ams/v1/catalogs" \
  -H "Content-Type: application/json" \
  -H "${HDR_SOURCE}" \
  -d '{
    "name":"olake_iceberg",
    "type":"custom",
    "optimizerGroup":"local",
    "tableFormatList":["ICEBERG"],
    "storageConfig":{
      "storage.type":"S3",
      "s3.endpoint":"http://minio:9000",
      "s3.region":"us-east-1"
    },
    "authConfig":{
      "auth.type":"AKSK",
      "auth.aksk.access-key":"admin",
      "auth.aksk.secret-key":"password"
    },
    "properties":{
      "catalog-impl":"org.apache.iceberg.jdbc.JdbcCatalog",
      "uri":"jdbc:postgresql://postgres:5432/iceberg?ssl=false",
      "jdbc.user":"iceberg",
      "jdbc.password":"password",
      "jdbc.driver":"org.postgresql.Driver",
      "jdbc.schema-version":"V1",
      "warehouse":"s3a://warehouse/olake_iceberg/"
    },
    "tableProperties":{}
  }' >/dev/null

echo "Done."


