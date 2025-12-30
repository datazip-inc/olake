#!/bin/sh
set -eu

until psql -h postgres -U iceberg -d postgres -c 'select 1' >/dev/null 2>&1; do
  echo 'waiting for postgres...'
  sleep 1
done

# Fix potential glibc / collation version mismatch that blocks CREATE DATABASE (seen on macOS/colima etc.)
# Warnings are fine; we only need CREATE DATABASE to succeed.
psql -h postgres -U iceberg -d postgres -c "ALTER DATABASE template1 REFRESH COLLATION VERSION;" || true
psql -h postgres -U iceberg -d postgres -c "ALTER DATABASE postgres  REFRESH COLLATION VERSION;" || true

if psql -h postgres -U iceberg -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = 'amoro'" | grep -q 1; then
  echo 'amoro database already exists'
else
  # Use template0 to avoid failures when template1 has a collation mismatch.
  # If another concurrent run created it, ignore the harmless 'already exists' error.
  psql -h postgres -U iceberg -d postgres -c "CREATE DATABASE amoro TEMPLATE template0" || true
fi


