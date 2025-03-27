#!/bin/bash

function psql_exec() {
  echo "$1" | docker exec -i postgres-primary psql -U olake -d testdb
}

# Wait for primary to be ready
sleep 5

# Enable replication settings
psql_exec "ALTER SYSTEM SET wal_level = 'replica';"
psql_exec "ALTER SYSTEM SET max_wal_senders = 10;"
psql_exec "SELECT pg_reload_conf();"

echo "PostgreSQL primary node setup completed."
