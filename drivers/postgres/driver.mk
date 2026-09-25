# postgres fragment for the root Makefile (see the driver-fragment contract there).

PROBE.postgres = docker exec olake_postgres-test psql -h localhost -U postgres -d postgres -c "SELECT 1"
