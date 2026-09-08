# mssql fragment for the root Makefile (see the driver-fragment contract there).

PROBE.mssql = docker exec olake-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Password!123' -C -Q "SELECT 1"
# The MSSQL image does not run init scripts by itself; this is idempotent.
POST_SETUP.mssql = echo "Applying MSSQL one-time init (idempotent)..." && docker exec olake-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Password!123' -C -d master -i /docker-entrypoint-initdb.d/01-init.sql
