# mssql fragment for the root Makefile (see the driver-fragment contract there).

PROBE.mssql = docker exec olake-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Password!123' -C -Q "SELECT 1"
