# oracle fragment for the root Makefile (see the driver-fragment contract there).

PROBE.oracle = docker exec oracle-23c bash -c "echo 'SELECT 1 FROM dual;' | sqlplus -s system/secret1234@//localhost:1521/ORCL"
