# db2 fragment for the root Makefile (see the driver-fragment contract there).

# The db2 container initializes a full instance on first boot; probe slowly.
WAIT_RETRIES.db2 := 30
WAIT_SLEEP.db2 := 25
PROBE.db2 = docker exec db2-test bash -c "su - db2inst1 -c 'db2 connect to TESTDB'"
BUILD_GUARD.db2 = test -n "$$IBM_DB_HOME" || { echo "ERROR: building drivers/db2 needs the IBM clidriver (CGO). Set IBM_DB_HOME/CGO_CFLAGS/CGO_LDFLAGS/LD_LIBRARY_PATH first (see setup_db2_clidriver in build.sh)."; exit 1; }
