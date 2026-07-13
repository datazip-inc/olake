# db2 fragment for the root Makefile (see the driver-fragment contract there).
# db2 is amd64-only (see drivers/platforms.conf, kept for the driver docker build);
# on a non-amd64 host run its db.db2.* / test.*.db2 targets under amd64 emulation.

# The db2 container initializes a full instance on first boot; probe slowly.
WAIT_RETRIES.db2 := 30
WAIT_SLEEP.db2 := 25
PROBE.db2 = docker exec db2-test bash -c "su - db2inst1 -c 'db2 connect to TESTDB'"
BUILD_GUARD.db2 = test -n "$$IBM_DB_HOME" || { echo "ERROR: building drivers/db2 needs the IBM clidriver (CGO). Set IBM_DB_HOME/CGO_CFLAGS/CGO_LDFLAGS/LD_LIBRARY_PATH first (see setup_db2_clidriver in build.sh)."; exit 1; }
# db2-specific make logic, included by the root Makefile. Overrides the
# generic prepare.%/build.% rules; recipes run from the repo root.

CLIDRIVER_DIR = $(OVERLAY_DIR)/opt/clidriver

prepare.db2:
	@[ "$$(uname -m)" = "x86_64" ] || { echo "DB2 driver is only supported on x86_64 (amd64); IBM provides no ARM64 clidriver."; exit 1; }
	mkdir -p $(OVERLAY_DIR)/opt $(OVERLAY_DIR)/etc/ld.so.conf.d
	go run github.com/ibmdb/go_ibm_db/installer@v0.4.5 $(OVERLAY_DIR)/opt
	echo /opt/clidriver/lib > $(OVERLAY_DIR)/etc/ld.so.conf.d/db2-clidriver.conf

# db2 needs cgo against the clidriver; rpath points at its runtime location.
build.db2: export IBM_DB_HOME = $(CLIDRIVER_DIR)
build.db2: export CGO_CFLAGS = -I$(CLIDRIVER_DIR)/include
build.db2: export CGO_LDFLAGS = -L$(CLIDRIVER_DIR)/lib -Wl,-rpath,/opt/clidriver/lib
build.db2: export LD_LIBRARY_PATH = $(CLIDRIVER_DIR)/lib
build.db2:
	go build -C drivers/db2 -o $(OUTPUT) main.go
