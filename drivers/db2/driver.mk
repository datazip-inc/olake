# db2 fragment for the root Makefile (see the driver-fragment contract there).

# The db2 container initializes a full instance on first boot; probe slowly.
WAIT_RETRIES.db2 := 30
WAIT_SLEEP.db2 := 25
PROBE.db2 = docker exec db2-test bash -c "su - db2inst1 -c 'db2 connect to TESTDB'"

# Compiling drivers/db2 needs cgo against IBM's clidriver. prepare.db2 finds
# an existing install or downloads the build for this OS/arch (mirrors
# download_db2_clidriver/setup_db2_clidriver in build.sh, incl. the search
# order); CLIDRIVER_DIR defaults to where it installs. On macOS it then bakes
# an absolute install name into libdb2.dylib: IBM ships it with a bare name
# that is only resolvable through DYLD_LIBRARY_PATH, and SIP strips DYLD_*
# around the hardened-runtime go binary, so go-test binaries can never load it.
CLIDRIVER_DIR = $(firstword $(wildcard /opt/clidriver $(HOME)/clidriver $(CURDIR)/clidriver) $(HOME)/clidriver)
CLIDRIVER_URL := https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli

.PHONY: prepare.db2
prepare.db2:
	@if [ -d "$(CLIDRIVER_DIR)" ]; then echo "DB2 clidriver found at $(CLIDRIVER_DIR)"; \
	else \
		case "$$(uname -s)/$$(uname -m)" in \
			Darwin/arm64) f=macarm64_odbc_cli.tar.gz ;; \
			Darwin/*)     f=macos64_odbc_cli.tar.gz ;; \
			Linux/*)      f=linuxx64_odbc_cli.tar.gz ;; \
			*) echo "ERROR: no IBM clidriver download for $$(uname -s)/$$(uname -m)"; exit 1 ;; \
		esac; \
		echo "Downloading DB2 clidriver ($$f) to $(HOME)/clidriver ..."; \
		curl -fL "$(CLIDRIVER_URL)/$$f" | tar -xz -C "$(HOME)"; \
	fi
	@if [ "$$(uname -s)" = Darwin ] && [ "$$(otool -D "$(CLIDRIVER_DIR)/lib/libdb2.dylib" | tail -1)" = "libdb2.dylib" ]; then \
		echo "Baking absolute install name into libdb2.dylib (IBM's bare name is unresolvable under SIP)"; \
		install_name_tool -id "$(CLIDRIVER_DIR)/lib/libdb2.dylib" "$(CLIDRIVER_DIR)/lib/libdb2.dylib"; \
		codesign -f -s - "$(CLIDRIVER_DIR)/lib/libdb2.dylib"; \
	fi

# cgo env for every go command that touches drivers/db2, exported on the
# recipe line (not as make-level export vars) so it reaches every command of
# a pipeline -- test.unit feeds go test through xargs. Deliberately no
# DYLD_LIBRARY_PATH: it cannot survive SIP (see prepare.db2's install-name
# fix, which makes it unnecessary).
GO_ENV.db2 = export IBM_DB_HOME="$(CLIDRIVER_DIR)" CGO_CFLAGS="-I$(CLIDRIVER_DIR)/include" CGO_LDFLAGS="-L$(CLIDRIVER_DIR)/lib -Wl,-rpath,$(CLIDRIVER_DIR)/lib" LD_LIBRARY_PATH="$(CLIDRIVER_DIR)/lib";

# The multi-stage Dockerfile keeps db2 in its own final stage (clidriver + cgo env); the
# default build would produce the driverless driver-stage. Point docker.db2.build at it.
DOCKER_TARGET.db2 := db2-stage
