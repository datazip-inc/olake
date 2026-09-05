SHELL := /bin/bash
.DEFAULT_GOAL := help

GOPATH = $(shell go env GOPATH)
GO_VERSION = $(shell awk '/^go / {print "go"$$2; exit}' go.mod)
# A driver is any drivers/ subdir with its own go.mod (excludes util folders like abstract).
DRIVERS = $(notdir $(patsubst %/go.mod,%,$(wildcard drivers/*/go.mod)))
ROOT_MODULES := $(shell go list -m -f '{{.Dir}}')
TEST_MODULES := $(notdir $(shell cd tests && go list -m -f '{{.Dir}}'))

# Platform resolution from drivers/platforms.conf; PLATFORMS=... overrides it.
# parse_platforms_conf: driver $(1)'s entry; falls back to the '*' default when $(2) is non-empty.
# driver_platforms: driver entry, else the '*' default (what releases use).
# local_driver_platforms: explicit driver entry only; empty builds for the host arch.
PLATFORMS ?=
parse_platforms_conf = $(shell awk -F' *= *' -v d=$(1) -v use_def=$(2) '/^[[:space:]]*(\#|$$)/ {next} $$1==d {v=$$2} $$1=="*" {def=$$2} END {print (v != "" ? v : (use_def ? def : ""))}' drivers/platforms.conf)
driver_platforms = $(or $(PLATFORMS),$(call parse_platforms_conf,$(1),1))
local_driver_platforms = $(or $(PLATFORMS),$(call parse_platforms_conf,$(1)))

# Queried by release-tool.sh; PLATFORMS env/arg forces the list.
print.platforms.%:
	@echo $(call driver_platforms,$*)

.PHONY: gomod golangci.install trivy gofmt pre-commit

# Build a driver image locally, e.g. `make docker.postgres.build IMAGE_TAG=v1.2.3`.
# Drivers with an explicit entry in drivers/platforms.conf are pinned to it.
# Concrete (non-pattern) targets so they are phony and shells can autocomplete them.
IMAGE_TAG ?= local
# DOCKER_BUILD lets CI swap in buildx and a shared layer cache without changing local behaviour,
# where it stays a plain `docker build`.
DOCKER_BUILD ?= docker build
.PHONY: $(addsuffix .build,$(addprefix docker.,$(DRIVERS)))
$(addsuffix .build,$(addprefix docker.,$(DRIVERS))): docker.%.build:
	$(DOCKER_BUILD) $(addprefix --platform ,$(call local_driver_platforms,$*)) \
		--build-arg DRIVER_NAME=$* \
		-t olake/source-$*:$(IMAGE_TAG) .

gomod:
	find . -name go.mod -execdir go mod tidy \;

golangci.install:
	GOTOOLCHAIN=$(GO_VERSION) go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# One golangci-lint run over every product module in go.work. Relative patterns on purpose: an
# absolute <dir>/... for the root module sweeps into tests/, while ./... stays module-scoped.
olake.lint: golangci.install prepare.all
	$(foreach d,$(DRIVERS),$(GO_ENV.$(d))) $(GOPATH)/bin/golangci-lint run $(patsubst $(CURDIR)%,.%/...,$(ROOT_MODULES))

trivy:
	trivy fs  --vuln-type  os,library --severity HIGH,CRITICAL .

gofmt:
	gofmt -l -s -w .

# Relative on purpose: worktrees share .git/config, so an absolute path would point every worktree
# at whichever one ran this target last. Git resolves it against each worktree's own root.
pre-commit:
	chmod +x .githooks/pre-commit
	chmod +x .githooks/commit-msg
	git config core.hooksPath .githooks

test.lint: golangci.install prepare.all
	cd tests && $(foreach m,$(TEST_MODULES),($(GO_ENV.$(m)) $(GOPATH)/bin/golangci-lint run ./$(m)/...) &&) true

# Mirrors CI's "Go Build and Lint" workflow
lint: olake.lint test.lint

# Referenced by the build-check job of the same workflow (root module, same
# command as the integration workflow's "Build Project" step; driver modules
# are compiled by their own test targets).
build:
	go build -v ./...

# ============================================================================
# Infrastructure, dev-build and test targets
#
# `make help` lists every target.
# olake.* targets manage the olake infrastructure stacks and nothing else; test.* targets
# only run tests and expect the infrastructure they need to already be up
# (e.g. `make olake.all.start` once, then iterate on test runs). The start
# targets are idempotent: compose up + wait-until-ready + one-time init.
# ============================================================================

# --- overridables -----------------------------------------------------------
COMPOSE ?= docker compose
MVN_IMAGE ?= maven:3.9-eclipse-temurin-17

# --- destination stack / iceberg writer JAR ---------------------------------
DEST_COMPOSE := destination/iceberg/local-test/docker-compose.yml
DEST_DATA_DIR := destination/iceberg/local-test/data
# Only these services are needed by the tests; a bare `up -d` would also start
# the unused lakekeeper REST catalog and its migrate job.
DEST_SERVICES := minio mc postgres spark-iceberg
ICEBERG_WRITER_DIR := destination/iceberg/olake-iceberg-java-writer
ICEBERG_JAR := $(ICEBERG_WRITER_DIR)/target/olake-iceberg-java-writer-0.0.1-SNAPSHOT.jar
ICEBERG_JAR_SRCS := $(ICEBERG_WRITER_DIR)/pom.xml $(shell find $(ICEBERG_WRITER_DIR)/src -type f 2>/dev/null)
ROOT_JAR := olake-iceberg-java-writer.jar

IMAGE_JAR_DEP = $(if $(OLAKE_DRIVER_IMAGE),,$(ICEBERG_JAR))

# --- readiness probes (polled by olake.<d>.wait / olake.destination.all.wait, incl. in CI)
# Defaults only; per-driver probes and overrides live in drivers/<d>/driver.mk.
WAIT_RETRIES := 30
WAIT_SLEEP := 5
# Covers a fresh ivy cache: first spark boot downloads its --packages jars.
WAIT_RETRIES.spark := 60
PROBE.minio = curl -f http://localhost:9000/minio/health/live
PROBE.spark = docker exec spark-iceberg grep -q ':3A9A' /proc/net/tcp6

# Optional per-service recovery hooks: wait_ready runs RECOVER.<name> after
# every failed probe, for services that cannot come back by themselves. Hooks
# must be safe to repeat and safe to run concurrently with the service's own
# startup. spark: on a fresh container the connect server can lose the
# first-boot ivy download race to the image's thriftserver and die (see the
# spark-iceberg entrypoint note in $(DEST_COMPOSE)); the entrypoint's own
# supervisor loop normally restarts it, but this hook also unsticks containers
# created from older compose configs. Guarded: fires only once the shared
# cache jar exists (never starts a racing download) and 15002 is still
# closed; start-connect-server.sh itself refuses to double-start.
RECOVER.spark = docker exec spark-iceberg sh -c '[ -f /root/.ivy2/cache/org.apache.spark/spark-connect_2.12/jars/spark-connect_2.12-3.5.2.jar ] && ! grep -q :3A9A /proc/net/tcp6 && /opt/spark/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem'

# Readiness gate: expands (at recipe time) into ONE shell line and must occupy
# an entire recipe line by itself -- its `exit` terminates that line's shell.
# Never chain it with && on the same line. Probes only observe; if a
# RECOVER.<name> hook is defined it runs after each failed attempt to nudge
# the service back to life (a no-op for everything else).
#   in a static rule:       @$(call wait_ready,minio)
#   in an eval'd template:  @$$(call wait_ready,$(1))
wait_ready = echo "Waiting for $(1) (up to $(or $(WAIT_RETRIES.$(1)),$(WAIT_RETRIES)) x $(or $(WAIT_SLEEP.$(1)),$(WAIT_SLEEP))s)..."; \
	for i in $$(seq 1 $(or $(WAIT_RETRIES.$(1)),$(WAIT_RETRIES))); do \
	if { $(PROBE.$(1)); } >/dev/null 2>&1; then echo "$(1) is ready."; exit 0; fi; \
	echo "  $(1) not ready yet (attempt $$i)"; \
	$(if $(RECOVER.$(1)),{ $(RECOVER.$(1)); } >/dev/null 2>&1 || true;) \
	sleep $(or $(WAIT_SLEEP.$(1)),$(WAIT_SLEEP)); \
	done; echo "ERROR: $(1) did not become ready in time"; exit 1

# --- per-driver fragments -----------------------------------------------------
# Everything driver-specific lives in drivers/<driver>/driver.mk. A fragment
# can define, for its driver d:
#   PROBE.<d>                          readiness probe (required with a compose stack)
#   WAIT_RETRIES.<d> / WAIT_SLEEP.<d>  probe retry overrides
#   RECOVER.<d>                        nudge hook run after each failed probe
#   POST_SETUP.<d>                     one-time init after the stack is ready (idempotent)
#   prepare.<d>                        override of the no-op default below: provision
#                                      host build deps (every build/test target that
#                                      compiles <d> already depends on it). The driver
#                                      image build sets OVERLAY_DIR, a dir the
#                                      Dockerfile copies onto / of the runtime image,
#                                      for deps that must ship with the binary
#   GO_ENV.<d>                         `export VAR=...;` recipe-line prefix stitched
#                                      into every go command that compiles <d> (must
#                                      be shell `export`s so the env survives SIP and
#                                      reaches all commands of a pipeline)
#   NON_CDC_DRIVERS += <d>             opt out of the 2PC suites
# plus driver-only targets. Recipes run from the repo root. Fragments are
# included before the driver lists below are derived and before the templates
# are expanded, so list traits set here take effect.
HELP_TARGETS :=
-include drivers/*/driver.mk

# --- driver lists -------------------------------------------------------------
# A driver is any drivers/ subdir with its own go.mod; the ones that also have
# a docker-compose.yml get olake.* stacks and test targets.
SOURCE_DRIVERS := $(filter $(DRIVERS),$(notdir $(patsubst %/docker-compose.yml,%,$(wildcard drivers/*/docker-compose.yml))))
CDC_DRIVERS := $(filter-out $(NON_CDC_DRIVERS),$(SOURCE_DRIVERS))
SOURCE_PKGS := $(addsuffix /...,$(addprefix ./,$(SOURCE_DRIVERS)))
CDC_PKGS := $(addsuffix /...,$(addprefix ./,$(CDC_DRIVERS)))

# The drivers the integration suites cover, queried by CI (integration-tests.yml) so the list
# lives in this file only: it is what the driver matrix fans out to on a push to master.
.PHONY: print.source-drivers
print.source-drivers:
	@echo $(SOURCE_DRIVERS)

# Every driver module, suite or not. CI subtracts the two lists to tell a driver with no suite
# apart from shared code, so a PR touching only that driver runs nothing instead of everything.
.PHONY: print.drivers
print.drivers:
	@echo $(DRIVERS)

# --- prepare ------------------------------------------------------------------
# prepare.<d> provisions whatever driver d needs before it can compile; the
# default is a no-op. Every build/test target below that compiles a driver
# depends on its prepare.<d>, so a fragment override (db2: the IBM clidriver)
# makes those targets work on a fresh machine of any OS/arch. Passing
# OVERLAY_DIR (what the driver image build does) provisions into that dir
# instead of onto the host, for deps that must ship next to the binary.
prepare.%:
	@$(if $(OVERLAY_DIR),mkdir -p $(OVERLAY_DIR),true)
prepare.all: $(addprefix prepare.,$(DRIVERS))
.PHONY: prepare.all

# --- source databases (generated per driver) ---------------------------------
# start is split into up (create the containers) + wait (block until the stack
# answers, then run its one-time init): `make -j olake.all.up` pulls every image
# at once, and `make -j olake.all.wait` collapses all the probes into one parallel
# step, so slow boots (db2, spark) overlap with each other and with whatever
# runs between the two -- what CI does.
# The compose files one driver's stack is made of: its own, plus the override named by
# EXTRA_COMPOSE_<d> when set. CI sets EXTRA_COMPOSE_mysql for the mysql-5.7 matrix entry, so
# the same olake.mysql.* targets bring up a 5.7 server without a second set of targets; unset,
# every stack stays a plain single-file one. Only files named exactly docker-compose.yml make a
# directory a driver (see SOURCE_DRIVERS), so an override never becomes a driver of its own.
compose_files = -f drivers/$(1)/docker-compose.yml $(if $(EXTRA_COMPOSE_$(1)),-f $(EXTRA_COMPOSE_$(1)))

define SOURCE_DB_template
.PHONY: olake.$(1).up olake.$(1).wait olake.$(1).start olake.$(1).stop olake.$(1).teardown olake.$(1).restart olake.$(1).refresh
olake.$(1).up:
	$$(COMPOSE) $$(call compose_files,$(1)) up -d

olake.$(1).wait:
	@$$(call wait_ready,$(1))
	@$$(POST_SETUP.$(1))

# Sequenced via sub-make so `make -j` cannot probe a stack that is not up yet.
olake.$(1).start:
	@$$(MAKE) --no-print-directory olake.$(1).up
	@$$(MAKE) --no-print-directory olake.$(1).wait

olake.$(1).stop:
	$$(COMPOSE) $$(call compose_files,$(1)) down --remove-orphans

olake.$(1).teardown:
	$$(COMPOSE) $$(call compose_files,$(1)) down --volumes --remove-orphans

# restart = stop then start (keeps volumes + data); refresh = teardown then start
# (wipes them). Both sequenced via sub-make so `make -j` can't start the stack
# before the down completes.
olake.$(1).restart:
	@$$(MAKE) --no-print-directory olake.$(1).stop
	@$$(MAKE) --no-print-directory olake.$(1).start
olake.$(1).refresh:
	@$$(MAKE) --no-print-directory olake.$(1).teardown
	@$$(MAKE) --no-print-directory olake.$(1).start
endef
$(foreach d,$(SOURCE_DRIVERS),$(eval $(call SOURCE_DB_template,$(d))))

olake.source.all.up: $(addprefix olake.,$(addsuffix .up,$(SOURCE_DRIVERS)))
olake.source.all.wait: $(addprefix olake.,$(addsuffix .wait,$(SOURCE_DRIVERS)))
olake.source.all.start: $(addprefix olake.,$(addsuffix .start,$(SOURCE_DRIVERS)))
olake.source.all.stop: $(addprefix olake.,$(addsuffix .stop,$(SOURCE_DRIVERS)))
olake.source.all.teardown: $(addprefix olake.,$(addsuffix .teardown,$(SOURCE_DRIVERS)))
olake.source.all.restart:
	@$(MAKE) --no-print-directory olake.source.all.stop
	@$(MAKE) --no-print-directory olake.source.all.start
olake.source.all.refresh:
	@$(MAKE) --no-print-directory olake.source.all.teardown
	@$(MAKE) --no-print-directory olake.source.all.start

# --- destination stack --------------------------------------------------------
olake.destination.all.up:
	mkdir -p $(DEST_DATA_DIR)/minio-data $(DEST_DATA_DIR)/postgres-data $(DEST_DATA_DIR)/ivy-cache
	$(COMPOSE) -f $(DEST_COMPOSE) up -d $(DEST_SERVICES)

olake.destination.all.wait:
	@$(call wait_ready,minio)
	@$(call wait_ready,spark)

olake.destination.all.start:
	@$(MAKE) --no-print-directory olake.destination.all.up
	@$(MAKE) --no-print-directory olake.destination.all.wait

olake.destination.all.stop:
	$(COMPOSE) -f $(DEST_COMPOSE) down --remove-orphans

olake.destination.all.teardown:
	$(COMPOSE) -f $(DEST_COMPOSE) down --volumes --remove-orphans
	@rm -rf $(DEST_DATA_DIR) || { echo "Could not remove $(DEST_DATA_DIR) (root-owned files on Linux?). Try: sudo rm -rf $(DEST_DATA_DIR)"; exit 1; }
	@echo "Removed docker volumes and $(DEST_DATA_DIR) (minio/postgres data and the hive-metastore ivy cache)"
olake.destination.all.restart:
	@$(MAKE) --no-print-directory olake.destination.all.stop
	@$(MAKE) --no-print-directory olake.destination.all.start
olake.destination.all.refresh:
	@$(MAKE) --no-print-directory olake.destination.all.teardown
	@$(MAKE) --no-print-directory olake.destination.all.start

olake.all.up: olake.source.all.up olake.destination.all.up
olake.all.wait: olake.source.all.wait olake.destination.all.wait
olake.all.start: olake.source.all.start olake.destination.all.start
olake.all.stop: olake.source.all.stop olake.destination.all.stop
olake.all.teardown: olake.source.all.teardown olake.destination.all.teardown
olake.all.restart:
	@$(MAKE) --no-print-directory olake.all.stop
	@$(MAKE) --no-print-directory olake.all.start
olake.all.refresh:
	@$(MAKE) --no-print-directory olake.all.teardown
	@$(MAKE) --no-print-directory olake.all.start

# --- iceberg writer JAR (file rule: skips maven when up to date) --------------
# Refreshes the repo-root copy too: build.sh and the iceberg writer prefer it,
# so a stale root JAR would otherwise shadow a fresh target/ build.
$(ICEBERG_JAR): $(ICEBERG_JAR_SRCS)
	@if command -v mvn >/dev/null 2>&1; then \
		mvn -f $(ICEBERG_WRITER_DIR)/pom.xml clean package -Dmaven.test.skip=true; \
	else \
		echo "mvn not found; building JAR with dockerized Maven ($(MVN_IMAGE))"; \
		docker run --rm -v "$(CURDIR)/$(ICEBERG_WRITER_DIR)":/build -v olake-m2-cache:/root/.m2 -w /build $(MVN_IMAGE) mvn clean package -Dmaven.test.skip=true; \
	fi
	cp $(ICEBERG_JAR) $(ROOT_JAR)

# Phony alias so CI (and humans) can `make iceberg.jar` without knowing the file path.
.PHONY: iceberg.jar
iceberg.jar: $(ICEBERG_JAR)

# The Dockerfile COPYs the writer JAR, so every image build gets it as a prerequisite here. Separate
# from the docker.%.build rule above only because $(ICEBERG_JAR) is defined further down.
$(addsuffix .build,$(addprefix docker.,$(DRIVERS))): $(ICEBERG_JAR)

# --- dev builds (generated per driver, incl. s3) ------------------------------
# CGO_ENABLED=0 is set here, not left to the environment: db2 is the one driver that needs cgo
# and GO_ENV.db2 turns it back on right after (it is appended to this same recipe line). Without
# the explicit default, a db2 build -- or a cancelled one -- leaves CGO_ENABLED=1 in the caller's
# shell and the next driver silently links against libc.
define DEV_BUILD_template
.PHONY: dev.$(1).build
dev.$(1).build: prepare.$(1)
	export CGO_ENABLED=0; $$(GO_ENV.$(1)) cd drivers/$(1) && go build -o olake main.go
	@echo "Built drivers/$(1)/olake"
endef
$(foreach d,$(DRIVERS),$(eval $(call DEV_BUILD_template,$(d))))

# --- tests --------------------------------------------------------------------
# Everything one driver's suites need, brought up concurrently. A recursive -j sub-make, since plain
# prerequisites only run in parallel when the caller passes -j; every goal is idempotent.
driver_test_setup = $(MAKE) --no-print-directory -j3 olake.$(1).start olake.destination.all.start $(IMAGE_JAR_DEP)

# Compile the driver's test binary without running it, so CI pays the cold build while its container
# pull and image build are still in flight. Through make, for db2's clidriver and cgo env.
define TEST_BUILD_template
.PHONY: test.build.$(1)
test.build.$(1): prepare.$(1)
	$$(GO_ENV.$(1)) cd tests && go test -c -o /dev/null ./$(1)/...
endef
$(foreach d,$(SOURCE_DRIVERS),$(eval $(call TEST_BUILD_template,$(d))))

# Every driver's test binary at once. `cd tests && go build ./...` cannot do this: tests/ is a
# workspace root, not a module, so ./... matches nothing there.
.PHONY: test.build.all
test.build.all: $(addprefix test.build.,$(SOURCE_DRIVERS))

# The whole CI surface for one driver -- Discover, Sync, 2PC and (kafka) Rebalance in one `go test`.
# Performance is excluded: it needs external infra and has its own workflow.
define DRIVER_TEST_template
.PHONY: test.integration.$(1)
test.integration.$(1): prepare.$(1)
	@$$(call driver_test_setup,$(1))
	$$(GO_ENV.$(1)) cd tests && go test -v ./$(1)/... -timeout 0 -count=1 -skip 'Performance'
endef
$(foreach d,$(SOURCE_DRIVERS),$(eval $(call DRIVER_TEST_template,$(d))))

define DRIVER_SUITE_template
.PHONY: test.$(2).$(1)
test.$(2).$(1): prepare.$(1)
	@$$(call driver_test_setup,$(1))
	$$(GO_ENV.$(1)) cd tests && go test -v ./$(1)/... -timeout 0 -count=1 -run '$(3)'
endef
$(foreach d,$(SOURCE_DRIVERS),$(eval $(call DRIVER_SUITE_template,$(d),discover,Discover)))
$(foreach d,$(SOURCE_DRIVERS),$(eval $(call DRIVER_SUITE_template,$(d),sync,Sync)))
$(foreach d,$(CDC_DRIVERS),$(eval $(call DRIVER_SUITE_template,$(d),2pc,2PC)))

# Benchmarks. Deliberately no stack prerequisites: these run against the remote instances named
# in the driver's testdata/source.json (CI reaches them over a VPN), never the local compose
# stacks, so depending on olake.$(1).start would boot containers the suite never touches.
define PERFORMANCE_TEST_template
.PHONY: test.performance.$(1)
test.performance.$(1): prepare.$(1) $$(ICEBERG_JAR)
	$$(GO_ENV.$(1)) cd tests && go test -v ./$(1)/... -timeout 0 -count=1 -run 'Performance'
endef
$(foreach d,$(SOURCE_DRIVERS),$(eval $(call PERFORMANCE_TEST_template,$(d))))

test.discover: $(addprefix prepare.,$(SOURCE_DRIVERS)) olake.all.start $(IMAGE_JAR_DEP)
	$(foreach d,$(SOURCE_DRIVERS),$(GO_ENV.$(d))) cd tests && go test -v -p $(words $(SOURCE_DRIVERS)) $(SOURCE_PKGS) -timeout 0 -count=1 -run 'Discover'

test.sync: $(addprefix prepare.,$(SOURCE_DRIVERS)) olake.all.start $(IMAGE_JAR_DEP)
	$(foreach d,$(SOURCE_DRIVERS),$(GO_ENV.$(d))) cd tests && go test -v -p $(words $(SOURCE_DRIVERS)) $(SOURCE_PKGS) -timeout 0 -count=1 -run 'Sync'

test.2pc: $(addprefix prepare.,$(CDC_DRIVERS)) $(addprefix olake.,$(addsuffix .start,$(CDC_DRIVERS))) olake.destination.all.start $(IMAGE_JAR_DEP)
	$(foreach d,$(CDC_DRIVERS),$(GO_ENV.$(d))) cd tests && go test -v -p $(words $(CDC_DRIVERS)) $(CDC_PKGS) -timeout 0 -count=1 -run '2PC'


# Unit tests across every module in the go.work workspace. Directory patterns
# ({{.Dir}}/...), not module-path patterns: in a go.work workspace a path pattern
# like <module>/... prefix-matches into sibling modules.
test.unit: $(addprefix prepare.,$(DRIVERS))
	$(foreach d,$(DRIVERS),$(GO_ENV.$(d))) go list -m -f '{{.Dir}}/...' | xargs go test -v -count=1 -skip '^Test.*(Discover|Sync|2PC|Performance|Rebalance)$$'

define print_help_targets
$(foreach t,$(HELP_TARGETS), \
	printf "  %-44s %s\n" "$(t)" "$(HELP.$(t))";)
endef

# --- help ----------------------------------------------------------------------
help:
	@echo "OLake Makefile  (SOURCE_DRIVERS: $(SOURCE_DRIVERS))"
	@echo ""
	@echo "Code quality:"
	@printf "  %-44s %s\n" "lint" "run CI lint locally (olake.lint + test.lint)"
	@printf "  %-44s %s\n" "olake.lint" "golangci-lint over root + driver modules (incl. db2; provisions its clidriver)"
	@printf "  %-44s %s\n" "test.lint" "golangci-lint over tests/ modules (incl. db2; provisions its clidriver)"
	@printf "  %-44s %s\n" "build" "compile the root module (CI build-check)"
	@printf "  %-44s %s\n" "gomod / golangci.install / trivy / gofmt / pre-commit" "tidy, lint-install, format and git-hook targets"
	@echo ""
	@echo "Source stacks (compose up + wait until ready; stop keeps volumes):"
	@$(foreach d,$(SOURCE_DRIVERS),printf "  %-44s %s\n" "olake.$(d).start" "start + wait for $(d) (= olake.$(d).up then olake.$(d).wait)";)
	@$(foreach d,$(SOURCE_DRIVERS),printf "  %-44s %s\n" "olake.$(d).stop" "stop $(d) (keep volumes + data)";)
	@$(foreach d,$(SOURCE_DRIVERS),printf "  %-44s %s\n" "olake.$(d).teardown" "stop $(d) + remove volumes";)
	@$(foreach d,$(SOURCE_DRIVERS),printf "  %-44s %s\n" "olake.$(d).restart" "stop then start $(d) (keep data)";)
	@$(foreach d,$(SOURCE_DRIVERS),printf "  %-44s %s\n" "olake.$(d).refresh" "teardown then start $(d) (wipe data)";)
	@printf "  %-44s %s\n" "olake.source.all.<verb>" "verb = start|stop|teardown|restart|refresh, all source stacks (make -j8)"
	@printf "  %-44s %s\n" "olake.<driver>.up | olake.<driver>.wait" "the two halves of start, for running each in parallel"
	@echo ""
	@echo "Destination stack (minio + mc + iceberg catalog + spark-connect):"
	@printf "  %-44s %s\n" "olake.destination.all.start|stop" "the iceberg/parquet test stack"
	@printf "  %-44s %s\n" "olake.destination.all.restart" "stop then start (keep data)"
	@printf "  %-44s %s\n" "olake.destination.all.teardown" "down --volumes + DELETE $(DEST_DATA_DIR)"
	@printf "  %-44s %s\n" "olake.destination.all.refresh" "teardown then start (fresh stack)"
	@printf "  %-44s %s\n" "olake.all.<verb>" "same verbs, sources + destination together"
	@printf "  %-44s %s\n" "olake.all.up | olake.all.wait" "boot everything, then block on every probe (make -j)"
	@echo ""
	@echo "Dev builds:"
	@$(foreach d,$(DRIVERS),printf "  %-44s %s\n" "dev.$(d).build" "host binary at drivers/$(d)/olake";)
	@printf "  %-44s %s\n" "prepare.<driver> | prepare.all" "provision host build deps (db2: IBM clidriver; else no-op)"
	@echo ""
	@echo "Docker images:"
	@$(foreach d,$(DRIVERS),printf "  %-44s %s\n" "docker.$(d).build" "build the $(d) driver image (olake/source-$(d):$(IMAGE_TAG))";)
	@echo ""
	@echo "Tests (auto-provision the stacks they need):"
	@printf "  %-44s %s\n" "iceberg.jar" "build the Iceberg writer JAR (skips maven when up to date)"
	@$(foreach d,$(SOURCE_DRIVERS),printf "  %-44s %s\n" "test.integration.$(d)" "every suite for $(d) (what the matrix job runs)";)
	@$(foreach d,$(SOURCE_DRIVERS),printf "  %-44s %s\n" "test.discover.$(d)" "discover suite for $(d) (catalog equality check)";)
	@$(foreach d,$(SOURCE_DRIVERS),printf "  %-44s %s\n" "test.sync.$(d)" "sync suite for $(d) (full load, CDC, incremental)";)
	@$(foreach d,$(CDC_DRIVERS),printf "  %-44s %s\n" "test.2pc.$(d)" "2PC recovery suite for $(d)";)
	@$(foreach d,$(SOURCE_DRIVERS),printf "  %-44s %s\n" "test.performance.$(d)" "benchmark suite for $(d) (remote instances, no local stack)";)
	@printf "  %-44s %s\n" "test.discover | test.sync | test.2pc | test.unit" "aggregate runs (all drivers at once)"
	@printf "  %-44s %s\n" "test.build.all" "compile every driver's test binary (CI cache warm)"
	@if [ -n "$(strip $(HELP_TARGETS))" ]; then \
		echo ""; \
		echo "Driver-specific:"; \
		$(call print_help_targets) \
	fi
	@echo ""
	@echo "Overridables: SOURCE_DRIVERS COMPOSE WAIT_RETRIES WAIT_SLEEP IMAGE_TAG"

.PHONY: lint olake.lint test.lint build \
	olake.source.all.start olake.source.all.stop olake.source.all.teardown olake.source.all.restart olake.source.all.refresh \
	olake.destination.all.start olake.destination.all.stop olake.destination.all.teardown olake.destination.all.restart olake.destination.all.refresh \
	olake.all.start olake.all.stop olake.all.teardown olake.all.restart olake.all.refresh \
	test.discover test.sync test.2pc test.unit test.build.all help
