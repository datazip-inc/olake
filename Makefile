SHELL := /bin/bash
.DEFAULT_GOAL := help

GOPATH = $(shell go env GOPATH)
GO_VERSION = $(shell awk '/^go / {print "go"$$2; exit}' go.mod)

gomod:
	find . -name go.mod -execdir go mod tidy \;

golangci:
	GOTOOLCHAIN=$(GO_VERSION) go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest;
	$(GOPATH)/bin/golangci-lint run

trivy:
	trivy fs  --vuln-type  os,library --severity HIGH,CRITICAL .

gofmt:
	gofmt -l -s -w .

pre-commit:
	chmod +x $(shell pwd)/.githooks/pre-commit
	chmod +x $(shell pwd)/.githooks/commit-msg
	git config core.hooksPath $(shell pwd)/.githooks

# Mirrors CI's "Go Build and Lint" workflow (.github/workflows/golang-ci.yml):
# its lint job installs golangci-lint via `go install ...@latest` and runs it
# against the repo's .golangci.yml -- exactly what the golangci target does.
lint: golangci

# Referenced by the build-check job of the same workflow (root module, same
# command as the integration workflow's "Build Project" step; driver modules
# are compiled by their own test targets).
build:
	go build -v ./...

BASE_NO_CACHE ?=
BASE_CACHE_FLAG = $(if $(BASE_NO_CACHE),--no-cache --pull)
GO_VERSION_NUM = $(shell echo $(GO_VERSION) | sed 's/go//')

BASE_IMAGE_TAG ?= build-$(GO_VERSION)

.PHONY: docker.base.build
docker.base.build:
	@if [ -z "$(strip $(GO_VERSION_NUM))" ]; then \
		echo "ERROR: could not read the go version from go.mod."; \
		exit 1; \
	fi
	docker build --target build $(BASE_CACHE_FLAG) --build-arg GO_VERSION=$(GO_VERSION_NUM) -t olakego/base:$(BASE_IMAGE_TAG) -f base.Dockerfile .

# ============================================================================
# Database, dev-build and test targets
#
# `make help` lists every target.
# db.* targets manage the database stacks and nothing else; test.* targets
# only run tests and expect the databases they need to already be up
# (e.g. `make db.all.start` once, then iterate on test runs). The start
# targets are idempotent: compose up + wait-until-ready + one-time init.
# ============================================================================

# --- overridables -----------------------------------------------------------
COMPOSE ?= docker compose
MVN_IMAGE ?= maven:3.9-eclipse-temurin-17
GIT_VERSION ?= $(shell git describe --tags --abbrev=0 2>/dev/null || echo dev)
GIT_COMMITSHA ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
RELEASE_CHANNEL ?= dev

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

# --- readiness probes (mirroring .github/workflows/integration-tests-runner.yml)
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
#   BUILD_GUARD.<d>                    precondition check for dev.<d>.build
#   NON_CDC_DRIVERS += <d>             opt out of the 2PC suites
# plus driver-only targets. Recipes run from the repo root. Fragments are
# included before the driver lists below are derived and before the templates
# are expanded, so list traits set here take effect.
-include drivers/*/driver.mk

# --- driver lists -------------------------------------------------------------
# A driver is any drivers/ subdir with its own go.mod; the ones that also have
# a docker-compose.yml get db.* stacks and test targets (s3 has no local stack).
BUILD_DRIVERS := $(notdir $(patsubst %/go.mod,%,$(wildcard drivers/*/go.mod)))
SOURCE_DRIVERS := $(filter $(BUILD_DRIVERS),$(notdir $(patsubst %/docker-compose.yml,%,$(wildcard drivers/*/docker-compose.yml))))
CDC_DRIVERS := $(filter-out $(NON_CDC_DRIVERS),$(SOURCE_DRIVERS))
INTEGRATION_PKGS := $(addsuffix /internal/...,$(addprefix ./drivers/,$(SOURCE_DRIVERS)))
CDC_PKGS := $(addsuffix /internal/...,$(addprefix ./drivers/,$(CDC_DRIVERS)))

# --- source databases (generated per driver) ---------------------------------
define SOURCE_DB_template
.PHONY: db.$(1).start db.$(1).stop db.$(1).teardown db.$(1).restart db.$(1).refresh
db.$(1).start:
	$$(COMPOSE) -f drivers/$(1)/docker-compose.yml up -d
	@$$(call wait_ready,$(1))
	@$$(POST_SETUP.$(1))

db.$(1).stop:
	$$(COMPOSE) -f drivers/$(1)/docker-compose.yml down --remove-orphans

db.$(1).teardown:
	$$(COMPOSE) -f drivers/$(1)/docker-compose.yml down --volumes --remove-orphans

# restart = stop then start (keeps volumes + data); refresh = teardown then start
# (wipes them). Both sequenced via sub-make so `make -j` can't start the stack
# before the down completes.
db.$(1).restart:
	@$$(MAKE) --no-print-directory db.$(1).stop
	@$$(MAKE) --no-print-directory db.$(1).start
db.$(1).refresh:
	@$$(MAKE) --no-print-directory db.$(1).teardown
	@$$(MAKE) --no-print-directory db.$(1).start
endef
$(foreach d,$(SOURCE_DRIVERS),$(eval $(call SOURCE_DB_template,$(d))))

db.source.all.start: $(addprefix db.,$(addsuffix .start,$(SOURCE_DRIVERS)))
db.source.all.stop: $(addprefix db.,$(addsuffix .stop,$(SOURCE_DRIVERS)))
db.source.all.teardown: $(addprefix db.,$(addsuffix .teardown,$(SOURCE_DRIVERS)))
db.source.all.restart:
	@$(MAKE) --no-print-directory db.source.all.stop
	@$(MAKE) --no-print-directory db.source.all.start
db.source.all.refresh:
	@$(MAKE) --no-print-directory db.source.all.teardown
	@$(MAKE) --no-print-directory db.source.all.start

# --- destination stack --------------------------------------------------------
db.destination.all.start:
	mkdir -p $(DEST_DATA_DIR)/minio-data $(DEST_DATA_DIR)/postgres-data $(DEST_DATA_DIR)/ivy-cache
	$(COMPOSE) -f $(DEST_COMPOSE) up -d $(DEST_SERVICES)
	@$(call wait_ready,minio)
	@$(call wait_ready,spark)

db.destination.all.stop:
	$(COMPOSE) -f $(DEST_COMPOSE) down --remove-orphans

db.destination.all.teardown:
	$(COMPOSE) -f $(DEST_COMPOSE) down --volumes --remove-orphans
	@rm -rf $(DEST_DATA_DIR) || { echo "Could not remove $(DEST_DATA_DIR) (root-owned files on Linux?). Try: sudo rm -rf $(DEST_DATA_DIR)"; exit 1; }
	@echo "Removed docker volumes and $(DEST_DATA_DIR) (minio/postgres data and the hive-metastore ivy cache)"
db.destination.all.restart:
	@$(MAKE) --no-print-directory db.destination.all.stop
	@$(MAKE) --no-print-directory db.destination.all.start
db.destination.all.refresh:
	@$(MAKE) --no-print-directory db.destination.all.teardown
	@$(MAKE) --no-print-directory db.destination.all.start

db.all.start: db.source.all.start db.destination.all.start
db.all.stop: db.source.all.stop db.destination.all.stop
db.all.teardown: db.source.all.teardown db.destination.all.teardown
db.all.restart:
	@$(MAKE) --no-print-directory db.all.stop
	@$(MAKE) --no-print-directory db.all.start
db.all.refresh:
	@$(MAKE) --no-print-directory db.all.teardown
	@$(MAKE) --no-print-directory db.all.start

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

# --- dev builds (generated per driver, incl. s3) ------------------------------
define DEV_BUILD_template
.PHONY: dev.$(1).build
dev.$(1).build:
	@$$(BUILD_GUARD.$(1))
	cd drivers/$(1) && go mod tidy && go build -ldflags="-w -s -X constants/constants.version=$$(GIT_VERSION) -X constants/constants.commitsha=$$(GIT_COMMITSHA) -X constants/constants.releasechannel=$$(RELEASE_CHANNEL)" -o olake main.go
	@echo "Built drivers/$(1)/olake (version $$(GIT_VERSION), commit $$(GIT_COMMITSHA))"
endef
$(foreach d,$(BUILD_DRIVERS),$(eval $(call DEV_BUILD_template,$(d))))

# --- tests --------------------------------------------------------------------
define INTEGRATION_TEST_template
.PHONY: test.integration.$(1)
test.integration.$(1): db.$(1).start db.destination.all.start $$(ICEBERG_JAR)
	go test -v ./drivers/$(1)/internal/... -timeout 0 -count=1 -run 'Integration'
endef
$(foreach d,$(SOURCE_DRIVERS),$(eval $(call INTEGRATION_TEST_template,$(d))))

define TWO_PC_TEST_template
.PHONY: test.2pc.$(1)
test.2pc.$(1): db.$(1).start db.destination.all.start $$(ICEBERG_JAR)
	go test -v ./drivers/$(1)/internal/... -timeout 0 -count=1 -run '2PC'
endef
$(foreach d,$(CDC_DRIVERS),$(eval $(call TWO_PC_TEST_template,$(d))))

test.integration: db.all.start $(ICEBERG_JAR)
	go test -v -p $(words $(SOURCE_DRIVERS)) $(INTEGRATION_PKGS) -timeout 0 -count=1 -run 'Integration'

test.2pc: $(addprefix db.,$(addsuffix .start,$(CDC_DRIVERS))) db.destination.all.start $(ICEBERG_JAR)
	go test -v -p $(words $(CDC_DRIVERS)) $(CDC_PKGS) -timeout 0 -count=1 -run '2PC'

# Unit tests across every module in the go.work workspace. Directory patterns
# ({{.Dir}}/...), not module-path patterns: in a go.work workspace a path pattern
# like <module>/... prefix-matches into sibling modules.
test.unit:
	go list -m -f '{{.Dir}}/...' | xargs go test -v -count=1 -skip 'Integration|2PC|Performance|Rebalance'

# --- help ----------------------------------------------------------------------
help:
	@echo "OLake Makefile  (SOURCE_DRIVERS: $(SOURCE_DRIVERS))"
	@echo ""
	@echo "Code quality:"
	@printf "  %-44s %s\n" "lint" "run CI lint locally (golangci-lint, alias of golangci)"
	@printf "  %-44s %s\n" "build" "compile the root module (CI build-check)"
	@printf "  %-44s %s\n" "gomod / golangci / trivy / gofmt / pre-commit" "tidy, lint, format and git-hook targets"
	@echo ""
	@echo "Source databases (compose up + wait until ready; stop keeps volumes):"
	@$(foreach d,$(SOURCE_DRIVERS),printf "  %-44s %s\n" "db.$(d).start" "start + wait for $(d)";)
	@$(foreach d,$(SOURCE_DRIVERS),printf "  %-44s %s\n" "db.$(d).stop" "stop $(d) (keep volumes + data)";)
	@$(foreach d,$(SOURCE_DRIVERS),printf "  %-44s %s\n" "db.$(d).teardown" "stop $(d) + remove volumes";)
	@$(foreach d,$(SOURCE_DRIVERS),printf "  %-44s %s\n" "db.$(d).restart" "stop then start $(d) (keep data)";)
	@$(foreach d,$(SOURCE_DRIVERS),printf "  %-44s %s\n" "db.$(d).refresh" "teardown then start $(d) (wipe data)";)
	@printf "  %-44s %s\n" "db.source.all.<verb>" "verb = start|stop|teardown|restart|refresh, all source DBs (make -j8)"
	@echo ""
	@echo "Destination stack (minio + mc + iceberg catalog + spark-connect):"
	@printf "  %-44s %s\n" "db.destination.all.start|stop" "the iceberg/parquet test stack"
	@printf "  %-44s %s\n" "db.destination.all.restart" "stop then start (keep data)"
	@printf "  %-44s %s\n" "db.destination.all.teardown" "down --volumes + DELETE $(DEST_DATA_DIR)"
	@printf "  %-44s %s\n" "db.destination.all.refresh" "teardown then start (fresh stack)"
	@printf "  %-44s %s\n" "db.all.<verb>" "same verbs, sources + destination together"
	@echo ""
	@echo "Dev builds:"
	@$(foreach d,$(BUILD_DRIVERS),printf "  %-44s %s\n" "dev.$(d).build" "host binary at drivers/$(d)/olake";)
	@echo ""
	@echo "Tests (auto-provision the databases they need):"
	@$(foreach d,$(SOURCE_DRIVERS),printf "  %-44s %s\n" "test.integration.$(d)" "integration suite for $(d)";)
	@$(foreach d,$(CDC_DRIVERS),printf "  %-44s %s\n" "test.2pc.$(d)" "2PC recovery suite for $(d)";)
	@printf "  %-44s %s\n" "test.rebalance.kafka" "kafka consumer-group rebalance recovery suite"
	@printf "  %-44s %s\n" "test.integration | test.2pc | test.unit" "aggregate runs (CI-equivalent)"
	@echo ""
	@echo "Overridables: SOURCE_DRIVERS COMPOSE WAIT_RETRIES WAIT_SLEEP"

.PHONY: lint build \
	db.source.all.start db.source.all.stop db.source.all.teardown db.source.all.restart db.source.all.refresh \
	db.destination.all.start db.destination.all.stop db.destination.all.teardown db.destination.all.restart db.destination.all.refresh \
	db.all.start db.all.stop db.all.teardown db.all.restart db.all.refresh \
	test.integration test.2pc test.unit help
