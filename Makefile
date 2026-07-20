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
