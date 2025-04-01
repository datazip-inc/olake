GOPATH = $(shell go env GOPATH)

gomod:
	find . -name go.mod -execdir go mod tidy \;

golangci:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest;
	$(GOPATH)/bin/golangci-lint run

trivy:
	trivy fs  --vuln-type  os,library --severity HIGH,CRITICAL .

gofmt:
	gofmt -l -s -w .

pre-commit:
	PYTHON=$$(command -v python3 || command -v python); \
	if [ -z "$$PYTHON" ]; then echo "Error: Python not found"; exit 1; fi; \
	$$PYTHON -m venv .venv; \
	. .venv/bin/activate && pip install pre-commit; \
	.venv/bin/pre-commit install
