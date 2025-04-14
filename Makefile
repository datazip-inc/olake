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
	chmod +x $(shell pwd)/.githooks/pre-commit
	chmod +x $(shell pwd)/.githooks/commit-msg
	git config core.hooksPath $(shell pwd)/.githooks
