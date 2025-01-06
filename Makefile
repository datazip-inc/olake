GOPATH = $(shell go env GOPATH)

gomod:
	find . -name go.mod -execdir go mod tidy \;

golangci:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.63.4;
	$(GOPATH)/bin/golangci-lint run


trivy:
	trivy fs  --vuln-type  os,library --severity HIGH,CRITICAL .