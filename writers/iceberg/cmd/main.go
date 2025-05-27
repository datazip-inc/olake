package main

import (
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	_ "github.com/datazip-inc/olake/writers/iceberg" // register the writer
)

func main() {
	err := protocol.CreateRootCommand(false, nil).Execute()
	if err != nil {
		logger.Fatalf("Failed to create iceberg destination root command: %s", err)
	}
}
