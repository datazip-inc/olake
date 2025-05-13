package olake

import (
	"os"

	protocol "github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/safego"
	_ "github.com/datazip-inc/olake/writers/iceberg" // registering local parquet writer
	_ "github.com/datazip-inc/olake/writers/parquet" // registering local parquet writer
)

func RegisterDriver(driver protocol.Driver) {
	defer safego.Recovery(true)

	// Execute the root command
	err := protocol.CreateRootCommand(true, driver).Execute()
	if err != nil {
		logger.Fatal(err)
	}

	os.Exit(0)
}
