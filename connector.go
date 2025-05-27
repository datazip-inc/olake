package olake

import (
	"os"

	_ "github.com/datazip-inc/olake/destination/iceberg" // registering local parquet writer
	_ "github.com/datazip-inc/olake/destination/parquet" // registering local parquet writer
	"github.com/datazip-inc/olake/drivers/abstract"
	protocol "github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/safego"
)

func RegisterDriver(driver abstract.DriverInterface) {
	defer safego.Recovery(true)

	// Execute the root command
	err := protocol.CreateRootCommand(true, driver).Execute()
	if err != nil {
		logger.Fatal(err)
	}

	os.Exit(0)
}
