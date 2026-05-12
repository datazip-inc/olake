package olake

import (
	"os"

	_ "github.com/datazip-inc/olake/destination/arrow/iceberg"  // registering iceberg arrow adapter
	_ "github.com/datazip-inc/olake/destination/arrow/parquet"  // registering parquet arrow adapter
	_ "github.com/datazip-inc/olake/destination/legacy/iceberg" // registering iceberg destination
	_ "github.com/datazip-inc/olake/destination/legacy/parquet" // registering parquet destination
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
