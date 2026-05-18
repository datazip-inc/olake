package olake

import (
	"os"

	_ "github.com/datazip-inc/olake/destination/arrow/iceberg"  // arrow adapter registration (arrowdst.RegisteredAdapters[Iceberg])
	_ "github.com/datazip-inc/olake/destination/arrow/parquet"  // arrow adapter registration (arrowdst.RegisteredAdapters[Parquet])
	_ "github.com/datazip-inc/olake/destination/legacy/iceberg" // legacy Writer registration (legacydst.RegisteredWriters[Iceberg])
	_ "github.com/datazip-inc/olake/destination/legacy/parquet" // legacy Writer registration (legacydst.RegisteredWriters[Parquet])
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
