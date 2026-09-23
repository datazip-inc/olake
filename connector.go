package olake

import (
	"os"

	"github.com/datazip-inc/olake/constants"
	_ "github.com/datazip-inc/olake/destination/iceberg" // registering iceberg destination
	_ "github.com/datazip-inc/olake/destination/parquet" // registering parquet destination
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
		protocol.ReportFailure(err)
		if constants.IsNonRetryable(err) {
			logger.Errorf("FATAL: manual intervention required: %s", err)
			os.Exit(constants.ExitCodeManualIntervention)
		}
		logger.Fatal(err)
	}

	os.Exit(0)
}
