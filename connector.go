package olake

import (
	"os"

	_ "github.com/datazip-inc/olake/destination/iceberg" // registering iceberg destination
	_ "github.com/datazip-inc/olake/destination/parquet" // registering parquet destination
	"github.com/datazip-inc/olake/drivers/abstract"
	protocol "github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/safego"
)

func RegisterDriver(driver abstract.DriverInterface) {
	defer safego.Recovery(true)

	// Execute the root command
	err := protocol.CreateRootCommand(true, driver).Execute()
	if err != nil {
		protocol.ReportFailure(err)
		// constants.ErrNonRetryable means retrying will not fix this: give a process supervisor
		// (shell retry loop, systemd, Kubernetes restart policy) a distinct exit code and a
		// greppable log line so it can stop retrying and page a human instead of looping forever.
		if utils.IsNonRetryable(err) {
			logger.FatalNonRetryable(err)
		}
		logger.Fatal(err)
	}

	os.Exit(0)
}
