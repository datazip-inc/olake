package olake

import (
	"os"
	"strings"

	"github.com/datazip-inc/olake/constants"
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
		if utils.IsManualIntervention(err) {
			// always lead with the marker; drop it from the start of err so it isn't repeated
			msg := strings.TrimPrefix(err.Error(), constants.ErrManualIntervention.Error()+": ")
			logger.Errorf("FATAL: manual intervention required: %s", msg)

			os.Exit(constants.ExitCodeManualIntervention)
		}
		logger.Fatalf("failed, a restart may recover: %s", err)
	}

	os.Exit(0)
}
