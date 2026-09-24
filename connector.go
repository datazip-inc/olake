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

	//it ensures the function recovers from unexpected panics and avoids
	// crashing the whole process in an uncontrolled way.
	defer safego.Recovery(true)

	// Execute the root command
	err := protocol.CreateRootCommand(true, driver).Execute()
	if err != nil {
		// centralized way to notify the protocol layer or reporting pipiline that a failure happend
		protocol.ReportFailure(err)

		//Checking if the error is non-retryable
		if constants.IsNonRetryable(err) {
			// logs fatala the clear message - manual interventaion is required
			logger.Errorf("FATAL: manual intervention required: %s", err)

			//ExitCodeIntervention is a status code,
			//  which is intended to singal that te system
			// should not simply retry automatically
			os.Exit(constants.ExitCodeManualIntervention)
		}
		// if error is retryablefunction call back to logger.Fatal(err)
		//no error occurs the funtion calls Exit(0)
		logger.Fatal(err)
	}

	//Indicate the successful completion

	os.Exit(0)
}
