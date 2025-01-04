package olake

import (
	"os"

	"github.com/datazip-inc/olake/logger"
	protocol "github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/safego"
	_ "github.com/datazip-inc/olake/writers/local"
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
