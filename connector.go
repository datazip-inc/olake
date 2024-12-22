package olake

import (
	"os"

	"github.com/datazip-inc/olake/logger"
	protocol "github.com/datazip-inc/olake/protocol"
	_ "github.com/datazip-inc/olake/writers/local"
	_ "github.com/datazip-inc/olake/writers/s3"
	"github.com/piyushsingariya/relec/safego"
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
