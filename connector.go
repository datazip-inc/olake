package olake

import (
	"fmt"
	"os"

	"github.com/datazip-inc/olake/logger"
	protocol "github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/safego"
)

var (
	globalDriver  protocol.Driver
	globalAdapter protocol.Writer
)

func RegisterDriver(driver protocol.Driver) {
	defer safego.Recovery(true)

	if globalAdapter != nil {
		logger.Fatal(fmt.Errorf("adapter already registered: %s", globalAdapter.Type()))
	}

	globalDriver = driver

	// Execute the root command
	err := protocol.CreateRootCommand(true, driver).Execute()
	if err != nil {
		logger.Fatal(err)
	}

	os.Exit(0)
}
