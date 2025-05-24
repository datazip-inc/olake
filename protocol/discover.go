package protocol

import (
	"errors"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/telemetry" // Add this import
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/spf13/cobra"
)

// discoverCmd represents the read command
var discoverCmd = &cobra.Command{
	Use:   "discover",
	Short: "discover command",
	PreRunE: func(_ *cobra.Command, _ []string) error {
		if configPath == "" {
			return fmt.Errorf("--config not passed")
		}

		if err := utils.UnmarshalFile(configPath, connector.GetConfigRef()); err != nil {
			return err
		}

		return nil
	},
	RunE: func(_ *cobra.Command, _ []string) error {
		// Discover Telemetry Tracking
		startTime := time.Now()
		var discoverError error
		var streamCount int

		defer func() {
			telemetry.TrackDiscoverCompleted(
				time.Since(startTime).Seconds(),
				discoverError == nil,
				streamCount,
				connector.Type(),
				discoverError,
			)
			telemetry.Flush()
		}()

		err := connector.Setup()
		if err != nil {
			discoverError = err
			return err
		}
		streams, err := connector.Discover(true)
		if err != nil {
			discoverError = err
			return err
		}

		streamCount = len(streams)
		if streamCount == 0 {
			discoverError = errors.New("no streams found in connector")
			return discoverError
		}

		types.LogCatalog(streams)
		return nil
	},
}
