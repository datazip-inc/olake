package protocol

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/telemetry"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var discoverCmd = &cobra.Command{
	Use:   "discover",
	Short: "discover command",
	PreRunE: func(_ *cobra.Command, _ []string) error {
		if streamsPath != "" && differencePath != "" {
			return nil
		}
		if configPath == "" {
			return fmt.Errorf("--config not passed")
		}

		if err := utils.UnmarshalFile(configPath, connector.GetConfigRef(), true); err != nil {
			return err
		}
		destinationDatabasePrefix = utils.Ternary(destinationDatabasePrefix == "", connector.Type(), destinationDatabasePrefix).(string)
		viper.Set(constants.DestinationDatabasePrefix, destinationDatabasePrefix)
		if streamsPath != "" {
			if err := utils.UnmarshalFile(streamsPath, &catalog, false); err != nil {
				return fmt.Errorf("failed to read streams from %s: %s", streamsPath, err)
			}
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		if streamsPath != "" && differencePath != "" {
			return HandleDifference()
		}

		err := connector.Setup(cmd.Context())
		if err != nil {
			return err
		}

		// build discover ctx
		discoverTimeout := utils.Ternary(timeout == -1, constants.DefaultDiscoverTimeout, time.Duration(timeout)*time.Second).(time.Duration)
		discoverCtx, cancel := context.WithTimeout(cmd.Context(), discoverTimeout)
		defer cancel()

		streams, err := connector.Discover(discoverCtx)
		if err != nil {
			return err
		}

		if len(streams) == 0 {
			return errors.New("no streams found in connector")
		}
		types.LogCatalog(streams, catalog, connector.Type())

		// Discover Telemetry Tracking
		defer func() {
			telemetry.TrackDiscover(len(streams), connector.Type())
			logger.Infof("Discover completed, wait 5 seconds cleanup in progress...")
			time.Sleep(5 * time.Second)
		}()
		return nil
	},
}

// HandleDifference reads two streams.json files, computes the difference, and writes the result to difference_streams.json
func HandleDifference() error {
	var catalog1, catalog2 types.Catalog
	if err := utils.UnmarshalFile(streamsPath, &catalog1, false); err != nil {
		return fmt.Errorf("failed to read old catalog: %s", err)
	}
	if err := utils.UnmarshalFile(differencePath, &catalog2, false); err != nil {
		return fmt.Errorf("failed to read new catalog: %s", err)
	}

	diffCatalog := types.GetCatalogDifference(&catalog1, &catalog2, connector.Type())

	diffFilepath := viper.GetString(constants.DifferencePath)
	if err := logger.FileLoggerWithPath(diffCatalog, diffFilepath); err != nil {
		return fmt.Errorf("failed to write difference streams to %s: %w", diffFilepath, err)
	}

	logger.Infof("Successfully wrote stream differences to %s", diffFilepath)
	return nil
}
