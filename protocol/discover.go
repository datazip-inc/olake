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
		if configPath == "" {
			return fmt.Errorf("--config not passed")
		}

		if err := utils.UnmarshalFile(configPath, connector.GetConfigRef(), true); err != nil {
			return err
		}
		if syncID != "" {
			viper.Set(constants.SyncID, syncID)
		}
		if streamsPath != "" {
			if err := utils.UnmarshalFile(streamsPath, &catalog, false); err != nil {
				return fmt.Errorf("failed to read streams from %s: %s", streamsPath, err)
			}
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
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
