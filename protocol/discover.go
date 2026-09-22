package protocol

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/telemetry"
	"github.com/datazip-inc/olake/utils/version"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// isStreamDifferenceCommand reports whether both old and new streams flags were passed
func isStreamDifferenceCommand() bool {
	hasOldStreams := streamsPath != "" || availableStreamsPath != "" || selectedStreamsPath != ""
	hasNewStreams := differencePath != "" || differenceAvailableStreamsPath != "" || differenceSelectedStreamsPath != ""
	return hasOldStreams && hasNewStreams
}

var discoverCmd = &cobra.Command{
	Use:   "discover",
	Short: "discover command",
	PreRunE: func(_ *cobra.Command, _ []string) (err error) {
		if err := validateCatalogFlags(false); err != nil {
			return err
		}
		if isStreamDifferenceCommand() {
			return nil
		}
		if configPath == "" {
			return errs.Precondition(errs.ConfigInvalid, codeFlagMissing, fmt.Errorf("--config not passed"))
		}

		if err := utils.UnmarshalFile(configPath, connector.GetConfigRef(), true); err != nil {
			return err
		}
		destinationDatabasePrefix = utils.Ternary(destinationDatabasePrefix == "", connector.Type(), destinationDatabasePrefix).(string)
		viper.Set(constants.DestinationDatabasePrefix, destinationDatabasePrefix)
		if streamsPath != "" {
			legacyCatalog, err = types.ResolveLegacyCatalog(streamsPath)
			if err != nil {
				return err
			}
		}
		if availableStreamsPath != "" || selectedStreamsPath != "" {
			catalog, err = types.ResolveCatalog("", availableStreamsPath, selectedStreamsPath)
			if err != nil {
				return err
			}
		}

		//version
		logger.Infof("Running OLake sync with version %s", version.GetOlakeCLIVersion())

		return nil
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		if isStreamDifferenceCommand() {
			return compareStreams()
		}

		err := connector.Setup(cmd.Context())
		if err != nil {
			return err
		}

		// build discover ctx
		discoverTimeout := utils.Ternary(timeout == -1, constants.DefaultDiscoverTimeout, time.Duration(timeout)*time.Second).(time.Duration)
		discoverCtx, cancel := context.WithTimeout(cmd.Context(), discoverTimeout)
		defer cancel()

		streams, err := connector.Discover(discoverCtx, maxDiscoverThreads, false)
		if err != nil {
			return err
		}

		if len(streams) == 0 {
			return errs.Precondition(errs.ObjectNotFound, codeNoStreams,
				errors.New("no streams found in connector"))
		}
		types.LogCatalog(streams, catalog, legacyCatalog, connector.Type())

		// Discover Telemetry Tracking
		// Added this check to avoid the sleep when tracking telemetry is disabled
		if !telemetry.Disabled() {
			defer func() {
				telemetry.TrackDiscover(len(streams), connector.Type())
				logger.Infof("Discover completed, wait 5 seconds cleanup in progress...")
				time.Sleep(5 * time.Second)
			}()
		}
		return nil
	},
}

// compareStreams reads two catalogs, computes the difference, and writes the result to difference_streams.json
func compareStreams() error {
	oldStreams, err := types.ResolveCatalog(streamsPath, availableStreamsPath, selectedStreamsPath)
	if err != nil {
		return fmt.Errorf("failed to read old catalog: %w", err)
	}

	newStreams, err := types.ResolveCatalog(differencePath, differenceAvailableStreamsPath, differenceSelectedStreamsPath)
	if err != nil {
		return fmt.Errorf("failed to read new catalog: %w", err)
	}

	diffCatalog := types.GetStreamsDelta(oldStreams, newStreams)
	// log the difference catalog to stdout

	if err := diffCatalog.WriteToFile(viper.GetString(constants.DifferencePath)); err != nil {
		return fmt.Errorf("failed to write difference streams: %w", err)
	}
	logger.Infof("Successfully wrote stream differences")
	message := types.Message{
		Type:    types.CatalogMessage,
		Catalog: diffCatalog,
	}
	logger.Info(message)
	return nil
}
