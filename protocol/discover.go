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

var discoverCmd = &cobra.Command{
	Use:   "discover",
	Short: "discover command",
	PreRunE: func(_ *cobra.Command, _ []string) (err error) {
		if err := validateCatalogFlags(false); err != nil {
			return err
		}
		if err := validateDifferenceFlags(); err != nil {
			return err
		}
		// optional: the stream difference uses its type to skip fields the destination ignores
		if err := loadDestinationConfig(); err != nil {
			return err
		}
		if convertStreams {
			if streamsPath == "" {
				return errs.Precondition(errs.ConfigInvalid, codeFlagMissing,
					fmt.Errorf("--convert-streams requires --streams"))
			}
			return nil
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
		if streamsPath != "" || (availableStreamsPath != "" && selectedStreamsPath != "") {
			catalog, err = types.ResolveCatalog(streamsPath, availableStreamsPath, selectedStreamsPath)
			if err != nil {
				return err
			}
		}

		//version
		logger.Infof("Running OLake sync with version %s", version.GetOlakeCLIVersion())

		return nil
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		if convertStreams {
			return convertLegacyStreams()
		}
		if isStreamDifferenceCommand() {
			return compareStreams()
		}

		// resolved before the slow discover so a bad flag or destination fails fast
		availableUpdateTypes, err := resolveAvailableUpdateTypes()
		if err != nil {
			return err
		}

		err = connector.Setup(cmd.Context())
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
		types.LogCatalog(streams, catalog, availableUpdateTypes, destinationConfig.Type)

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

// isStreamDifferenceCommand reports whether both old and new streams flags were passed
func isStreamDifferenceCommand() bool {
	hasOldStreams := streamsPath != "" || (availableStreamsPath != "" && selectedStreamsPath != "")
	hasNewStreams := differencePath != "" || (differenceAvailableStreamsPath != "" && differenceSelectedStreamsPath != "")
	return hasOldStreams && hasNewStreams
}

// convertLegacyStreams rewrites the --streams file as available_streams.json and selected_streams.json.
// used to convert raw file to new format without discovering the source
func convertLegacyStreams() error {
	legacy, err := types.ResolveLegacyCatalog(streamsPath)
	if err != nil {
		return err
	}
	if err := types.WriteConvertedCatalog(legacy); err != nil {
		return err
	}
	logger.Infof("Successfully converted %s into available_streams and selected_streams", streamsPath)
	return nil
}

// resolveAvailableUpdateTypes turns --target-query-engines and --destination into the delete
// formats every stream may use. Neither input is persisted, so a caller that wants them applied
// passes both flags on every discover.
func resolveAvailableUpdateTypes() ([]types.UpdateType, error) {
	engines, err := types.ParseQueryEngines(targetQueryEngines)
	if err != nil {
		return nil, errs.Precondition(errs.ConfigInvalid, codeQueryEngineInvalid, err)
	}

	available, err := types.AvailableUpdateTypes(engines, destinationConfig)
	if err != nil {
		return nil, errs.Precondition(errs.ConfigInvalid, codeDestinationConfigInvalid, err)
	}

	// Not fatal: append writes no deletes, and the empty list tells the UI to warn. Expected,
	// so silent, for an append-only destination.
	if len(available) == 0 && types.UpsertSupported(destinationConfig.Type) {
		logger.Warnf("No delete format is readable by all of the selected query engines %v and writable to the destination; upsert streams need append mode to sync", engines)
	}

	return available, nil
}

// compareStreams reads the old and new catalogs, computes the difference, and writes the result to
// difference_streams.json, or to difference_available_streams.json and difference_selected_streams.json
// for the v2 format.
func compareStreams() error {
	oldStreams, err := types.ResolveCatalog(streamsPath, availableStreamsPath, selectedStreamsPath)
	if err != nil {
		return fmt.Errorf("failed to read old catalog: %w", err)
	}

	newStreams, err := types.ResolveCatalog(differencePath, differenceAvailableStreamsPath, differenceSelectedStreamsPath)
	if err != nil {
		return fmt.Errorf("failed to read new catalog: %w", err)
	}

	diffCatalog := types.GetStreamsDelta(oldStreams, newStreams, destinationConfig.Type)
	if len(diffCatalog.Streams) == 0 && len(diffCatalog.SelectedStreams) == 0 {
		logger.Infof("No differences found between the old and new catalogs")
	}

	if differenceSelectedStreamsPath != "" {
		err = diffCatalog.WriteSplitToFiles(viper.GetString(constants.DifferenceAvailableStreamsPath), viper.GetString(constants.DifferenceSelectedStreamsPath))
	} else {
		err = diffCatalog.WriteToFile(viper.GetString(constants.DifferencePath))
	}
	if err != nil {
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
