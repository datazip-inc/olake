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
	PreRunE: func(_ *cobra.Command, _ []string) error {
		if streamsPath != "" && differencePath != "" {
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
			if err := utils.UnmarshalFile(streamsPath, &catalog, false); err != nil {
				return fmt.Errorf("failed to read streams from %s: %w", streamsPath, err)
			}
		}

		if err := resolveUpdateTypeConstraints(); err != nil {
			return err
		}

		//version
		logger.Infof("Ruuning OLake sync with version %s", version.GetOlakeCLIVersion())

		return nil
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		if streamsPath != "" && differencePath != "" {
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
		types.LogCatalog(streams, catalog, connector.Type(), updateTypeConstraints)

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

// resolveUpdateTypeConstraints reads what this discover run must satisfy: the target query
// engines from --target-query-engines and the destination's catalog from --destination. Both
// are pure inputs: the run turns them into each stream's available_update_types and keeps
// nothing else, so a caller that wants them applied passes both flags every time.
func resolveUpdateTypeConstraints() error {
	engines, err := types.ParseQueryEngines(targetQueryEngines)
	if err != nil {
		return errs.Precondition(errs.ConfigInvalid, codeQueryEngineInvalid, err)
	}

	if destinationConfigPath != "not-set" && destinationConfigPath != "" {
		destinationConfig = &types.WriterConfig{}
		if err := utils.UnmarshalFile(destinationConfigPath, destinationConfig, true); err != nil {
			return err
		}
	}
	catalogType, err := types.CatalogTypeFromConfig(destinationConfig)
	if err != nil {
		return errs.Precondition(errs.ConfigInvalid, codeDestinationConfigInvalid, err)
	}

	constraints := types.UpdateTypeConstraints{Engines: engines, Catalog: catalogType}
	// Only upsert needs a delete format; append writes none, so the job stays valid and
	// streams default to append.
	if len(constraints.Available()) == 0 {
		logger.Warnf("No delete format is readable by all of the selected query engines %v and writable to destination catalog %q; streams will default to append mode",
			engines, catalogType)
	}

	updateTypeConstraints = constraints
	return nil
}

// compareStreams reads two streams.json files, computes the difference, and writes the result to difference_streams.json
func compareStreams() error {
	var oldStreams, newStreams types.Catalog
	if serr := utils.UnmarshalFile(streamsPath, &oldStreams, false); serr != nil {
		return fmt.Errorf("failed to read old catalog: %w", serr)
	}

	if derr := utils.UnmarshalFile(differencePath, &newStreams, false); derr != nil {
		return fmt.Errorf("failed to read new catalog: %w", derr)
	}

	diffCatalog := types.GetStreamsDelta(&oldStreams, &newStreams)
	// log the difference catalog to stdout

	if err := logger.FileLoggerWithPath(diffCatalog, viper.GetString(constants.DifferencePath)); err != nil {
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
