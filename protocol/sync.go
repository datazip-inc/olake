package protocol

import (
	"fmt"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/telemetry"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// syncCmd represents the read command
var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Olake sync command",
	PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
		if configPath == "" {
			return fmt.Errorf("--config not passed")
		} else if destinationConfigPath == "" {
			return fmt.Errorf("--destination not passed")
		} else if streamsPath == "" {
			return fmt.Errorf("--catalog not passed")
		}

		// unmarshal source config
		if err := utils.UnmarshalFile(configPath, connector.GetConfigRef(), true); err != nil {
			return err
		}

		// unmarshal destination config
		destinationConfig = &types.WriterConfig{}
		if err := utils.UnmarshalFile(destinationConfigPath, destinationConfig, true); err != nil {
			return err
		}

		// to set prefix for "test_olake" db created by OLake
		if destinationDatabasePrefix != "" {
			viper.Set(constants.DestinationDatabasePrefix, destinationDatabasePrefix)
		}

		catalog = &types.Catalog{}
		if err := utils.UnmarshalFile(streamsPath, catalog, false); err != nil {
			return err
		}

		syncID = utils.ComputeConfigHash(configPath, destinationConfigPath)

		// default state
		state = &types.State{
			Type: types.StreamType,
		}
		if statePath != "" {
			if err := utils.UnmarshalFile(statePath, state, false); err != nil {
				return err
			}
		}

		state.RWMutex = &sync.RWMutex{}
		stateBytes, _ := state.MarshalJSON()
		logger.Infof("Running sync with state: %s", stateBytes)
		return nil
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		// setup conector first
		err := connector.Setup(cmd.Context())
		if err != nil {
			return err
		}
		// Get Source Streams
		streams, err := connector.Discover(cmd.Context())
		if err != nil {
			return err
		}

		// get all types of selected streams
		selectedStreamsMetadata, err := GetStreamsClassification(catalog, streams, state)
		if err != nil {
			return fmt.Errorf("failed to get selected streams for clearing: %s", err)
		}

		// for clearing streams
		dropStreams := []types.StreamInterface{}
		dropStreams = append(dropStreams, selectedStreamsMetadata.FullLoadStreams...)
		if len(dropStreams) > 0 {
			logger.Infof("Clearing state for full refresh streams")
			// get the state for modification in clearstate
			connector.SetupState(state)
			if state, err = connector.ClearState(dropStreams); err != nil {
				return fmt.Errorf("error clearing state for full refresh streams: %s", err)
			}
			cerr := destination.ClearDestination(cmd.Context(), destinationConfig, dropStreams)
			if cerr != nil {
				return fmt.Errorf("failed to clear destination: %w", err)
			}
		}

		pool, err := destination.NewWriterPool(cmd.Context(), destinationConfig, selectedStreamsMetadata.SelectedStreams, batchSize)
		if err != nil {
			return err
		}

		// start monitoring stats
		logger.StatsLogger(cmd.Context(), func() (int64, int64, int64) {
			stats := pool.GetStats()
			return stats.ThreadCount.Load(), stats.TotalRecordsToSync.Load(), stats.ReadCount.Load()
		})

		// Setup State for Connector
		connector.SetupState(state)
		// Sync Telemetry tracking
		telemetry.TrackSyncStarted(syncID, streams, selectedStreamsMetadata.SelectedStreams, selectedStreamsMetadata.CDCStreams, connector.Type(), destinationConfig, catalog)
		defer func() {
			telemetry.TrackSyncCompleted(err == nil, pool.GetStats().ReadCount.Load())
			logger.Infof("Sync completed, wait 5 seconds cleanup in progress...")
			time.Sleep(5 * time.Second)
		}()

		// init group
		err = connector.Read(cmd.Context(), pool, selectedStreamsMetadata.FullLoadStreams, selectedStreamsMetadata.CDCStreams, selectedStreamsMetadata.IncrementalStreams)
		if err != nil {
			return fmt.Errorf("error occurred while reading records: %s", err)
		}
		state.LogWithLock()
		logger.Infof("Total records read: %d", pool.GetStats().ReadCount.Load())
		return nil
	},
}
