package protocol

import (
	"fmt"
	"strings"
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

// various stream formats
type StreamClassification struct {
	SelectedStreams    []string
	CDCStreams         []types.StreamInterface
	IncrementalStreams []types.StreamInterface
	FullLoadStreams    []types.StreamInterface
	NewStreamsState    []*types.StreamState
}

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

		// Initialize state
		state = &types.State{
			Type: types.StreamType,
		}
		if statePath != "" {
			if err := utils.UnmarshalFile(statePath, state, false); err != nil {
				return err
			}
		} else {
			state.Version = constants.LatestStateVersion
		}
		// set state version to global variable to use throughout olake cli instance
		constants.LoadedStateVersion = state.Version

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
		selectedStreamsMetadata, err := classifyStreams(catalog, streams, state)
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
				return fmt.Errorf("failed to clear destination: %s", cerr)
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
		telemetry.TrackSyncStarted(syncID, streams, selectedStreamsMetadata.SelectedStreams, selectedStreamsMetadata.FullLoadStreams, selectedStreamsMetadata.CDCStreams, connector.Type(), destinationConfig, catalog)
		defer func() {
			telemetry.TrackSyncCompleted(syncID, err == nil, pool.GetStats().ReadCount.Load())
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

func classifyStreams(catalog *types.Catalog, streams []*types.Stream, state *types.State) (*StreamClassification, error) {
	// stream-specific classifications
	classifications := &StreamClassification{
		SelectedStreams:    []string{},
		CDCStreams:         []types.StreamInterface{},
		IncrementalStreams: []types.StreamInterface{},
		FullLoadStreams:    []types.StreamInterface{},
		NewStreamsState:    []*types.StreamState{},
	}
	// create a map for namespace and streamMetadata
	selectedStreamsMap := make(map[string]types.StreamMetadata)
	for namespace, streamsMetadata := range catalog.SelectedStreams {
		for _, streamMetadata := range streamsMetadata {
			selectedStreamsMap[fmt.Sprintf("%s.%s", namespace, streamMetadata.StreamName)] = streamMetadata
		}
	}

	// Create a map for quick state lookup by stream ID
	stateStreamMap := make(map[string]*types.StreamState)
	for _, stream := range state.Streams {
		stateStreamMap[fmt.Sprintf("%s.%s", stream.Namespace, stream.Stream)] = stream
	}

	_, _ = utils.ArrayContains(catalog.Streams, func(elem *types.ConfiguredStream) bool {
		sMetadata, selected := selectedStreamsMap[elem.ID()]
		// Check if the stream is in the selectedStreamMap
		if !(catalog.SelectedStreams == nil || selected) {
			logger.Debugf("Skipping stream %s.%s; not in selected streams.", elem.Namespace(), elem.Name())
			return false
		}

		if streams != nil {
			source, found := types.StreamsToMap(streams...)[elem.ID()]
			if !found {
				logger.Warnf("Skipping; Configured Stream %s not found in source", elem.ID())
				return false
			}

			types.MergeSelectedColumns(&sMetadata, elem.Stream.Schema, source.Schema, elem.Stream)

			elem.StreamMetadata = sMetadata
			err := elem.Validate(source)
			if err != nil {
				logger.Warnf("Skipping; Configured Stream %s found invalid due to reason: %s", elem.ID(), err)
				return false
			}
		}

		classifications.SelectedStreams = append(classifications.SelectedStreams, elem.ID())
		switch elem.Stream.SyncMode {
		case types.CDC, types.STRICTCDC:
			classifications.CDCStreams = append(classifications.CDCStreams, elem)
			streamState, exists := stateStreamMap[elem.ID()]
			if exists {
				classifications.NewStreamsState = append(classifications.NewStreamsState, streamState)
			}
		case types.INCREMENTAL:
			classifications.IncrementalStreams = append(classifications.IncrementalStreams, elem)
			streamState, exists := stateStreamMap[elem.ID()]
			if exists {
				classifications.NewStreamsState = append(classifications.NewStreamsState, streamState)
			}
		default:
			classifications.FullLoadStreams = append(classifications.FullLoadStreams, elem)
		}

		return false
	})
	// Clear previous state streams for non-selected streams.
	// Must not be called during clear destination to retain the global and stream state. (clear dest. -> when streams == nil)
	if streams != nil {
		state.Streams = classifications.NewStreamsState
	}
	if len(classifications.SelectedStreams) == 0 {
		return nil, fmt.Errorf("no valid streams found in catalog")
	}

	logger.Infof("Valid selected streams are %s", strings.Join(classifications.SelectedStreams, ", "))
	return classifications, nil
}
