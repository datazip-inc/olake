package protocol

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/telemetry"
	"github.com/spf13/cobra"
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

		catalog = &types.Catalog{}
		if err := utils.UnmarshalFile(streamsPath, catalog, false); err != nil {
			return err
		}

		syncID = utils.ComputeConfigHash(configPath, destinationConfigPath)

		// default state
		state = &types.State{
			Type: types.StreamType,
		}
		if statePath != "" && !clearDestinationFlag {
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

		streamsMap := types.StreamsToMap(streams...)

		// create a map for namespace and streamMetadata
		selectedStreamsMap := make(map[string]types.StreamMetadata)
		for namespace, streamsMetadata := range catalog.SelectedStreams {
			for _, streamMetadata := range streamsMetadata {
				selectedStreamsMap[fmt.Sprintf("%s.%s", namespace, streamMetadata.StreamName)] = streamMetadata
			}
		}

		// Validating Streams and attaching State
		selectedStreams := []string{}
		cdcStreams := []types.StreamInterface{}
		incrementalStreams := []types.StreamInterface{}
		standardModeStreams := []types.StreamInterface{}
		newStreamsState := []*types.StreamState{}
		fullLoadStreams := []string{}

		var stateStreamMap = make(map[string]*types.StreamState)
		for _, stream := range state.Streams {
			stateStreamMap[fmt.Sprintf("%s.%s", stream.Namespace, stream.Stream)] = stream
		}
		_, _ = utils.ArrayContains(catalog.Streams, func(elem *types.ConfiguredStream) bool {
			sMetadata, selected := selectedStreamsMap[fmt.Sprintf("%s.%s", elem.Namespace(), elem.Name())]
			// Check if the stream is in the selectedStreamMap
			if !(catalog.SelectedStreams == nil || selected) {
				logger.Debugf("Skipping stream %s.%s; not in selected streams.", elem.Namespace(), elem.Name())
				return false
			}

			source, found := streamsMap[elem.ID()]
			if !found {
				logger.Warnf("Skipping; Configured Stream %s not found in source", elem.ID())
				return false
			}

			elem.StreamMetadata = sMetadata

			err := elem.Validate(source)
			if err != nil {
				logger.Warnf("Skipping; Configured Stream %s found invalid due to reason: %s", elem.ID(), err)
				return false
			}

			selectedStreams = append(selectedStreams, elem.ID())
			switch elem.Stream.SyncMode {
			case types.CDC, types.STRICTCDC:
				cdcStreams = append(cdcStreams, elem)
				streamState, exists := stateStreamMap[fmt.Sprintf("%s.%s", elem.Namespace(), elem.Name())]
				if exists {
					newStreamsState = append(newStreamsState, streamState)
				}
			case types.INCREMENTAL:
				incrementalStreams = append(incrementalStreams, elem)
				streamState, exists := stateStreamMap[fmt.Sprintf("%s.%s", elem.Namespace(), elem.Name())]
				if exists {
					newStreamsState = append(newStreamsState, streamState)
				}
			default:
				fullLoadStreams = append(fullLoadStreams, elem.ID())
				standardModeStreams = append(standardModeStreams, elem)
			}

			return false
		})
		state.Streams = newStreamsState
		if len(selectedStreams) == 0 {
			return fmt.Errorf("no valid streams found in catalog")
		}

		logger.Infof("Valid selected streams are %s", strings.Join(selectedStreams, ", "))

		fullLoadStreams = utils.Ternary(clearDestinationFlag, selectedStreams, fullLoadStreams).([]string)
		pool, err := destination.NewWriterPool(cmd.Context(), destinationConfig, selectedStreams, fullLoadStreams, batchSize)
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
		telemetry.TrackSyncStarted(syncID, streams, selectedStreams, cdcStreams, connector.Type(), destinationConfig, catalog)
		defer func() {
			telemetry.TrackSyncCompleted(err == nil, pool.GetStats().ReadCount.Load())
			logger.Infof("Sync completed, wait 5 seconds cleanup in progress...")
			time.Sleep(5 * time.Second)
		}()

		// init group
		err = connector.Read(cmd.Context(), pool, standardModeStreams, cdcStreams, incrementalStreams)
		if err != nil {
			return fmt.Errorf("error occurred while reading records: %s", err)
		}
		state.LogWithLock()
		logger.Infof("Total records read: %d", pool.GetStats().ReadCount.Load())
		return nil
	},
}
