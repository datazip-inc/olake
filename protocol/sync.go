package protocol

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/datazip-inc/olake/destination"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
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
		} else if catalogPath == "" {
			return fmt.Errorf("--catalog not passed")
		}

		// unmarshal source config
		if err := utils.UnmarshalFile(configPath, connector.GetConfigRef()); err != nil {
			return err
		}

		// unmarshal destination config
		destinationConfig = &types.WriterConfig{}
		if err := utils.UnmarshalFile(destinationConfigPath, destinationConfig); err != nil {
			return err
		}

		catalog = &types.Catalog{}
		if err := utils.UnmarshalFile(catalogPath, catalog); err != nil {
			return err
		}

		// default state
		state = &types.State{
			Type: types.StreamType,
		}
		if statePath != "" {
			if err := utils.UnmarshalFile(statePath, state); err != nil {
				return err
			}
		}

		state.RWMutex = &sync.RWMutex{}
		stateBytes, _ := state.MarshalJSON()
		logger.Infof("Running sync with state: %s", stateBytes)
		return nil
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		pool, err := destination.NewWriter(cmd.Context(), destinationConfig)
		if err != nil {
			return err
		}
		// setup conector first
		err = connector.Setup(cmd.Context())
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
		standardModeStreams := []types.StreamInterface{}
		cdcStreamsState := []*types.StreamState{}

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

			err := elem.Validate(source)
			if err != nil {
				logger.Warnf("Skipping; Configured Stream %s found invalid due to reason: %s", elem.ID(), err)
				return false
			}

			elem.StreamMetadata = sMetadata
			selectedStreams = append(selectedStreams, elem.ID())

			if elem.Stream.SyncMode == types.CDC || elem.Stream.SyncMode == types.STRICTCDC {
				cdcStreams = append(cdcStreams, elem)
				streamState, exists := stateStreamMap[fmt.Sprintf("%s.%s", elem.Namespace(), elem.Name())]
				if exists {
					cdcStreamsState = append(cdcStreamsState, streamState)
				}
			} else {
				standardModeStreams = append(standardModeStreams, elem)
			}

			return false
		})
		state.Streams = cdcStreamsState
		if len(selectedStreams) == 0 {
			return fmt.Errorf("no valid streams found in catalog")
		}

		logger.Infof("Valid selected streams are %s", strings.Join(selectedStreams, ", "))

		// start monitoring stats
		logger.StatsLogger(cmd.Context(), func() (int64, int64, int64) {
			return pool.SyncedRecords(), pool.ThreadCounter.Load(), pool.GetRecordsToSync()
		})

		clearDestinationFlag, _ := cmd.Flags().GetBool("clear-destination")
		// If the clear-destination flag is passed, perform a full reset.
		if clearDestinationFlag {
			logger.Info("--clear-destination flag detected. Preparing for a full reset.")

			// Step 1: Clear the destination for the selected streams.
			logger.Info("Clearing destination for selected streams...")
			if err := clearDestination(cmd.Context(), destinationConfig, selectedStreams); err != nil {
				// This is a critical failure. Stop the sync.
				return fmt.Errorf("failed to clear destination during reset: %w", err)
			}
			logger.Info("Destination cleared successfully.")

			// Step 2: Reset the state to ensure a fresh sync from the beginning.
			logger.Info("Resetting sync state for a fresh start.")
			state = &types.State{
				Type:    types.StreamType,
				RWMutex: &sync.RWMutex{},
			}
			logger.Info("Sync state has been reset.")
		}

		// Setup State for Connector
		connector.SetupState(state)
		// init group
		err = connector.Read(cmd.Context(), pool, standardModeStreams, cdcStreams)
		if err != nil {
			return fmt.Errorf("error occurred while reading records: %s", err)
		}
		logger.Infof("Total records read: %d", pool.SyncedRecords())
		state.LogWithLock()
		return nil
	},
}

// clearDestination clears the destination for the specified streams
func clearDestination(ctx context.Context, config *types.WriterConfig, selectedStreams []string) error {
	if len(selectedStreams) == 0 {
		return nil
	}

	// Create a temporary writer to call the Clear method
	writerFunc, found := destination.RegisteredWriters[config.Type]
	if !found {
		return fmt.Errorf("unsupported destination type: %s", config.Type)
	}

	writer := writerFunc()

	// Configure the writer
	if err := utils.Unmarshal(config.WriterConfig, writer.GetConfigRef()); err != nil {
		return fmt.Errorf("failed to configure writer for clear operation: %s", err)
	}

	// Check the destination
	if err := writer.Check(ctx); err != nil {
		return fmt.Errorf("failed to check destination for clear operation: %s", err)
	}

	// Call the Clear method
	if err := writer.Clear(selectedStreams); err != nil {
		return fmt.Errorf("failed to clear destination: %s", err)
	}

	return nil
}
