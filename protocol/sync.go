package protocol

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
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
		pool, err := NewWriter(cmd.Context(), destinationConfig)
		if err != nil {
			return err
		}
		// setup conector first
		err = connector.Setup()
		if err != nil {
			return err
		}
		// Get Source Streams
		streams, err := connector.Discover(false)
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
		cdcStreams := []Stream{}
		standardModeStreams := []Stream{}
		_, _ = utils.ArrayContains(catalog.Streams, func(elem *types.ConfiguredStream) bool {

			sMetadata, selected := selectedStreamsMap[fmt.Sprintf("%s.%s", elem.Namespace(), elem.Name())]
			// Check if the stream is in the selectedStreamMap
			if !(catalog.SelectedStreams == nil || selected) {
				logger.Warnf("Skipping stream %s.%s; not in selected streams.", elem.Name(), elem.Namespace())
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

			if elem.Stream.SyncMode == types.CDC {
				cdcStreams = append(cdcStreams, elem)
			} else {
				standardModeStreams = append(standardModeStreams, elem)
			}

			return false
		})
		logger.Infof("Valid selected streams are %s", strings.Join(selectedStreams, ", "))

		// start monitoring stats
		logger.StatsLogger(cmd.Context(), func() (int64, int64, int64) {
			return pool.SyncedRecords(), pool.threadCounter.Load(), pool.GetRecordsToSync()
		})

		// Setup State for Connector
		connector.SetupState(state)

		// Execute driver ChangeStreams mode
		GlobalCxGroup.Add(func(_ context.Context) error { // context is not used to keep processes mutually exclusive
			if connector.ChangeStreamSupported() {
				driver, yes := connector.(ChangeStreamDriver)
				if !yes {
					return fmt.Errorf("%s does not implement ChangeStreamDriver", connector.Type())
				}

				logger.Info("Starting ChangeStream process in driver")

				err := driver.RunChangeStream(pool, cdcStreams...)
				if err != nil {
					return fmt.Errorf("error occurred while reading records: %s", err)
				}
			}
			return nil
		})

		// Execute streams in Standard Stream mode
		// TODO: Separate streams with FULL and Incremental here only
		utils.ConcurrentInGroup(GlobalCxGroup, standardModeStreams, func(_ context.Context, stream Stream) error { // context is not used to keep processes mutually exclusive
			logger.Infof("Reading stream[%s] in %s", stream.ID(), stream.GetSyncMode())

			streamStartTime := time.Now()
			err := connector.Read(pool, stream)
			if err != nil {
				return fmt.Errorf("error occurred while reading records: %s", err)
			}

			logger.Infof("Finished reading stream %s[%s] in %s", stream.Name(), stream.Namespace(), time.Since(streamStartTime).String())

			return nil
		})

		if err := GlobalCxGroup.Block(); err != nil {
			return err
		}

		// wait for writer pool to finish
		if err := pool.Wait(); err != nil {
			return fmt.Errorf("error occurred in writer pool: %s", err)
		}

		logger.Infof("Total records read: %d", pool.SyncedRecords())
		state.LogWithLock()

		return nil
	},
}
