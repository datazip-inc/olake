package protocol

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/spf13/cobra"
)

// ReadCmd represents the read command
var ReadCmd = &cobra.Command{
	Use:   "read",
	Short: "Olake read command",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if config_ == "" {
			return fmt.Errorf("--config not passed")
		} else if destinationConfig_ == "" {
			return fmt.Errorf("--destination not passed")
		} else if catalog_ == "" {
			return fmt.Errorf("--catalog not passed")
		}

		// unmarshal source config
		if err := utils.UnmarshalFile(config_, connector.GetConfigRef()); err != nil {
			return err
		}

		// unmarshal destination config
		destinationConfig = &types.WriterConfig{}
		if err := utils.UnmarshalFile(destinationConfig_, destinationConfig); err != nil {
			return err
		}

		catalog = &types.Catalog{}
		if err := utils.UnmarshalFile(catalog_, catalog); err != nil {
			return err
		}

		// default state
		state = &types.State{
			Type: types.StreamType,
		}
		if state_ != "" {
			if err := utils.UnmarshalFile(state_, state); err != nil {
				return err
			}
		}
		state.Mutex = &sync.Mutex{}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		pool, err := NewWriter(cmd.Context(), destinationConfig)
		if err != nil {
			return err
		}

		// Get Source Streams
		streams, err := connector.Discover()
		if err != nil {
			return err
		}

		streamsMap := types.StreamsToMap(streams...)

		// Validating Streams and attaching State
		selectedStreams := []string{}
		validStreams := []Stream{}
		_, _ = utils.ArrayContains(catalog.Streams, func(elem *types.ConfiguredStream) bool {
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

			err = elem.SetupState(state, int(batchSize_))
			if err != nil {
				logger.Warnf("failed to set stream[%s] state due to reason: %s", elem.ID(), err)
			}

			selectedStreams = append(selectedStreams, elem.ID())
			validStreams = append(validStreams, elem)
			return false
		})

		logger.Infof("Valid selected streams are %s", strings.Join(selectedStreams, ", "))

		// Driver running on GroupRead
		if connector.ChangeStreamSupported() {
			driver, yes := connector.(BulkDriver)
			if !yes {
				return fmt.Errorf("%s does not implement BulkDriver", connector.Type())
			}

			// Setup Global State from Connector
			if err := driver.SetupGlobalState(state); err != nil {
				return err
			}

			err := driver.RunChangeStream(pool, validStreams...)
			if err != nil {
				return fmt.Errorf("error occurred while reading records: %s", err)
			}
		}

		// Driver running on Stream mode
		for _, stream := range validStreams {
			logger.Infof("Reading stream %s", stream.ID())

			streamStartTime := time.Now()
			err := connector.Read(pool, stream)
			if err != nil {
				return fmt.Errorf("error occurred while reading records: %s", err)
			}

			logger.Infof("Finished reading stream %s[%s] in %s", stream.Name(), stream.Namespace(), time.Since(streamStartTime).String())
		}

		logger.Infof("Total records read: %d", pool.TotalRecords())
		if !state.IsZero() {
			logger.LogState(state)
		}

		return pool.Wait()
	},
}
