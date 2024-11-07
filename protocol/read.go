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
		} else {
			if err := utils.ValidateFile(config_, _rawConnector.GetConfigRef()); err != nil {
				return err
			}
		}

		if catalog_ != "" {
			catalog = &types.Catalog{}
			if err := utils.ValidateFile(catalog_, catalog); err != nil {
				return err
			}
		}

		if state_ != "" {
			state = &types.State{}
			if err := utils.ValidateFile(state_, state); err != nil {
				return err
			}
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		// Driver Setup
		err := _driver.Setup()
		if err != nil {
			return err
		}

		// Setup state defaults
		if state == nil {
			state = &types.State{
				Type: types.StreamType,
			}
		}
		state.Mutex = &sync.Mutex{}

		// Setting Record iteration
		recordStream := make(chan types.Record, 2*batchSize_)
		numRecords := int64(0)
		batch := uint(0)
		recordIterationWait := sync.WaitGroup{}

		recordIterationWait.Add(1)
		go func() {
			defer recordIterationWait.Done()
			defer close(recordStream)

			for message := range recordStream {
				// close the iteration
				if message.Close {
					break
				}

				logger.LogRecord(message)
				numRecords++
				batch++

				// log state after a batch
				if batch >= batchSize_ {
					if !state.IsZero() {
						logger.LogState(state)
					}
					// reset batch
					batch = 0
				}
			}
		}()

		// Get Source Streams
		streams, err := _driver.Discover()
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
		if _driver.BulkRead() {
			driver, yes := _driver.(BulkDriver)
			if !yes {
				return fmt.Errorf("%s does not implement BulkDriver", _driver.Type())
			}

			// Setup Global State from Connector
			if err := driver.SetupGlobalState(state); err != nil {
				return err
			}

			err := driver.GroupRead(recordStream, validStreams...)
			if err != nil {
				return fmt.Errorf("error occurred while reading records: %s", err)
			}
		} else {
			// Driver running on Stream mode
			for _, stream := range validStreams {
				logger.Infof("Reading stream %s", stream.ID())

				streamStartTime := time.Now()
				err := _driver.Read(stream, recordStream)
				if err != nil {
					return fmt.Errorf("error occurred while reading records: %s", err)
				}

				logger.Infof("Finished reading stream %s[%s] in %s", stream.Name(), stream.Namespace(), time.Since(streamStartTime).String())
			}
		}

		// stop record iteration
		recordStream <- types.Record{
			Close: true,
		}
		recordIterationWait.Wait()

		logger.Infof("Total records read: %d", numRecords)
		if !state.IsZero() {
			logger.LogState(state)
		}

		return nil
	},
}
