package protocol

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/spf13/cobra"
)

var clearCmd = &cobra.Command{
	Use:   "clear-destination",
	Short: "Olake clear command to clear destination data and state for selected streams",
	PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
		if destinationConfigPath == "" {
			return fmt.Errorf("--destination not passed")
		} else if streamsPath == "" {
			return fmt.Errorf("--streams not passed")
		}

		destinationConfig = &types.WriterConfig{}
		if err := utils.UnmarshalFile(destinationConfigPath, destinationConfig, true); err != nil {
			return err
		}

		catalog = &types.Catalog{}
		if err := utils.UnmarshalFile(streamsPath, catalog, false); err != nil {
			return err
		}

		// Initialize state - version will be set only for new state files
		state = &types.State{
			Type:    types.StreamType,
			Version: 0,
		}
		if statePath != "" {
			if err := utils.UnmarshalFile(statePath, state, false); err != nil {
				if strings.Contains(err.Error(), "does not exist") {
					state.Version = constants.StateVersion
				} else {
					return err
				}
			}
		} else {
			state.Version = constants.StateVersion
		}
		types.SetStateVersion(state.Version)
		return nil
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		selectedStreamsMetadata, err := classifyStreams(catalog, nil, state)
		if err != nil {
			return fmt.Errorf("failed to get selected streams for clearing: %w", err)
		}
		dropStreams := []types.StreamInterface{}
		dropStreams = append(dropStreams, append(append(selectedStreamsMetadata.IncrementalStreams, selectedStreamsMetadata.FullLoadStreams...), selectedStreamsMetadata.CDCStreams...)...)
		if len(dropStreams) == 0 {
			logger.Infof("No streams selected for clearing")
			return nil
		}

		connector.SetupState(state)
		// clear state for selected streams
		newState, err := connector.ClearState(dropStreams)
		if err != nil {
			return fmt.Errorf("error clearing state: %w", err)
		}
		logger.Infof("State for selected streams cleared successfully.")
		// Setup new state after clear for connector
		connector.SetupState(newState)

		// drop/clear streams from destination
		cerr := destination.ClearDestination(cmd.Context(), destinationConfig, dropStreams)
		if cerr != nil {
			return fmt.Errorf("failed to clear destination: %s", cerr)
		}
		logger.Infof("Successfully cleared destination data for selected streams.")
		// save new state in state file
		newState.LogState()
		stateBytes, _ := newState.MarshalJSON()
		logger.Infof("New saved state: %s", stateBytes)
		return nil
	},
}
