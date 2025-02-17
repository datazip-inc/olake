package protocol

import (
	"fmt"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/spf13/cobra"
)

// checkCmd represents the check command
var checkCmd = &cobra.Command{
	Use:   "check",
	Short: "check command",
	PreRunE: func(_ *cobra.Command, _ []string) error {
		if configPath == "" {
			return fmt.Errorf("--config not passed")
		}

		if err := utils.UnmarshalFile(configPath, connector.GetConfigRef()); err != nil {
			return err
		}

		if catalogPath != "" {
			catalog = &types.Catalog{}
			if err := utils.UnmarshalFile(catalogPath, &catalog); err != nil {
				return err
			}
		}

		return nil
	},
	Run: func(_ *cobra.Command, _ []string) {
		err := func() error {
			// Catalog has been passed setup and is driver; Connector should be setup
			if catalog != nil {
				// Get Source Streams
				streams, err := connector.Discover(false)
				if err != nil {
					return err
				}

				streamsMap := types.StreamsToMap(streams...)

				// Validating Streams
				invalidStreams := []string{}
				missingStreams := []string{}
				_, _ = utils.ArrayContains(catalog.Streams, func(stream *types.ConfiguredStream) bool {
					source, found := streamsMap[stream.ID()]
					if !found {
						missingStreams = append(missingStreams, stream.ID())
						return false
					}

					err := stream.Validate(source)
					if err != nil {
						logger.Error(err)
						invalidStreams = append(invalidStreams, stream.ID())
					}

					return false
				})

				if len(invalidStreams) > 0 && len(missingStreams) > 0 {
					return fmt.Errorf("found missing streams: %v and invalid streams: %v", missingStreams, invalidStreams)
				} else if len(invalidStreams) > 0 {
					return fmt.Errorf("found invalid streams: %v", invalidStreams)
				} else if len(missingStreams) > 0 {
					return fmt.Errorf("found missing streams: %v", missingStreams)
				}
			} else {
				// Only perform checks
				err := connector.Check()
				if err != nil {
					return err
				}
			}

			return nil
		}()

		// log success
		message := types.Message{
			Type: types.ConnectionStatusMessage,
			ConnectionStatus: &types.StatusRow{
				Status: types.ConnectionSucceed,
			},
		}
		if err != nil {
			message.ConnectionStatus.Message = err.Error()
			message.ConnectionStatus.Status = types.ConnectionFailed
		}
		logger.Info(message)
	},
}
