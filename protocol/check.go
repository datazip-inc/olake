package protocol

import (
	"fmt"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/spf13/cobra"
)

// checkCmd represents the check command
var checkCmd = &cobra.Command{
	Use:   "check",
	Short: "check command",
	PreRunE: func(_ *cobra.Command, _ []string) error {
		// If connector is not set, we are checking the destination
		if destinationConfigPath == "not-set" && configPath == "not-set" {
			return fmt.Errorf("no connector config or destination config provided")
		}

		// check for destination config
		if destinationConfigPath != "not-set" {
			destinationConfig = &types.WriterConfig{}
			return utils.UnmarshalFile(destinationConfigPath, destinationConfig, true)
		}

		// check for source config
		if configPath != "not-set" {
			return utils.UnmarshalFile(configPath, connector.GetConfigRef(), true)
		}

		return nil
	},
	Run: func(cmd *cobra.Command, _ []string) {
		err := func() error {
			// If connector is not set, we are checking the destination
			if destinationConfigPath != "not-set" {
				_, err := destination.NewWriterPool(cmd.Context(), destinationConfig, nil, nil, batchSize)
				return err
			}

			if configPath != "not-set" {
				return connector.Setup(cmd.Context())
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
