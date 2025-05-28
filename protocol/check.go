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
		// If connector is not set, we are checking the destination
		if destinationConfigPath != "not-set" {
			destinationConfig = &types.WriterConfig{}
			if err := utils.UnmarshalFile(destinationConfigPath, destinationConfig); err != nil {
				return err
			}
		} else if configPath != "not-set" {
			if err := utils.UnmarshalFile(configPath, connector.GetConfigRef()); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("no connector config or destination config provided")
		}

		return nil
	},
	Run: func(cmd *cobra.Command, _ []string) {
		err := func() error {
			// If connector is not set, we are checking the destination
			if destinationConfigPath != "not-set" {
				_, err := NewWriter(cmd.Context(), destinationConfig)
				return err
			}

			if configPath != "not-set" {
				return connector.Check()
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
