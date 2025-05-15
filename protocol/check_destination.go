package protocol

import (
	"fmt"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/spf13/cobra"
)

// checkDestinationCmd represents the check-destination command
var checkDestinationCmd = &cobra.Command{
	Use:   "check-destination",
	Short: "check-destination command",
	PreRunE: func(_ *cobra.Command, _ []string) error {
		if destinationConfigPath == "" {
			return fmt.Errorf("--destination not passed")
		}

		destinationConfig = &types.WriterConfig{}
		if err := utils.UnmarshalFile(destinationConfigPath, destinationConfig); err != nil {
			return err
		}

		return nil
	},
	Run: func(cmd *cobra.Command, _ []string) {
		err := func() error {

			_, err := NewWriter(cmd.Context(), destinationConfig)
			if err != nil {
				return err
			}

			return nil
		}()

		// log success or failure
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
