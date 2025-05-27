package protocol

import (
	"fmt"

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
	Run: func(cmd *cobra.Command, _ []string) {
		err := func() error {
			err := connector.Check(cmd.Context())
			if err != nil {
				return err
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
