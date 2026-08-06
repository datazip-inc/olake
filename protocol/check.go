package protocol

import (
	"context"
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
	RunE: func(cmd *cobra.Command, _ []string) error {
		err := func() error {
			// If connector is not set, we are checking the destination
			if destinationConfigPath != "not-set" {
				// NewWriterPool initializes destination resources and runs Check;
				// close immediately since a check has no further work.
				pool, err := destination.NewWriterPool(cmd.Context(), destinationConfig, types.DeleteModeEquality, nil, batchSize)
				if err != nil {
					return err
				}
				pool.Shutdown(context.Background())
				return nil
			}

			if configPath != "not-set" {
				return connector.Setup(cmd.Context())
			}

			return nil
		}()

		// Report the outcome as a connection-status message, then surface any failure through
		// the exit code: returning a non-nil error makes RootCmd.Execute() call logger.Fatal,
		// so `check` exits non-zero on a failed connection instead of always exiting 0.
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
		return err
	},
}
