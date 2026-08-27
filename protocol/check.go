package protocol

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
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
			return errs.Precondition(errs.ConfigInvalid, codeFlagMissing,
				fmt.Errorf("no connector config or destination config provided"))
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
				// NewWriterPool initializes destination resources and runs Check;
				// close immediately since a check has no further work.
				pool, err := destination.NewWriterPool(cmd.Context(), destinationConfig, nil, batchSize)
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

		// A failed connection is a successful check: the verdict is this message, not the exit
		// code. Exiting non-zero fails the caller's activity before it ever parses the message.
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

		// Reported here because check exits zero: RegisterDriver's hook only fires on a returned
		// error. PreRunE failures still reach it, so this cannot double-report.
		ReportFailure(err)
	},
}
