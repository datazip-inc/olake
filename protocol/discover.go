package protocol

import (
	"errors"
	"fmt"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/utils"
	"github.com/spf13/cobra"
)

// discoverCmd represents the read command
var discoverCmd = &cobra.Command{
	Use:   "discover",
	Short: "discover command",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if config_ == "" {
			return fmt.Errorf("--config not passed")
		} else {
			if err := utils.UnmarshalFile(config_, connector.GetConfigRef()); err != nil {
				return err
			}
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		err := connector.Setup()
		if err != nil {
			return err
		}

		streams, err := connector.Discover()
		if err != nil {
			return err
		}

		if len(streams) == 0 {
			return errors.New("no streams found in connector")
		}

		logger.LogCatalog(streams)
		return nil
	},
}
