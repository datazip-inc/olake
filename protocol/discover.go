package protocol

import (
	"errors"
	"fmt"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/spf13/cobra"
)

// discoverCmd represents the read command
var discoverCmd = &cobra.Command{
	Use:   "discover",
	Short: "discover command",
	PreRunE: func(_ *cobra.Command, _ []string) error {
		if configPath == "" {
			return fmt.Errorf("--config not passed")
		}

		if err := utils.UnmarshalFile(configPath, connector.GetConfigRef()); err != nil {
			return err
		}

		return nil
	},
	RunE: func(_ *cobra.Command, _ []string) error {
		err := connector.Setup()
		if err != nil {
			return err
		}
		streams, err := connector.Discover(true)
		if err != nil {
			return err
		}

		if len(streams) == 0 {
			return errors.New("no streams found in connector")
		}

		types.LogCatalog(streams)
		return nil
	},
}
