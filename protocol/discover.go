package protocol

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/datazip-inc/olake/logger"
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
		if !noSave {
			folder := filepath.Dir(configPath)
			configFolder = &folder
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

		logger.LogCatalog(streams, configFolder)
		return nil
	},
}
