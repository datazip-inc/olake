package protocol

import (
	"fmt"

	"github.com/datazip-inc/olake/logger/console"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	config_    string
	state_     string
	catalog_   string
	batchSize_ uint

	catalog *types.Catalog
	state   *types.State

	isDriver        = false
	driverCommands  = []*cobra.Command{}
	adapterCommands = []*cobra.Command{}

	_driver       Driver
	_adapter      Adapter
	_rawConnector Connector
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "olake",
	Short: "root command",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}

		if ok := utils.IsValidSubcommand(getAvailableCommands(), args[0]); !ok {
			return fmt.Errorf("'%s' is an invalid command. Use 'olake --help' to display usage guide", args[0])
		}

		return nil
	},
}

func CreateRootCommand(forDriver bool, connector any) *cobra.Command {
	if forDriver {
		RootCmd.AddCommand(driverCommands...)
		_driver = connector.(Driver)
		isDriver = true
	} else {
		RootCmd.AddCommand(adapterCommands...)
		_adapter = connector.(Adapter)
	}

	_rawConnector = connector.(Connector)

	return RootCmd
}

func getAvailableCommands() []*cobra.Command {
	if isDriver {
		return driverCommands
	}

	return adapterCommands
}

func init() {
	driverCommands = append(driverCommands, SpecCmd, CheckCmd, DiscoverCmd, ReadCmd)
	adapterCommands = append(adapterCommands, SpecCmd, CheckCmd, DiscoverCmd, WriteCmd)

	RootCmd.PersistentFlags().StringVarP(&config_, "config", "", "", "(Required) Config for connector")
	RootCmd.PersistentFlags().StringVarP(&catalog_, "catalog", "", "", "(Required) Catalog for connector")
	RootCmd.PersistentFlags().StringVarP(&state_, "state", "", "", "(Required) State for connector")
	RootCmd.PersistentFlags().UintVarP(&batchSize_, "batch", "", 10000, "(Optional) Batch size for connector")

	// Disable Cobra CLI's built-in usage and error handling
	RootCmd.SilenceUsage = true
	RootCmd.SilenceErrors = true

	// Disable logging
	logrus.SetOutput(nil)

	console.SetupWriter(RootCmd.OutOrStdout(), RootCmd.ErrOrStderr())
}
