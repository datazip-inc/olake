package protocol

import (
	"fmt"
	"path/filepath"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/telemetry"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	configPath            string
	destinationConfigPath string
	statePath             string
	streamsPath           string
	syncID                string
	batchSize             int64
	noSave                bool
	clearDestinationFlag  bool
	encryptionKey         string
	catalog               *types.Catalog
	state                 *types.State
	timeout               int64 // timeout in seconds
	destinationConfig     *types.WriterConfig

	commands  = []*cobra.Command{}
	connector *abstract.AbstractDriver
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "olake",
	Short: "root command",
	RunE: func(cmd *cobra.Command, args []string) error {

		// set global variables
		if !noSave {
			viper.Set(constants.ConfigFolder, utils.Ternary(configPath == "not-set", filepath.Dir(destinationConfigPath), filepath.Dir(configPath)))
		}

		if encryptionKey != "" {
			viper.Set(constants.EncryptionKey, encryptionKey)
		}
		// logger uses CONFIG_FOLDER
		logger.Init()
		telemetry.Init()

		if len(args) == 0 {
			return cmd.Help()
		}

		if ok := utils.IsValidSubcommand(commands, args[0]); !ok {
			return fmt.Errorf("'%s' is an invalid command. Use 'olake --help' to display usage guide", args[0])
		}

		return nil
	},
}

func CreateRootCommand(_ bool, driver any) *cobra.Command {
	RootCmd.AddCommand(commands...)
	connector = abstract.NewAbstractDriver(RootCmd.Context(), driver.(abstract.DriverInterface))

	return RootCmd
}

func init() {
	// TODO: replace --catalog flag with --streams
	commands = append(commands, specCmd, checkCmd, discoverCmd, syncCmd)
	RootCmd.PersistentFlags().StringVarP(&configPath, "config", "", "not-set", "(Required) Config for connector")
	RootCmd.PersistentFlags().StringVarP(&destinationConfigPath, "destination", "", "not-set", "(Required) Destination config for connector")
	RootCmd.PersistentFlags().StringVarP(&streamsPath, "catalog", "", "", "Path to the streams file for the connector")
	RootCmd.PersistentFlags().StringVarP(&streamsPath, "streams", "", "", "Path to the streams file for the connector")
	RootCmd.PersistentFlags().StringVarP(&statePath, "state", "", "", "(Required) State for connector")
	RootCmd.PersistentFlags().Int64VarP(&batchSize, "batch", "", 10000, "(Optional) Batch size for connector")
	RootCmd.PersistentFlags().BoolVarP(&noSave, "no-save", "", false, "(Optional) Flag to skip logging artifacts in file")
	RootCmd.PersistentFlags().BoolVarP(&clearDestinationFlag, "clear-destination", "", false, "(Optional) Flag to clear destination and reset sync state for selected streams to force full refresh. Note: Destination is automatically cleared for full refresh streams regardless of this flag.")
	RootCmd.PersistentFlags().StringVarP(&encryptionKey, "encryption-key", "", "", "(Optional) Decryption key. Provide the ARN of a KMS key, a UUID, or a custom string based on your encryption configuration.")
	RootCmd.PersistentFlags().Int64VarP(&timeout, "timeout", "", -1, "(Optional) Timeout to override default timeouts (in seconds)")
	// Disable Cobra CLI's built-in usage and error handling
	RootCmd.SilenceUsage = true
	RootCmd.SilenceErrors = true
	err := RootCmd.Execute()
	if err != nil {
		logger.Fatal(err)
	}
}
