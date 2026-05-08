package protocol

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

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
	configPath                string
	destinationConfigPath     string
	statePath                 string
	streamsPath               string
	destinationDatabasePrefix string
	syncID                    string
	batchSize                 int64
	maxDiscoverThreads        int
	noSave                    bool
	encryptionKey             string
	destinationType           string
	catalog                   *types.Catalog
	state                     *types.State
	timeout                   int64 // timeout in seconds
	destinationConfig         *types.WriterConfig
	differencePath            string

	commands  = []*cobra.Command{}
	connector *abstract.AbstractDriver
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "olake",
	Short: "root command",
	RunE: func(cmd *cobra.Command, args []string) error {

		// set global variables

		viper.SetDefault(constants.ConfigFolder, os.TempDir())
		viper.SetDefault(constants.StatePath, filepath.Join(os.TempDir(), "state.json"))
		viper.SetDefault(constants.StreamsPath, filepath.Join(os.TempDir(), "streams.json"))
		viper.SetDefault(constants.DifferencePath, filepath.Join(os.TempDir(), "difference_streams.json"))
		if !noSave {
			configFolder := utils.Ternary(configPath == "not-set", filepath.Dir(destinationConfigPath), filepath.Dir(configPath)).(string)
			streamsPathEnv := utils.Ternary(streamsPath == "", filepath.Join(configFolder, "streams.json"), streamsPath).(string)
			differencePathEnv := utils.Ternary(streamsPath != "", filepath.Join(filepath.Dir(streamsPath), "difference_streams.json"), filepath.Join(configFolder, "difference_streams.json")).(string)
			statePathEnv := utils.Ternary(statePath == "", filepath.Join(configFolder, "state.json"), statePath).(string)
			viper.Set(constants.ConfigFolder, configFolder)
			viper.Set(constants.StatePath, statePathEnv)
			viper.Set(constants.StreamsPath, streamsPathEnv)
			viper.Set(constants.DifferencePath, differencePathEnv)
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

// CreateRootCommand wires the cobra root for the given driver. It mutates
// package-level state (RootCmd, connector) and installs a process-wide signal
// handler, so it must be called at most once per process — the existing
// connector.RegisterDriver entry point already enforces this.
func CreateRootCommand(_ bool, driver any) *cobra.Command {
	RootCmd.AddCommand(commands...)

	// Wire SIGINT/SIGTERM into the root context so CDC, backfill and
	// destination-writer paths reach their existing ctx.Done() branches on
	// pod eviction, docker stop, or Ctrl-C, instead of being killed mid-read.
	ctx := signalAwareRootContext(RootCmd.Context())
	RootCmd.SetContext(ctx)

	connector = abstract.NewAbstractDriver(ctx, driver.(abstract.DriverInterface))

	return RootCmd
}

func signalAwareRootContext(parent context.Context) context.Context {
	ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	// signal.NotifyContext keeps the signal handler installed until stop() is
	// called. Releasing it after the first cancellation lets a subsequent
	// SIGINT/SIGTERM fall through to the Go runtime default (terminate), which
	// is the behavior an operator hitting Ctrl-C twice expects.
	go func() {
		<-ctx.Done()
		stop()
	}()

	return ctx
}

func init() {
	// TODO: replace --catalog flag with --streams
	commands = append(commands, specCmd, checkCmd, discoverCmd, syncCmd, clearCmd)
	RootCmd.PersistentFlags().StringVarP(&configPath, "config", "", "not-set", "(Required) Config for connector")
	RootCmd.PersistentFlags().StringVarP(&destinationConfigPath, "destination", "", "not-set", "(Required) Destination config for connector")
	RootCmd.PersistentFlags().StringVarP(&destinationType, "destination-type", "", "not-set", "Destination type for spec")
	RootCmd.PersistentFlags().StringVarP(&streamsPath, "catalog", "", "", "Path to the streams file for the connector")
	RootCmd.PersistentFlags().StringVarP(&streamsPath, "streams", "", "", "Path to the streams file for the connector")
	RootCmd.PersistentFlags().StringVarP(&statePath, "state", "", "", "(Required) State for connector")
	RootCmd.PersistentFlags().Int64VarP(&batchSize, "destination-buffer-size", "", 10000, "(Optional) Batch size for destination")
	RootCmd.PersistentFlags().IntVarP(&maxDiscoverThreads, "max-discover-threads", "", 50, "(Optional) Max number of parallel threads for discovery of table in database")
	RootCmd.PersistentFlags().BoolVarP(&noSave, "no-save", "", false, "(Optional) Flag to skip logging artifacts in file")
	RootCmd.PersistentFlags().StringVarP(&encryptionKey, "encryption-key", "", "", "(Optional) Decryption key. Provide the ARN of a KMS key, a UUID, or a custom string based on your encryption configuration.")
	RootCmd.PersistentFlags().StringVarP(&destinationDatabasePrefix, "destination-database-prefix", "", "", "(Optional) Destination database prefix is used as prefix for destination database name")
	RootCmd.PersistentFlags().Int64VarP(&timeout, "timeout", "", -1, "(Optional) Timeout to override default timeouts (in seconds)")
	RootCmd.PersistentFlags().StringVarP(&differencePath, "difference", "", "", "new streams.json file path to be compared. Generates a difference_streams.json file.")
	// Disable Cobra CLI's built-in usage and error handling
	RootCmd.SilenceUsage = true
	RootCmd.SilenceErrors = true
	err := RootCmd.Execute()
	if err != nil {
		logger.Fatal(err)
	}
}
