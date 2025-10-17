package protocol

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

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

type StreamClassification struct {
	SelectedStreams    []string
	CDCStreams         []types.StreamInterface
	IncrementalStreams []types.StreamInterface
	FullLoadStreams    []types.StreamInterface
	NewStreamsState    []*types.StreamState
}

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

func CreateRootCommand(_ bool, driver any) *cobra.Command {
	RootCmd.AddCommand(commands...)
	connector = abstract.NewAbstractDriver(RootCmd.Context(), driver.(abstract.DriverInterface))

	return RootCmd
}

func GetStreamsClassification(catalog *types.Catalog, streams []*types.Stream, state *types.State) (*StreamClassification, error) {
	// stream-specific classifications
	classifications := &StreamClassification{
		SelectedStreams:    []string{},
		CDCStreams:         []types.StreamInterface{},
		IncrementalStreams: []types.StreamInterface{},
		FullLoadStreams:    []types.StreamInterface{},
		NewStreamsState:    []*types.StreamState{},
	}
	// create a map for namespace and streamMetadata
	selectedStreamsMap := make(map[string]types.StreamMetadata)
	for namespace, streamsMetadata := range catalog.SelectedStreams {
		for _, streamMetadata := range streamsMetadata {
			selectedStreamsMap[fmt.Sprintf("%s.%s", namespace, streamMetadata.StreamName)] = streamMetadata
		}
	}

	// Create a map for quick state lookup by stream ID
	stateStreamMap := make(map[string]*types.StreamState)
	for _, stream := range state.Streams {
		stateStreamMap[fmt.Sprintf("%s.%s", stream.Namespace, stream.Stream)] = stream
	}

	_, _ = utils.ArrayContains(catalog.Streams, func(elem *types.ConfiguredStream) bool {
		sMetadata, selected := selectedStreamsMap[elem.ID()]
		// Check if the stream is in the selectedStreamMap
		if !(catalog.SelectedStreams == nil || selected) {
			logger.Debugf("Skipping stream %s.%s; not in selected streams.", elem.Namespace(), elem.Name())
			return false
		}

		if streams != nil {
			source, found := types.StreamsToMap(streams...)[elem.ID()]
			if !found {
				logger.Warnf("Skipping; Configured Stream %s not found in source", elem.ID())
				return false
			}
			elem.StreamMetadata = sMetadata
			err := elem.Validate(source)
			if err != nil {
				logger.Warnf("Skipping; Configured Stream %s found invalid due to reason: %s", elem.ID(), err)
				return false
			}
		}

		classifications.SelectedStreams = append(classifications.SelectedStreams, elem.ID())
		switch elem.Stream.SyncMode {
		case types.CDC, types.STRICTCDC:
			classifications.CDCStreams = append(classifications.CDCStreams, elem)
			streamState, exists := stateStreamMap[elem.ID()]
			if exists {
				classifications.NewStreamsState = append(classifications.NewStreamsState, streamState)
			}
		case types.INCREMENTAL:
			classifications.IncrementalStreams = append(classifications.IncrementalStreams, elem)
			streamState, exists := stateStreamMap[elem.ID()]
			if exists {
				classifications.NewStreamsState = append(classifications.NewStreamsState, streamState)
			}
		default:
			classifications.FullLoadStreams = append(classifications.FullLoadStreams, elem)
		}

		return false
	})
	// Clear previous state streams for non-selected streams.
	// Must not be called during clear destination to retain the global and stream state. (clear dest. -> when streams == nil)
	if streams != nil {
		state.Streams = classifications.NewStreamsState
	}
	if len(classifications.SelectedStreams) == 0 {
		return nil, fmt.Errorf("no valid streams found in catalog")
	}

	logger.Infof("Valid selected streams are %s", strings.Join(classifications.SelectedStreams, ", "))
	return classifications, nil
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
	RootCmd.PersistentFlags().BoolVarP(&noSave, "no-save", "", false, "(Optional) Flag to skip logging artifacts in file")
	RootCmd.PersistentFlags().StringVarP(&encryptionKey, "encryption-key", "", "", "(Optional) Decryption key. Provide the ARN of a KMS key, a UUID, or a custom string based on your encryption configuration.")
	RootCmd.PersistentFlags().StringVarP(&destinationDatabasePrefix, "destination-database-prefix", "", "", "(Optional) Destination database prefix is used as prefix for destination database name")
	RootCmd.PersistentFlags().Int64VarP(&timeout, "timeout", "", -1, "(Optional) Timeout to override default timeouts (in seconds)")
	RootCmd.PersistentFlags().StringVarP(&differencePath, "difference", "", "", "old streams.json file path to compare. Generates a difference_streams.json file.")
	// Disable Cobra CLI's built-in usage and error handling
	RootCmd.SilenceUsage = true
	RootCmd.SilenceErrors = true
	err := RootCmd.Execute()
	if err != nil {
		logger.Fatal(err)
	}
}
