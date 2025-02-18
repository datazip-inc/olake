package protocol

import (
	"fmt"
	"os"
	"path"

	"github.com/goccy/go-json"

	"github.com/datazip-inc/olake/jsonschema"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"

	"github.com/spf13/cobra"
)

var (
	generate bool
	airbyte  bool
)

// specCmd represents the read command
var specCmd = &cobra.Command{
	Use:   "spec",
	Short: "spec command",
	RunE: func(_ *cobra.Command, _ []string) error {
		wd, _ := os.Getwd()
		specfile := path.Join(wd, "generated.json")
		spec := make(map[string]interface{})
		if generate {
			logger.Info("Generating Spec")

			config := connector.Spec()
			schema, err := jsonschema.Reflect(config)
			if err != nil {
				return err
			}

			err = utils.Unmarshal(schema, &spec)
			if err != nil {
				return fmt.Errorf("failed to generate json schema for config: %s", err)
			}

			file, err := os.OpenFile(specfile, os.O_CREATE|os.O_RDWR, os.ModePerm)
			if err != nil {
				return err
			}
			defer file.Close()

			bytes, err := json.MarshalIndent(spec, "", "\t")
			if err != nil {
				return err
			}

			_, err = file.Write(bytes)
			if err != nil {
				return err
			}
		} else {
			logger.Info("Reading cached Spec")

			err := utils.UnmarshalFile(specfile, &spec)
			if err != nil {
				return err
			}
		}

		if airbyte {
			spec = map[string]any{
				"connectionSpecification": spec,
			}
		}

		// log spec
		message := types.Message{
			Spec: spec,
			Type: types.SpecMessage,
		}
		logger.Info(message)
		err := logger.FileLogger(message.Spec, "config", ".json")
		if err != nil {
			logger.Fatalf("failed to create spec file: %s", err)
		}

		return nil
	},
}

func init() {
	// TODO: Set false
	RootCmd.PersistentFlags().BoolVarP(&generate, "generate", "", false, "(Optional) Generate Config")
	RootCmd.PersistentFlags().BoolVarP(&airbyte, "airbyte", "", true, "(Optional) Print Config wrapped like airbyte")
}
