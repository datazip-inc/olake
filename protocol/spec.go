package protocol

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/jsonschema"
	"github.com/datazip-inc/olake/utils/logger"

	"github.com/spf13/cobra"
)

var specCmd = &cobra.Command{
	Use:   "spec",
	Short: "spec command",
	RunE: func(_ *cobra.Command, _ []string) error {
		config, fileName, err := resolveSpecConfig(destinationConfigPath)
		if err != nil {
			return err
		}

		// check if spec already exists
		var specData map[string]interface{}
		if err := utils.UnmarshalFile(fmt.Sprintf("%s.json", fileName), &specData, false); err == nil {
			logger.Info(specData)
			return nil
		}

		// generate jsonschema
		schemaVal, err := jsonschema.Reflect(config)
		if err != nil {
			return fmt.Errorf("failed to reflect config: %v", err)
		}

		// load uischema
		schemaType := utils.Ternary(destinationConfigPath == "not-set", connector.Type(), destinationConfigPath).(string)
		uiSchema, err := jsonschema.LoadUISchema(schemaType)
		if err != nil {
			return fmt.Errorf("failed to get ui schema: %v", err)
		}

		specSchema := map[string]interface{}{
			"spec":     schemaVal,
			"uischema": uiSchema,
		}

		if err := logger.FileLogger(specSchema, fileName, ".json"); err != nil {
			return fmt.Errorf("failed to log spec: %v", err)
		}

		logger.Info(specSchema)
		return nil
	},
}

// resolveSpecConfig returns the config and file name, using destinationConfigPath if provided
// destinationConfigPath is the destination type (iceberg, parquet)
func resolveSpecConfig(destinationConfigPath string) (any, string, error) {
	var config any
	var fileName string
	if destinationConfigPath == "not-set" {
		config = connector.Spec()
		fileName = "spec"
	} else {
		writerConfig := types.WriterConfig{
			Type: types.AdapterType(strings.ToUpper(destinationConfigPath)),
		}

		newFunc, found := destination.RegisteredWriters[writerConfig.Type]
		if !found {
			return nil, "", fmt.Errorf("invalid destination type has been passed [%s]", writerConfig.Type)
		}

		writer := newFunc()
		config = writer.Spec()
		fileName = fmt.Sprintf("%s-spec", strings.ToLower(destinationConfigPath))
	}

	return config, fileName, nil
}
