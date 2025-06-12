package protocol

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/jsonschema"
	"github.com/datazip-inc/olake/utils/logger"

	"github.com/spf13/cobra"
)

var specCmd = &cobra.Command{
	Use:   "spec",
	Short: "spec command",
	RunE: func(_ *cobra.Command, _ []string) error {
		var config any
		if destinationConfigPath == "not-set" {
			config = connector.Spec()
		} else {
			writerConfig := types.WriterConfig{
				Type: types.AdapterType(strings.ToUpper(destinationConfigPath)),
			}

			newFunc, found := destination.RegisteredWriters[writerConfig.Type]
			if !found {
				return fmt.Errorf("invalid destination type has been passed [%s]", writerConfig.Type)
			}

			writer := newFunc()
			config = writer.Spec()
		}

		schemaVal, err := jsonschema.Reflect(config)
		if err != nil {
			return fmt.Errorf("failed to reflect config: %v", err)
		}

		specSchema := map[string]interface{}{
			"spec": schemaVal,
		}

		logger.FileLogger(specSchema, "spec", ".json")

		return nil
	},
}
