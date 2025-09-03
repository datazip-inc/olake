package protocol

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/spec"
	"github.com/spf13/cobra"
)

var specCmd = &cobra.Command{
	Use:   "spec",
	Short: "spec command",
	RunE: func(_ *cobra.Command, _ []string) error {
		specPath, err := resolveSpecPath()
		if err != nil {
			return err
		}

		var specData map[string]interface{}
		if err := utils.UnmarshalFile(specPath, &specData, false); err != nil {
			return fmt.Errorf("failed to read spec file %s: %v", specPath, err)
		}

		schemaType := utils.Ternary(destinationType == "not-set", connector.Type(), destinationType).(string)
		uiSchema, err := spec.LoadUISchema(schemaType)
		if err != nil {
			return fmt.Errorf("failed to get ui schema: %v", err)
		}

		specSchema := map[string]interface{}{
			"jsonschema": specData,
			"uischema":   uiSchema,
		}

		logger.Info(specSchema)
		return nil
	},
}

func resolveSpecPath() (string, error) {
	// pwd is olake/drivers/(driver) or olake/destination/(destination)
	pwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	// olakeRoot is olake's root path
	olakeRoot := filepath.Join(pwd, "..", "..")
	specPath := utils.Ternary(destinationType == "not-set", filepath.Join(olakeRoot, "drivers", connector.Type(), "resources/spec.json"), filepath.Join(olakeRoot, "destination", destinationType, "resources/spec.json")).(string)

	return specPath, nil
}
