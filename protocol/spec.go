package protocol

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/spf13/cobra"
)

var specCmd = &cobra.Command{
	Use:   "spec",
	Short: "spec command",
	RunE: func(_ *cobra.Command, _ []string) error {
		resourcesDir, err := resolveResourcesDir()
		if err != nil {
			return err
		}

		specPath := filepath.Join(resourcesDir, "spec.json")
		var specData map[string]interface{}
		if err := utils.UnmarshalFile(specPath, &specData, false); err != nil {
			return fmt.Errorf("failed to read spec file %s: %v", specPath, err)
		}

		uiSchemaPath := filepath.Join(resourcesDir, "uischema.json")
		uiSchema, err := os.ReadFile(uiSchemaPath)
		if err != nil {
			return fmt.Errorf("failed to read ui schema file %s: %v", uiSchemaPath, err)
		}

		specSchema := map[string]interface{}{
			"jsonschema": specData,
			"uischema":   strings.TrimSpace(string(uiSchema)),
		}

		logger.Info(specSchema)
		return nil
	},
}

// resolveResourcesDir locates the connector's resources directory, which holds its
// jsonschema (spec.json) and the UI layout of that schema (uischema.json).
func resolveResourcesDir() (string, error) {
	// pwd is olake/drivers/(driver) or olake/destination/(destination)
	pwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	// olakeRoot is olake's root path
	olakeRoot := filepath.Join(pwd, "..", "..")

	return utils.Ternary(destinationType == "not-set",
		filepath.Join(olakeRoot, "drivers", connector.Type(), "resources"),
		filepath.Join(olakeRoot, "destination", destinationType, "resources"),
	).(string), nil
}
