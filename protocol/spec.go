package protocol

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/spf13/cobra"
)

var specCmd = &cobra.Command{
	Use:   "spec",
	Short: "spec command",
	RunE: func(_ *cobra.Command, _ []string) error {
		specPath, err := resolveSpecPath(destinationConfigPath)
		if err != nil {
			return err
		}

		var specData map[string]interface{}
		if err := utils.UnmarshalFile(specPath, &specData, false); err != nil {
			return fmt.Errorf("failed to read spec file %s: %v", specPath, err)
		}

		schemaType := utils.Ternary(destinationConfigPath == "not-set", connector.Type(), destinationConfigPath).(string)
		uiSchema, err := constants.LoadUISchema(schemaType)
		if err != nil {
			return fmt.Errorf("failed to get ui schema: %v", err)
		}

		specSchema := map[string]interface{}{
			"spec":     specData,
			"uischema": uiSchema,
		}

		logger.Info(specSchema)
		return nil
	},
}

func resolveSpecPath(destinationConfigPath string) (string, error) {
	pwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	olakeRoot := filepath.Join(pwd, "..", "..")
	specPath := utils.Ternary(destinationConfigPath == "not-set", filepath.Join(olakeRoot, "drivers", connector.Type(), "spec.json"), filepath.Join(olakeRoot, "destination", destinationConfigPath, "spec.json")).(string)

	// Check if the spec file exists
	if _, err := os.Stat(specPath); os.IsNotExist(err) {
		return "", fmt.Errorf("spec file not found at %s", specPath)
	}

	return specPath, nil
}
