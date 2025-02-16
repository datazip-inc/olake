package protocol

import (
	"fmt"

	"github.com/goccy/go-json"

	"github.com/datazip-inc/olake/logger"

	"github.com/spf13/cobra"
)

// specCmd represents the read command
var specCmd = &cobra.Command{
	Use:   "spec",
	Short: "spec command",
	RunE: func(_ *cobra.Command, _ []string) error {
		spec := make(map[string]interface{})
		Config := connector.Spec()

		data, err := json.Marshal(Config)
		if err != nil {
			return fmt.Errorf("failed to marshal emptyConfig: %v", err)
		}
		err = json.Unmarshal(data, &spec)
		if err != nil {
			return fmt.Errorf("failed to unmarshal into map: %v", err)
		}

		if airbyte {
			spec = map[string]any{
				"connectionSpecification": spec,
			}
		}
		logger.LogSpec(spec)

		return nil
	},
}
