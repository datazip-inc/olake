package protocol

import "github.com/spf13/cobra"

// specCmd represents the discover command which performs discovery on the connector;
// returns renderable JSONSchema structure (https://json-schema.org/) for respective Connector's Config struct as well as Destination Integrations available which helps UI libraries
// such as https://github.com/rjsf-team/react-jsonschema-form to render Config details easily
var specCmd = &cobra.Command{
	Use:   "spec",
	Short: "Colake Spec command",
	Long:  `Spec returns a renderable JSON Schema for connector config which helps user identify what config is accepted by a connector.`,
	Example: `
// Base command:
colake spec
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// TODO: implement
		return nil
	},
}
