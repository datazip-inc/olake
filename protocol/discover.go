package protocol

import "github.com/spf13/cobra"

// discoverCmd represents the discover command which performs discovery on the connector;
// fetches total streams and metadata about streams such as Primary Keys, Schema
var discoverCmd = &cobra.Command{
	Use:   "discover",
	Short: "Colake Discover command",
	Long:  `discover is performed after check command; It collects detail from respective connector about possible Streams and their metadata`,
	Example: `
// Base command:
colake discover --config path/to/config
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// TODO: implement
		return nil
	},
}
