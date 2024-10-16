package protocol

import "github.com/spf13/cobra"

// checkCmd represents the check command which performs connector based checks on the provided Source config and destination config;
// also validates optionally passed Catalog or State
var checkCmd = &cobra.Command{
	Use:   "check",
	Short: "Colake Check command",
	Long:  `check performs connector based checking and confirm if Config, Catalog, and State passed are valid`,
	Example: `
// Base command:
colake check --config path/to/config --destination path/to/destination/config

// Validate catalog:
colake check --config path/to/config --destination path/to/destination/config --catalog path/to/catalog

// Validate state:
colake check --config path/to/config --destination path/to/destination/config --catalog path/to/catalog --state /path/to/state
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// TODO: implement
		return nil
	},
}
