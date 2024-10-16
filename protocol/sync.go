package protocol

import "github.com/spf13/cobra"

// syncCmd represents the discover command which performs discovery on the connector;
var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Colake Sync command",
	Long:  `Sync command initiates source fetchers and destination writes and starts running sync`,
	Example: `
// Base command:
colake check --config path/to/config --destination path/to/destination/config --catalog path/to/catalog

// With State:
colake check --config path/to/config --destination path/to/destination/config --catalog path/to/catalog --state /path/to/state

`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// TODO: implement
		return nil
	},
}
