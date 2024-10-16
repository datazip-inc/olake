package protocol

import (
	"github.com/spf13/cobra"
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "colake",
	Short: "Colake Command Line tool",
	Long:  `OpenSource Data-Ingestion connector ecosystem`,
	Example: `
// Base command:
colake

// Display help about command/subcommand:
colake --help
colake sync --help

// For viewing verbose output:
colake -v [or] --verbose
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}

		return nil
	},
}

func init() {
	RootCmd.SilenceUsage = true
	RootCmd.AddCommand(specCmd, checkCmd, discoverCmd, syncCmd)
}
