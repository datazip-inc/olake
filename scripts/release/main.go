package main

import (
	"os"

	"github.com/datazip-inc/olake/logger"
	"github.com/spf13/cobra"
)

var releaserCMD = &cobra.Command{
	Use: "",
	RunE: func(_ *cobra.Command, _ []string) error {
		return nil
	},
}

func main() {
	err := releaserCMD.Execute()
	if err != nil {
		logger.Error(err)
		os.Exit(1)
	}
}
