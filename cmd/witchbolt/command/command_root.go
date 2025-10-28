package command

import (
	"github.com/spf13/cobra"
)

const (
	cliName        = "witchbolt"
	cliDescription = "A simple command line tool for inspecting witchbolt databases"
)

func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:     cliName,
		Short:   cliDescription,
		Version: "dev",
	}

	rootCmd.AddCommand(
		newVersionCommand(),
		newSurgeryCommand(),
		newInspectCommand(),
		newCheckCommand(),
		newBucketsCommand(),
		newInfoCommand(),
		newCompactCommand(),
		newStatsCommand(),
		newPagesCommand(),
		newKeysCommand(),
		newDumpCommand(),
		newPageItemCommand(),
		newPageCommand(),
		newBenchCommand(),
		newGetCommand(),
		newStreamCommand(),
	)

	return rootCmd
}
