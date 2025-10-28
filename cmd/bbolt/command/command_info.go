package command

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/delaneyj/witchbolt"
)

func newInfoCommand() *cobra.Command {
	infoCmd := &cobra.Command{
		Use:   "info <bbolt-file>",
		Short: "prints basic information about the bbolt database.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return infoFunc(cmd, args[0])
		},
	}

	return infoCmd
}

func infoFunc(cmd *cobra.Command, dbPath string) error {
	if _, err := checkSourceDBPath(dbPath); err != nil {
		return err
	}

	// Open database.
	db, err := witchbolt.Open(dbPath, 0600, &witchbolt.Options{
		ReadOnly: true,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	// Print basic database info.
	info := db.Info()
	fmt.Fprintf(cmd.OutOrStdout(), "Page Size: %d\n", info.PageSize)

	return nil
}
