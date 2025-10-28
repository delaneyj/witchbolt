package command

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/delaneyj/witchbolt"
	"github.com/spf13/cobra"
)

func newInspectCommand() *cobra.Command {
	inspectCmd := &cobra.Command{
		Use:   "inspect <bbolt-file>",
		Short: "inspect the structure of the database",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return inspectFunc(args[0])
		},
	}

	return inspectCmd
}

func inspectFunc(srcDBPath string) error {
	if _, err := checkSourceDBPath(srcDBPath); err != nil {
		return err
	}

	db, err := witchbolt.Open(srcDBPath, 0600, &witchbolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()

	return db.View(func(tx *witchbolt.Tx) error {
		bs := tx.Inspect()
		out, err := json.MarshalIndent(bs, "", "    ")
		if err != nil {
			return err
		}
		fmt.Fprintln(os.Stdout, string(out))
		return nil
	})
}
