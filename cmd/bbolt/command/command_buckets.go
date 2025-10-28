package command

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/delaneyj/witchbolt"
)

func newBucketsCommand() *cobra.Command {
	bucketsCmd := &cobra.Command{
		Use:   "buckets <bbolt-file>",
		Short: "print a list of buckets in bbolt database",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return bucketsFunc(cmd, args[0])
		},
	}

	return bucketsCmd
}

func bucketsFunc(cmd *cobra.Command, dbPath string) error {
	if _, err := checkSourceDBPath(dbPath); err != nil {
		return err
	}

	// Open database.
	db, err := witchbolt.Open(dbPath, 0600, &witchbolt.Options{
		ReadOnly:        true,
		PreLoadFreelist: true,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	// Print buckets.
	return db.View(func(tx *witchbolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *witchbolt.Bucket) error {
			fmt.Fprintln(cmd.OutOrStdout(), string(name))
			return nil
		})
	})
}
