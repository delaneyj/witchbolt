package command

import (
	"fmt"

	"github.com/delaneyj/witchbolt"
	"github.com/delaneyj/witchbolt/internal/guts_cli"
)

type CheckCmd struct {
	Path       string `arg:"" help:"Path to witchbolt database file" type:"path"`
	FromPageID uint64 `help:"Check db integrity starting from the given page ID"`
}

func (c *CheckCmd) Run() error {
	if _, err := checkSourceDBPath(c.Path); err != nil {
		return err
	}

	// Open database.
	db, err := witchbolt.Open(c.Path, 0600, &witchbolt.Options{
		ReadOnly:        true,
		PreLoadFreelist: true,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	opts := []witchbolt.CheckOption{witchbolt.WithKVStringer(CmdKvStringer())}
	if c.FromPageID != 0 {
		opts = append(opts, witchbolt.WithPageId(c.FromPageID))
	}
	// Perform consistency check.
	return db.View(func(tx *witchbolt.Tx) error {
		var count int
		for err := range tx.Check(opts...) {
			fmt.Println(err)
			count++
		}

		// Print summary of errors.
		if count > 0 {
			fmt.Printf("%d errors found\n", count)
			return guts_cli.ErrCorrupt
		}

		// Notify user that database is valid.
		fmt.Println("OK")
		return nil
	})
}
