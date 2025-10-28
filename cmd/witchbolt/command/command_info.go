package command

import (
	"fmt"

	"github.com/delaneyj/witchbolt"
)

type InfoCmd struct {
	Path string `arg:"" help:"Path to witchbolt database file" type:"path"`
}

func (c *InfoCmd) Run() error {
	if _, err := checkSourceDBPath(c.Path); err != nil {
		return err
	}

	// Open database.
	db, err := witchbolt.Open(c.Path, 0600, &witchbolt.Options{
		ReadOnly: true,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	// Print basic database info.
	info := db.Info()
	fmt.Printf("Page Size: %d\n", info.PageSize)

	return nil
}
