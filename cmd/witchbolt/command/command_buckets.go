package command

import (
	"fmt"

	"github.com/delaneyj/witchbolt"
)

type BucketsCmd struct {
	Path string `arg:"" help:"Path to witchbolt database file" type:"path"`
}

func (c *BucketsCmd) Run() error {
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

	// Print buckets.
	return db.View(func(tx *witchbolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *witchbolt.Bucket) error {
			fmt.Println(string(name))
			return nil
		})
	})
}
