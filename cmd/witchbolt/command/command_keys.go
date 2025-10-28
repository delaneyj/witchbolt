package command

import (
	"os"

	"github.com/delaneyj/witchbolt"
)

type KeysCmd struct {
	Path    string   `arg:"" help:"Path to witchbolt database file" type:"path"`
	Buckets []string `arg:"" help:"Bucket path (one or more bucket names)"`
	Format  string   `short:"f" default:"auto" help:"Output format: auto|ascii-encoded|hex|bytes"`
}

func (c *KeysCmd) Run() error {
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

	// Print keys.
	return db.View(func(tx *witchbolt.Tx) error {
		// Find bucket.
		lastBucket, err := findLastBucket(tx, c.Buckets)
		if err != nil {
			return err
		}

		// Iterate over each key.
		return lastBucket.ForEach(func(key, _ []byte) error {
			return writelnBytes(os.Stdout, key, c.Format)
		})
	})
}
