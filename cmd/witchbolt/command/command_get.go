package command

import (
	"fmt"
	"os"

	"github.com/delaneyj/witchbolt"
	"github.com/delaneyj/witchbolt/errors"
)

type GetCmd struct {
	Path        string   `arg:"" help:"Path to witchbolt database file" type:"path"`
	BucketKey   []string `arg:"" help:"Bucket path (one or more bucket names) followed by the key to retrieve" placeholder:"bucket [subbucket ...] key"`
	ParseFormat string   `default:"ascii-encoded" help:"Input format: ascii-encoded|hex"`
	Format      string   `default:"auto" help:"Output format: auto|ascii-encoded|hex|bytes"`
}

func (c *GetCmd) Run() error {
	if c.Path == "" {
		return ErrPathRequired
	}

	if len(c.BucketKey) < 2 {
		return fmt.Errorf("bucket is required: %w", ErrBucketRequired)
	}

	buckets := c.BucketKey[:len(c.BucketKey)-1]
	keyArg := c.BucketKey[len(c.BucketKey)-1]

	key, err := parseBytes(keyArg, c.ParseFormat)
	if err != nil {
		return err
	}

	if len(key) == 0 {
		return fmt.Errorf("key is required: %w", errors.ErrKeyRequired)
	}

	// check if the source DB path is valid
	if _, err := checkSourceDBPath(c.Path); err != nil {
		return err
	}

	// open the database
	db, err := witchbolt.Open(c.Path, 0600, &witchbolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()

	// access the database and get the value
	return db.View(func(tx *witchbolt.Tx) error {
		lastBucket, err := findLastBucket(tx, buckets)
		if err != nil {
			return err
		}
		val := lastBucket.Get(key)
		if val == nil {
			return fmt.Errorf("Error %w for key: %q hex: \"%x\"", ErrKeyNotFound, key, string(key))
		}
		return writelnBytes(os.Stdout, val, c.Format)
	})
}
