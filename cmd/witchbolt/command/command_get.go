package command

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/delaneyj/witchbolt"
	"github.com/delaneyj/witchbolt/errors"
)

type getOptions struct {
	parseFormat string
	format      string
}

func newGetCommand() *cobra.Command {
	var opts getOptions

	cmd := &cobra.Command{
		Use:   "get PATH [BUCKET..] KEY",
		Short: "get the value of a key from a (sub)bucket in a witchbolt database",
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]
			if path == "" {
				return ErrPathRequired
			}
			buckets := args[1 : len(args)-1]
			keyStr := args[len(args)-1]

			// validate input parameters
			if len(buckets) == 0 {
				return fmt.Errorf("bucket is required: %w", ErrBucketRequired)
			}

			key, err := parseBytes(keyStr, opts.parseFormat)
			if err != nil {
				return err
			}

			if len(key) == 0 {
				return fmt.Errorf("key is required: %w", errors.ErrKeyRequired)
			}

			return getFunc(cmd, path, buckets, key, opts)
		},
	}

	cmd.Flags().StringVar(&opts.parseFormat, "parse-format", "ascii-encoded", "Input format one of: ascii-encoded|hex")
	cmd.Flags().StringVar(&opts.format, "format", "auto", "Output format one of: "+FORMAT_MODES+" (default: auto)")

	return cmd
}

// getFunc opens the given witchbolt db file and retrieves the key value from the bucket path.
func getFunc(cmd *cobra.Command, path string, buckets []string, key []byte, opts getOptions) error {
	// check if the source DB path is valid
	if _, err := checkSourceDBPath(path); err != nil {
		return err
	}

	// open the database
	db, err := witchbolt.Open(path, 0600, &witchbolt.Options{ReadOnly: true})
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
		return writelnBytes(cmd.OutOrStdout(), val, opts.format)
	})
}
