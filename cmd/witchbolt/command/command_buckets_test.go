package command_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/delaneyj/witchbolt"
	"github.com/delaneyj/witchbolt/internal/btesting"
)

// Ensure the "buckets" command can print a list of buckets.
func TestBucketsCommand_Run(t *testing.T) {

	testCases := []struct {
		name      string
		args      []string
		expErr    error
		expOutput string
	}{
		{
			name:      "buckets all buckets in witchbolt database",
			args:      []string{"buckets", "path"},
			expErr:    nil,
			expOutput: "bar\nbaz\nfoo\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			t.Log("Creating sample DB")
			db := btesting.MustCreateDB(t)
			if err := db.Update(func(tx *witchbolt.Tx) error {
				for _, name := range []string{"foo", "bar", "baz"} {
					_, err := tx.CreateBucket([]byte(name))
					if err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			db.Close()
			defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

			t.Log("Running buckets cmd")
			args := append([]string{}, tc.args...)
			args[1] = db.Path()
			res := runCLI(t, args...)
			if tc.expErr != nil {
				require.ErrorContains(t, res.err, tc.expErr.Error())
			} else {
				require.NoError(t, res.err)
			}

			require.Containsf(t, res.stdout, tc.expOutput, "unexpected stdout:\n\n%s", res.stdout)
		})
	}
}
