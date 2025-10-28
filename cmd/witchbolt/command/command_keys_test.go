package command_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/delaneyj/witchbolt"
	"github.com/delaneyj/witchbolt/internal/btesting"
)

// Ensure the "keys" command can print a list of keys for a bucket.
func TestKeysCommand_Run(t *testing.T) {
	testCases := []struct {
		name       string
		printable  bool
		testBucket string
		expected   string
	}{
		{
			name:       "printable keys",
			printable:  true,
			testBucket: "foo",
			expected:   "foo-0\nfoo-1\nfoo-2\n",
		},
		{
			name:       "non printable keys",
			printable:  false,
			testBucket: "bar",
			expected:   convertInt64KeysIntoHexString(100001, 100002, 100003),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Creating test database for subtest '%s'", tc.name)
			db := btesting.MustCreateDB(t)
			err := db.Update(func(tx *witchbolt.Tx) error {
				t.Logf("creating test bucket %s", tc.testBucket)
				b, bErr := tx.CreateBucketIfNotExists([]byte(tc.testBucket))
				if bErr != nil {
					return fmt.Errorf("error creating test bucket %q: %v", tc.testBucket, bErr)
				}

				t.Logf("inserting test data into test bucket %s", tc.testBucket)
				if tc.printable {
					for i := 0; i < 3; i++ {
						key := fmt.Sprintf("%s-%d", tc.testBucket, i)
						if pErr := b.Put([]byte(key), []byte{0}); pErr != nil {
							return pErr
						}
					}
				} else {
					for i := 100001; i < 100004; i++ {
						k := convertInt64IntoBytes(int64(i))
						if pErr := b.Put(k, []byte{0}); pErr != nil {
							return pErr
						}
					}
				}
				return nil
			})
			require.NoError(t, err)
			db.Close()
			defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

			t.Log("Running Keys cmd")
			res := runCLI(t, "keys", db.Path(), tc.testBucket)
			require.NoError(t, res.err)
			require.Equalf(t, tc.expected, res.stdout, "unexpected stdout:\n\n%s", res.stdout)
		})
	}
}

func TestKeyCommand_NoArgs(t *testing.T) {
	res := runCLI(t, "keys")
	require.Error(t, res.err)
	require.Contains(t, res.err.Error(), "expected \"<path> <buckets> ...\"")
}
