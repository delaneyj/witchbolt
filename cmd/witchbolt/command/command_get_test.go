package command_test

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/delaneyj/witchbolt"
	"github.com/delaneyj/witchbolt/internal/btesting"
)

func TestGetCommand_Run(t *testing.T) {
	testCases := []struct {
		name          string
		printable     bool
		testBucket    string
		testKey       string
		expectedValue string
	}{
		{
			name:          "printable data",
			printable:     true,
			testBucket:    "foo",
			testKey:       "foo-1",
			expectedValue: "value-foo-1\n",
		},
		{
			name:          "non printable data",
			printable:     false,
			testBucket:    "bar",
			testKey:       "100001",
			expectedValue: hex.EncodeToString(convertInt64IntoBytes(100001)) + "\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Creating test database for subtest '%s'", tc.name)
			db := btesting.MustCreateDB(t)

			t.Log("Inserting test data")
			err := db.Update(func(tx *witchbolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists([]byte(tc.testBucket))
				if err != nil {
					return fmt.Errorf("create bucket %q: %w", tc.testBucket, err)
				}

				if tc.printable {
					return b.Put([]byte(tc.testKey), []byte("value-"+tc.testKey))
				}

				return b.Put([]byte(tc.testKey), convertInt64IntoBytes(100001))
			})
			require.NoError(t, err)
			db.Close()
			defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

			args := []string{"get", db.Path(), tc.testBucket, tc.testKey}

			t.Log("Running get command")
			res := runCLI(t, args...)
			require.NoError(t, res.err)
			require.Equalf(t, tc.expectedValue, res.stdout, "unexpected stdout:\n\n%s", res.stdout)
		})
	}
}

func TestGetCommand_NoArgs(t *testing.T) {
	res := runCLI(t, "get")
	require.Error(t, res.err)
	require.Contains(t, res.err.Error(), "expected \"<path> <bucket-key> ...\"")
}
