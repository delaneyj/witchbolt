package command_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/delaneyj/witchbolt"
	"github.com/delaneyj/witchbolt/cmd/witchbolt/command"
	"github.com/delaneyj/witchbolt/internal/btesting"
)

func TestPageCommand_Run(t *testing.T) {
	t.Log("Creating a new database")
	db := btesting.MustCreateDBWithOption(t, &witchbolt.Options{PageSize: 4096})
	db.Close()

	defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

	exp := "Page ID:    0\n" +
		"Page Type:  meta\n" +
		"Total Size: 4096 bytes\n" +
		"Overflow pages: 0\n" +
		"Version:    2\n" +
		"Page Size:  4096 bytes\n" +
		"Flags:      00000000\n" +
		"Root:       <pgid=3>\n" +
		"Freelist:   <pgid=2>\n" +
		"HWM:        <pgid=4>\n" +
		"Txn ID:     0\n" +
		"Checksum:   07516e114689fdee\n\n"

	t.Log("Running page command")
	res := runCLI(t, "page", db.Path(), "0")
	require.NoError(t, res.err)
	require.Equal(t, exp, res.stdout, "unexpected stdout")
}

func TestPageCommand_ExclusiveArgs(t *testing.T) {
	testCases := []struct {
		name    string
		pageIds string
		allFlag string
		expErr  error
	}{
		{
			name:    "flag only",
			pageIds: "",
			allFlag: "--all",
			expErr:  nil,
		},
		{
			name:    "pageIds only",
			pageIds: "0",
			allFlag: "",
			expErr:  nil,
		},
		{
			name:    "pageIds and flag",
			pageIds: "0",
			allFlag: "--all",
			expErr:  command.ErrInvalidPageArgs,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Log("Creating a new database")
			db := btesting.MustCreateDBWithOption(t, &witchbolt.Options{PageSize: 4096})
			db.Close()

			defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

			t.Log("Running page command")
			args := []string{"page", db.Path()}
			if tc.pageIds != "" {
				args = append(args, tc.pageIds)
			}
			if tc.allFlag != "" {
				args = append(args, tc.allFlag)
			}

			res := runCLI(t, args...)
			if tc.expErr != nil {
				require.ErrorIs(t, res.err, tc.expErr)
			} else {
				require.NoError(t, res.err)
			}
		})
	}
}

func TestPageCommand_NoArgs(t *testing.T) {
	res := runCLI(t, "page")
	require.Error(t, res.err)
	require.Contains(t, res.err.Error(), "expected \"<path>\"")
}
