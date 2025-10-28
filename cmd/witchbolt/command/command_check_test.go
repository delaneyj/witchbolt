package command_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/delaneyj/witchbolt/internal/btesting"
	"github.com/delaneyj/witchbolt/internal/guts_cli"
)

func TestCheckCommand_Run(t *testing.T) {
	testCases := []struct {
		name      string
		args      []string
		expErr    error
		expOutput string
	}{
		{
			name:      "check whole db",
			args:      []string{"check", "path"},
			expErr:    nil,
			expOutput: "OK\n",
		},
		{
			name:      "check valid pageId",
			args:      []string{"check", "path", "--from-page-id", "3"},
			expErr:    nil,
			expOutput: "OK\n",
		},
		{
			name:      "check invalid pageId",
			args:      []string{"check", "path", "--from-page-id", "1"},
			expErr:    guts_cli.ErrCorrupt,
			expOutput: "page ID (1) out of range [2, 4)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			t.Log("Creating sample DB")
			db := btesting.MustCreateDB(t)
			db.Close()
			defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

			t.Log("Running check cmd")
			args := append([]string{}, tc.args...)
			args[1] = db.Path()
			res := runCLI(t, args...)
			if tc.expErr != nil {
				require.ErrorIs(t, res.err, tc.expErr)
			} else {
				require.NoError(t, res.err)
			}
			require.Containsf(t, res.stdout, tc.expOutput, "unexpected stdout:\n\n%s", res.stdout)
		})
	}
}
