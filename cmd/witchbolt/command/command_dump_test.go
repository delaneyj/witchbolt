package command_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/delaneyj/witchbolt"
	"github.com/delaneyj/witchbolt/internal/btesting"
)

func TestDumpCommand_Run(t *testing.T) {
	t.Log("Creating database")
	db := btesting.MustCreateDBWithOption(t, &witchbolt.Options{PageSize: 4096})
	require.NoError(t, db.Close())
	defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

	t.Log("Running dump command")
	res := runCLI(t, "dump", db.Path(), "0")
	require.NoError(t, res.err)

	t.Log("Checking output")
	exp := `0000010 edda 0ced 0200 0000 0010 0000 0000 0000`
	require.True(t, strings.Contains(res.stdout, exp), "unexpected stdout:", res.stdout)
}

func TestDumpCommand_NoArgs(t *testing.T) {
	res := runCLI(t, "dump")
	require.Error(t, res.err)
	require.Contains(t, res.err.Error(), "expected \"<path> <page-i-ds> ...\"")
}
