package command_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/delaneyj/witchbolt/internal/btesting"
)

// Ensure the "info" command can print information about a database.
func TestInfoCommand_Run(t *testing.T) {
	t.Log("Creating sample DB")
	db := btesting.MustCreateDB(t)
	db.Close()
	defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

	t.Log("Running info cmd")
	res := runCLI(t, "info", db.Path())
	require.NoError(t, res.err)
}

func TestInfoCommand_NoArgs(t *testing.T) {
	res := runCLI(t, "info")
	require.Error(t, res.err)
	require.Contains(t, res.err.Error(), "expected \"<path>\"")
}
