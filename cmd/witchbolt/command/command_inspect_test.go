package command_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/delaneyj/witchbolt"
	"github.com/delaneyj/witchbolt/internal/btesting"
)

func TestInspect(t *testing.T) {
	pageSize := 4096
	db := btesting.MustCreateDBWithOption(t, &witchbolt.Options{PageSize: pageSize})
	srcPath := db.Path()
	db.Close()

	defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

	res := runCLI(t, "inspect", srcPath)
	require.NoError(t, res.err)
}
