package command_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/delaneyj/witchbolt"
	"github.com/delaneyj/witchbolt/internal/btesting"
)

// Ensure the "pages" command neither panic, nor change the db file.
func TestPagesCommand_Run(t *testing.T) {
	t.Log("Creating sample DB")
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *witchbolt.Tx) error {
		for _, name := range []string{"foo", "bar"} {
			b, err := tx.CreateBucket([]byte(name))
			if err != nil {
				return err
			}
			for i := 0; i < 3; i++ {
				key := fmt.Sprintf("%s-%d", name, i)
				val := fmt.Sprintf("val-%s-%d", name, i)
				if err := b.Put([]byte(key), []byte(val)); err != nil {
					return err
				}
			}
		}
		return nil
	})
	require.NoError(t, err)
	db.Close()
	defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

	t.Log("Running pages cmd")
	res := runCLI(t, "pages", db.Path())
	require.NoError(t, res.err)
}

func TestPagesCommand_NoArgs(t *testing.T) {
	res := runCLI(t, "pages")
	require.Error(t, res.err)
	require.Contains(t, res.err.Error(), "expected \"<path>\"")
}
