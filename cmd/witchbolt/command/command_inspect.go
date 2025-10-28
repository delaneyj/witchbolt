package command

import (
	"encoding/json"
	"fmt"

	"github.com/delaneyj/witchbolt"
)

type InspectCmd struct {
	Path string `arg:"" help:"Path to witchbolt database file" type:"path"`
}

func (c *InspectCmd) Run() error {
	if _, err := checkSourceDBPath(c.Path); err != nil {
		return err
	}

	db, err := witchbolt.Open(c.Path, 0600, &witchbolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()

	return db.View(func(tx *witchbolt.Tx) error {
		bs := tx.Inspect()
		out, err := json.MarshalIndent(bs, "", "    ")
		if err != nil {
			return err
		}
		fmt.Println(string(out))
		return nil
	})
}
