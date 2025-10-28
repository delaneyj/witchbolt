package command

import (
	"fmt"
	"strconv"

	"github.com/delaneyj/witchbolt"
)

type PageError struct {
	ID  int
	Err error
}

func (e *PageError) Error() string {
	return fmt.Sprintf("page error: id=%d, err=%s", e.ID, e.Err)
}

type PagesCmd struct {
	Path string `arg:"" help:"Path to witchbolt database file" type:"path"`
}

func (c *PagesCmd) Run() error {
	if _, err := checkSourceDBPath(c.Path); err != nil {
		return err
	}

	// Open database.
	db, err := witchbolt.Open(c.Path, 0600, &witchbolt.Options{
		ReadOnly:        true,
		PreLoadFreelist: true,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	// Write header.
	fmt.Println("ID       TYPE       ITEMS  OVRFLW")
	fmt.Println("======== ========== ====== ======")

	return db.View(func(tx *witchbolt.Tx) error {
		var id int
		for {
			p, err := tx.Page(id)
			if err != nil {
				return &PageError{ID: id, Err: err}
			} else if p == nil {
				break
			}

			// Only display count and overflow if this is a non-free page.
			var count, overflow string
			if p.Type != "free" {
				count = strconv.Itoa(p.Count)
				if p.OverflowCount > 0 {
					overflow = strconv.Itoa(p.OverflowCount)
				}
			}

			// Print table row.
			fmt.Printf("%-8d %-10s %-6s %-6s\n", p.ID, p.Type, count, overflow)

			// Move to the next non-overflow page.
			id += 1
			if p.Type != "free" {
				id += p.OverflowCount
			}
		}
		return nil
	})
}
