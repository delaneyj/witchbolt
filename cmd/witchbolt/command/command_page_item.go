package command

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/delaneyj/witchbolt/internal/common"
	"github.com/delaneyj/witchbolt/internal/guts_cli"
)

type PageItemCmd struct {
	Path      string `arg:"" help:"Path to witchbolt database file" type:"path"`
	PageID    uint64 `arg:"" help:"Page ID"`
	ItemID    uint64 `arg:"" help:"Item ID"`
	KeyOnly   bool   `help:"Print only the key"`
	ValueOnly bool   `help:"Print only the value"`
	Format    string `default:"auto" help:"Output format: auto|ascii-encoded|hex|bytes"`
}

func (c *PageItemCmd) Run() error {
	if c.KeyOnly && c.ValueOnly {
		return errors.New("the --key-only or --value-only flag may be set, but not both")
	}

	if _, err := checkSourceDBPath(c.Path); err != nil {
		return err
	}

	// retrieve page info and page size.
	_, buf, err := guts_cli.ReadPage(c.Path, c.PageID)
	if err != nil {
		return err
	}

	if !c.ValueOnly {
		err := pageItemPrintLeafItemKey(os.Stdout, buf, uint16(c.ItemID), c.Format)
		if err != nil {
			return err
		}
	}
	if !c.KeyOnly {
		err := pageItemPrintLeafItemValue(os.Stdout, buf, uint16(c.ItemID), c.Format)
		if err != nil {
			return err
		}
	}

	return nil
}

func pageItemPrintLeafItemKey(w io.Writer, pageBytes []byte, index uint16, format string) error {
	k, _, err := pageItemLeafPageElement(pageBytes, index)
	if err != nil {
		return err
	}

	return writelnBytes(w, k, format)
}

func pageItemPrintLeafItemValue(w io.Writer, pageBytes []byte, index uint16, format string) error {
	_, v, err := pageItemLeafPageElement(pageBytes, index)
	if err != nil {
		return err
	}
	return writelnBytes(w, v, format)
}

func pageItemLeafPageElement(pageBytes []byte, index uint16) ([]byte, []byte, error) {
	p := common.LoadPage(pageBytes)
	if index >= p.Count() {
		return nil, nil, fmt.Errorf("leafPageElement: expected item index less than %d, but got %d", p.Count(), index)
	}
	if p.Typ() != "leaf" {
		return nil, nil, fmt.Errorf("leafPageElement: expected page type of 'leaf', but got '%s'", p.Typ())
	}

	e := p.LeafPageElement(index)
	return e.Key(), e.Value(), nil
}
