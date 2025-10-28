package command

import (
	"errors"
	"fmt"
	"os"

	"github.com/delaneyj/witchbolt"
)

type CompactCmd struct {
	Src       string `arg:"" help:"Source witchbolt database file" type:"path"`
	Output    string `short:"o" required:"" help:"Destination database file" type:"path"`
	TxMaxSize int64  `default:"65536" help:"Maximum transaction size"`
	NoSync    bool   `help:"Disable fsync for destination database"`
}

func (c *CompactCmd) Run() error {
	if c.Output == "" {
		return errors.New("output file required")
	}

	// ensure source file exists.
	fi, err := checkSourceDBPath(c.Src)
	if err != nil {
		return err
	}
	initialSize := fi.Size()

	// open source database.
	src, err := witchbolt.Open(c.Src, 0400, &witchbolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer src.Close()

	// open destination database.
	dst, err := witchbolt.Open(c.Output, fi.Mode(), &witchbolt.Options{NoSync: c.NoSync})
	if err != nil {
		return err
	}
	defer dst.Close()

	// run compaction.
	if err := witchbolt.Compact(dst, src, c.TxMaxSize); err != nil {
		return err
	}

	// report stats on new size.
	fi, err = os.Stat(c.Output)
	if err != nil {
		return err
	} else if fi.Size() == 0 {
		return fmt.Errorf("zero db size")
	}
	fmt.Printf("%d -> %d bytes (gain=%.2fx)\n", initialSize, fi.Size(), float64(initialSize)/float64(fi.Size()))

	return nil
}
