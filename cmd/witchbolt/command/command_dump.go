package command

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/delaneyj/witchbolt/internal/guts_cli"
	"github.com/valyala/bytebufferpool"
)

type DumpCmd struct {
	Path    string   `arg:"" help:"Path to witchbolt database file" type:"path"`
	PageIDs []string `arg:"" help:"Page IDs to dump (one or more)"`
}

func (c *DumpCmd) Run() error {
	pageIDs, err := stringToPages(c.PageIDs)
	if err != nil {
		return err
	} else if len(pageIDs) == 0 {
		return ErrPageIDRequired
	}

	if _, err := checkSourceDBPath(c.Path); err != nil {
		return err
	}

	// open database to retrieve page size.
	pageSize, _, err := guts_cli.ReadPageAndHWMSize(c.Path)
	if err != nil {
		return err
	}

	// open database file handler.
	f, err := os.Open(c.Path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// print each page listed.
	for i, pageID := range pageIDs {
		// print a separator.
		if i > 0 {
			fmt.Println("===============================================")
		}

		// print page to stdout.
		if err := dumpPage(os.Stdout, f, pageID, uint64(pageSize)); err != nil {
			return err
		}
	}

	return nil
}

func dumpPage(w io.Writer, r io.ReaderAt, pageID uint64, pageSize uint64) error {
	const bytesPerLineN = 16

	// read page into buffer.
	size := int(pageSize)
	if uint64(size) != pageSize {
		return fmt.Errorf("page size %d exceeds int capacity", pageSize)
	}

	buf, pooled := sizedBytes(size)
	defer bytebufferpool.Put(pooled)
	addr := pageID * uint64(pageSize)
	if n, err := r.ReadAt(buf, int64(addr)); err != nil {
		return err
	} else if uint64(n) != pageSize {
		return io.ErrUnexpectedEOF
	}

	// write out to writer in 16-byte lines.
	var prev []byte
	var skipped bool
	for offset := uint64(0); offset < pageSize; offset += bytesPerLineN {
		// retrieve current 16-byte line.
		line := buf[offset : offset+bytesPerLineN]
		isLastLine := (offset == (pageSize - bytesPerLineN))

		// if it's the same as the previous line then print a skip.
		if bytes.Equal(line, prev) && !isLastLine {
			if !skipped {
				fmt.Fprintf(w, "%07x *\n", addr+offset)
				skipped = true
			}
		} else {
			// print line as hexadecimal in 2-byte groups.
			fmt.Fprintf(w, "%07x %04x %04x %04x %04x %04x %04x %04x %04x\n", addr+offset,
				line[0:2], line[2:4], line[4:6], line[6:8],
				line[8:10], line[10:12], line[12:14], line[14:16],
			)

			skipped = false
		}

		// save the previous line.
		prev = line
	}
	fmt.Fprint(w, "\n")

	return nil
}
