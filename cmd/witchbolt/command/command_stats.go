package command

import (
	"bytes"
	"fmt"

	"github.com/delaneyj/witchbolt"
)

type StatsCmd struct {
	Path   string `arg:"" help:"Path to witchbolt database file" type:"path"`
	Prefix string `arg:"" optional:"" help:"Bucket name prefix filter"`
}

func (c *StatsCmd) Run() error {
	if _, err := checkSourceDBPath(c.Path); err != nil {
		return err
	}

	// open database.
	db, err := witchbolt.Open(c.Path, 0600, &witchbolt.Options{
		ReadOnly:        true,
		PreLoadFreelist: true,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	return db.View(func(tx *witchbolt.Tx) error {
		var s witchbolt.BucketStats
		var count int
		if err := tx.ForEach(func(name []byte, b *witchbolt.Bucket) error {
			if bytes.HasPrefix(name, []byte(c.Prefix)) {
				s.Add(b.Stats())
				count += 1
			}
			return nil
		}); err != nil {
			return err
		}

		fmt.Printf("Aggregate statistics for %d buckets\n\n", count)

		fmt.Println("Page count statistics")
		fmt.Printf("\tNumber of logical branch pages: %d\n", s.BranchPageN)
		fmt.Printf("\tNumber of physical branch overflow pages: %d\n", s.BranchOverflowN)
		fmt.Printf("\tNumber of logical leaf pages: %d\n", s.LeafPageN)
		fmt.Printf("\tNumber of physical leaf overflow pages: %d\n", s.LeafOverflowN)

		fmt.Println("Tree statistics")
		fmt.Printf("\tNumber of keys/value pairs: %d\n", s.KeyN)
		fmt.Printf("\tNumber of levels in B+tree: %d\n", s.Depth)

		fmt.Println("Page size utilization")
		fmt.Printf("\tBytes allocated for physical branch pages: %d\n", s.BranchAlloc)
		var percentage int
		if s.BranchAlloc != 0 {
			percentage = int(float32(s.BranchInuse) * 100.0 / float32(s.BranchAlloc))
		}
		fmt.Printf("\tBytes actually used for branch data: %d (%d%%)\n", s.BranchInuse, percentage)
		fmt.Printf("\tBytes allocated for physical leaf pages: %d\n", s.LeafAlloc)
		percentage = 0
		if s.LeafAlloc != 0 {
			percentage = int(float32(s.LeafInuse) * 100.0 / float32(s.LeafAlloc))
		}
		fmt.Printf("\tBytes actually used for leaf data: %d (%d%%)\n", s.LeafInuse, percentage)

		fmt.Println("Bucket statistics")
		fmt.Printf("\tTotal number of buckets: %d\n", s.BucketN)
		percentage = 0
		if s.BucketN != 0 {
			percentage = int(float32(s.InlineBucketN) * 100.0 / float32(s.BucketN))
		}
		fmt.Printf("\tTotal number on inlined buckets: %d (%d%%)\n", s.InlineBucketN, percentage)
		percentage = 0
		if s.LeafInuse != 0 {
			percentage = int(float32(s.InlineBucketInuse) * 100.0 / float32(s.LeafInuse))
		}
		fmt.Printf("\tBytes used for inlined buckets: %d (%d%%)\n", s.InlineBucketInuse, percentage)

		return nil
	})
}
