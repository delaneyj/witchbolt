package command

import (
	"fmt"
	"os"

	"github.com/delaneyj/witchbolt"
	"github.com/delaneyj/witchbolt/internal/common"
	"github.com/delaneyj/witchbolt/internal/surgeon"
)

type SurgeryFreelistCmd struct {
	Abandon SurgeryFreelistAbandonCmd `cmd:"" help:"Abandon the freelist from both meta pages."`
	Rebuild SurgeryFreelistRebuildCmd `cmd:"" help:"Rebuild the freelist."`
}

type SurgeryFreelistAbandonCmd struct {
	Src    string `arg:"" help:"Path to witchbolt database file" type:"path"`
	Output string `name:"output" required:"" help:"Path to the output database file" type:"path"`
}

func (c *SurgeryFreelistAbandonCmd) Run() error {
	cfg := surgeryBaseOptions{outputDBFilePath: c.Output}
	if err := cfg.Validate(); err != nil {
		return err
	}
	return surgeryFreelistAbandonFunc(c.Src, cfg)
}

func surgeryFreelistAbandonFunc(srcDBPath string, cfg surgeryBaseOptions) error {
	if _, err := checkSourceDBPath(srcDBPath); err != nil {
		return err
	}

	if err := common.CopyFile(srcDBPath, cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("[freelist abandon] copy file failed: %w", err)
	}

	if err := surgeon.ClearFreelist(cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("abandom-freelist command failed: %w", err)
	}

	fmt.Fprintf(os.Stdout, "The freelist was abandoned in both meta pages.\nIt may cause some delay on next startup because witchbolt needs to scan the whole db to reconstruct the free list.\n")
	return nil
}

type SurgeryFreelistRebuildCmd struct {
	Src    string `arg:"" help:"Path to witchbolt database file" type:"path"`
	Output string `name:"output" required:"" help:"Path to the output database file" type:"path"`
}

func (c *SurgeryFreelistRebuildCmd) Run() error {
	cfg := surgeryBaseOptions{outputDBFilePath: c.Output}
	if err := cfg.Validate(); err != nil {
		return err
	}
	return surgeryFreelistRebuildFunc(c.Src, cfg)
}

func surgeryFreelistRebuildFunc(srcDBPath string, cfg surgeryBaseOptions) error {
	// Ensure source file exists.
	fi, err := checkSourceDBPath(srcDBPath)
	if err != nil {
		return err
	}

	// make sure the freelist isn't present in the file.
	meta, err := readMetaPage(srcDBPath)
	if err != nil {
		return err
	}
	if meta.IsFreelistPersisted() {
		return ErrSurgeryFreelistAlreadyExist
	}

	if err := common.CopyFile(srcDBPath, cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("[freelist rebuild] copy file failed: %w", err)
	}

	// witchboltDB automatically reconstruct & sync freelist in write mode.
	db, err := witchbolt.Open(cfg.outputDBFilePath, fi.Mode(), &witchbolt.Options{NoFreelistSync: false})
	if err != nil {
		return fmt.Errorf("[freelist rebuild] open db file failed: %w", err)
	}
	err = db.Close()
	if err != nil {
		return fmt.Errorf("[freelist rebuild] close db file failed: %w", err)
	}

	fmt.Fprintf(os.Stdout, "The freelist was successfully rebuilt.\n")
	return nil
}
