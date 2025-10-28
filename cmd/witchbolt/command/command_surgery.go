package command

import (
	"errors"
	"fmt"
	"os"

	"github.com/delaneyj/witchbolt/internal/common"
	"github.com/delaneyj/witchbolt/internal/guts_cli"
	"github.com/delaneyj/witchbolt/internal/surgeon"
)

type SurgeryCmd struct {
	RevertMetaPage    SurgeryRevertMetaPageCmd    `cmd:"" help:"Revert the meta page to undo the latest transaction."`
	CopyPage          SurgeryCopyPageCmd          `cmd:"" help:"Copy a page to another page."`
	ClearPage         SurgeryClearPageCmd         `cmd:"" help:"Clear all elements from a page."`
	ClearPageElements SurgeryClearPageElementsCmd `cmd:"" help:"Clear a range of elements from a page."`
	Freelist          SurgeryFreelistCmd          `cmd:"" help:"Freelist related surgery commands."`
	Meta              SurgeryMetaCmd              `cmd:"" help:"Meta page related surgery commands."`
}

type SurgeryRevertMetaPageCmd struct {
	Src    string `arg:"" help:"Path to witchbolt database file" type:"path"`
	Output string `name:"output" required:"" help:"Path to the output database file" type:"path"`
}

func (c *SurgeryRevertMetaPageCmd) Run() error {
	cfg := surgeryBaseOptions{outputDBFilePath: c.Output}
	if err := cfg.Validate(); err != nil {
		return err
	}
	return surgeryRevertMetaPageFunc(c.Src, cfg)
}

type SurgeryCopyPageCmd struct {
	Src      string `arg:"" help:"Path to witchbolt database file" type:"path"`
	Output   string `name:"output" required:"" help:"Path to the output database file" type:"path"`
	FromPage uint64 `name:"from-page" required:"" help:"Source page ID"`
	ToPage   uint64 `name:"to-page" required:"" help:"Destination page ID"`
}

func (c *SurgeryCopyPageCmd) Run() error {
	cfg := surgeryCopyPageOptions{
		surgeryBaseOptions: surgeryBaseOptions{outputDBFilePath: c.Output},
		sourcePageId:       c.FromPage,
		destinationPageId:  c.ToPage,
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	return surgeryCopyPageFunc(c.Src, cfg)
}

type SurgeryClearPageCmd struct {
	Src    string `arg:"" help:"Path to witchbolt database file" type:"path"`
	Output string `name:"output" required:"" help:"Path to the output database file" type:"path"`
	PageID uint64 `name:"pageId" required:"" help:"Page ID to clear"`
}

func (c *SurgeryClearPageCmd) Run() error {
	cfg := surgeryClearPageOptions{
		surgeryBaseOptions: surgeryBaseOptions{outputDBFilePath: c.Output},
		pageId:             c.PageID,
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	return surgeryClearPageFunc(c.Src, cfg)
}

type SurgeryClearPageElementsCmd struct {
	Src       string `arg:"" help:"Path to witchbolt database file" type:"path"`
	Output    string `name:"output" required:"" help:"Path to the output database file" type:"path"`
	PageID    uint64 `name:"pageId" required:"" help:"Page ID to modify"`
	FromIndex int    `name:"from-index" required:"" help:"Start element index (inclusive)."`
	ToIndex   int    `name:"to-index" required:"" help:"End element index (exclusive). Use -1 for the end of page."`
}

func (c *SurgeryClearPageElementsCmd) Run() error {
	cfg := surgeryClearPageElementsOptions{
		surgeryBaseOptions: surgeryBaseOptions{outputDBFilePath: c.Output},
		pageId:             c.PageID,
		startElementIdx:    c.FromIndex,
		endElementIdx:      c.ToIndex,
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	return surgeryClearPageElementFunc(c.Src, cfg)
}

type surgeryBaseOptions struct {
	outputDBFilePath string
}

func (o *surgeryBaseOptions) Validate() error {
	if o.outputDBFilePath == "" {
		return errors.New("output database path wasn't given, specify output database file path with --output option")
	}
	return nil
}

func surgeryRevertMetaPageFunc(srcDBPath string, cfg surgeryBaseOptions) error {
	if _, err := checkSourceDBPath(srcDBPath); err != nil {
		return err
	}

	if err := common.CopyFile(srcDBPath, cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("[revert-meta-page] copy file failed: %w", err)
	}

	if err := surgeon.RevertMetaPage(cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("revert-meta-page command failed: %w", err)
	}

	fmt.Fprintln(os.Stdout, "The meta page is reverted.")

	return nil
}

type surgeryCopyPageOptions struct {
	surgeryBaseOptions
	sourcePageId      uint64
	destinationPageId uint64
}

func (o *surgeryCopyPageOptions) Validate() error {
	if err := o.surgeryBaseOptions.Validate(); err != nil {
		return err
	}
	if o.sourcePageId == o.destinationPageId {
		return fmt.Errorf("'--from-page' and '--to-page' have the same value: %d", o.sourcePageId)
	}
	return nil
}

func surgeryCopyPageFunc(srcDBPath string, cfg surgeryCopyPageOptions) error {
	if _, err := checkSourceDBPath(srcDBPath); err != nil {
		return err
	}

	if err := common.CopyFile(srcDBPath, cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("[copy-page] copy file failed: %w", err)
	}

	if err := surgeon.CopyPage(cfg.outputDBFilePath, common.Pgid(cfg.sourcePageId), common.Pgid(cfg.destinationPageId)); err != nil {
		return fmt.Errorf("copy-page command failed: %w", err)
	}

	meta, err := readMetaPage(srcDBPath)
	if err != nil {
		return err
	}
	if meta.IsFreelistPersisted() {
		fmt.Fprintf(os.Stdout, "WARNING: the free list might have changed.\n")
		fmt.Fprintf(os.Stdout, "Please consider executing `./witchbolt surgery freelist abandon ...`\n")
	}

	fmt.Fprintf(os.Stdout, "The page %d was successfully copied to page %d\n", cfg.sourcePageId, cfg.destinationPageId)
	return nil
}

type surgeryClearPageOptions struct {
	surgeryBaseOptions
	pageId uint64
}

func (o *surgeryClearPageOptions) Validate() error {
	if err := o.surgeryBaseOptions.Validate(); err != nil {
		return err
	}
	if o.pageId < 2 {
		return fmt.Errorf("the pageId must be at least 2, but got %d", o.pageId)
	}
	return nil
}

func surgeryClearPageFunc(srcDBPath string, cfg surgeryClearPageOptions) error {
	if _, err := checkSourceDBPath(srcDBPath); err != nil {
		return err
	}

	if err := common.CopyFile(srcDBPath, cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("[clear-page] copy file failed: %w", err)
	}

	needAbandonFreelist, err := surgeon.ClearPage(cfg.outputDBFilePath, common.Pgid(cfg.pageId))
	if err != nil {
		return fmt.Errorf("clear-page command failed: %w", err)
	}

	if needAbandonFreelist {
		fmt.Fprintf(os.Stdout, "WARNING: The clearing has abandoned some pages that are not yet referenced from free list.\n")
		fmt.Fprintf(os.Stdout, "Please consider executing `./witchbolt surgery freelist abandon ...`\n")
	}

	fmt.Fprintf(os.Stdout, "The page (%d) was cleared\n", cfg.pageId)
	return nil
}

type surgeryClearPageElementsOptions struct {
	surgeryBaseOptions
	pageId          uint64
	startElementIdx int
	endElementIdx   int
}

func (o *surgeryClearPageElementsOptions) Validate() error {
	if err := o.surgeryBaseOptions.Validate(); err != nil {
		return err
	}
	if o.pageId < 2 {
		return fmt.Errorf("the pageId must be at least 2, but got %d", o.pageId)
	}
	return nil
}

func surgeryClearPageElementFunc(srcDBPath string, cfg surgeryClearPageElementsOptions) error {
	if _, err := checkSourceDBPath(srcDBPath); err != nil {
		return err
	}

	if err := common.CopyFile(srcDBPath, cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("[clear-page-element] copy file failed: %w", err)
	}

	needAbandonFreelist, err := surgeon.ClearPageElements(cfg.outputDBFilePath, common.Pgid(cfg.pageId), cfg.startElementIdx, cfg.endElementIdx, false)
	if err != nil {
		return fmt.Errorf("clear-page-element command failed: %w", err)
	}

	if needAbandonFreelist {
		fmt.Fprintf(os.Stdout, "WARNING: The clearing has abandoned some pages that are not yet referenced from free list.\n")
		fmt.Fprintf(os.Stdout, "Please consider executing `./witchbolt surgery freelist abandon ...`\n")
	}

	fmt.Fprintf(os.Stdout, "All elements in [%d, %d) in page %d were cleared\n", cfg.startElementIdx, cfg.endElementIdx, cfg.pageId)
	return nil
}

func readMetaPage(path string) (*common.Meta, error) {
	pageSize, _, err := guts_cli.ReadPageAndHWMSize(path)
	if err != nil {
		return nil, fmt.Errorf("read Page size failed: %w", err)
	}

	m := make([]*common.Meta, 2)
	for i := 0; i < 2; i++ {
		m[i], _, err = ReadMetaPageAt(path, uint32(i), uint32(pageSize))
		if err != nil {
			return nil, fmt.Errorf("read meta page %d failed: %w", i, err)
		}
	}

	if m[0].Txid() > m[1].Txid() {
		return m[0], nil
	}
	return m[1], nil
}
