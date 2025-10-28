package command

import "errors"

// BrowseCmd - interactive TUI browser (to be implemented with bubbletea)
type BrowseCmd struct {
	Path string `arg:"" help:"Path to witchbolt database file" type:"path"`
}

func (c *BrowseCmd) Run() error {
	return errors.New("browse command (interactive TUI) not yet implemented - will use charmbracelet/bubbletea")
}
