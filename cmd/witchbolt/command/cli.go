package command

import "github.com/delaneyj/witchbolt/version"

const (
	cliName        = "witchbolt"
	cliDescription = "A simple command line tool for inspecting witchbolt databases"
)

// CLI is the main command structure
var CLI struct {
	Version VersionCmd `cmd:"" help:"Print the current version of witchbolt"`

	// Database inspection commands
	Inspect InspectCmd `cmd:"" help:"Inspect the structure of the database"`
	Check   CheckCmd   `cmd:"" help:"Verify integrity of witchbolt database"`
	Info    InfoCmd    `cmd:"" help:"Print basic info about witchbolt database"`
	Stats   StatsCmd   `cmd:"" help:"Iterate over all pages in a database"`

	// Data access commands
	Buckets BucketsCmd `cmd:"" help:"Print a list of buckets"`
	Keys    KeysCmd    `cmd:"" help:"Print a list of keys in a bucket"`
	Get     GetCmd     `cmd:"" help:"Get the value of a key from a bucket"`
	Dump    DumpCmd    `cmd:"" help:"Dump all key/value pairs from specified buckets or entire database"`

	// Page-level commands
	Pages    PagesCmd    `cmd:"" help:"Dump page IDs for all page types"`
	Page     PageCmd     `cmd:"" help:"Print a page"`
	PageItem PageItemCmd `cmd:"" aliases:"page-item" help:"Print the key and value of a page item"`

	// Database modification commands
	Compact CompactCmd `cmd:"" help:"Creates a compacted copy of the database"`
	Surgery SurgeryCmd `cmd:"" help:"Perform surgery on a witchbolt database"`

	// Performance commands
	Bench BenchCmd `cmd:"" help:"Benchmark the database"`

	// Interactive commands
	Browse BrowseCmd `cmd:"" help:"Interactive database browser (TUI)"`
}

// KongVars returns variables for Kong parser
func KongVars() map[string]string {
	return map[string]string{
		"version": version.Version,
	}
}
