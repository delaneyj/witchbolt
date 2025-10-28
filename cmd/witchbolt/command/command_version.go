package command

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"

	"github.com/delaneyj/witchbolt/version"
)

func newVersionCommand() *cobra.Command {
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "print the current version of witchbolt",
		Long:  "print the current version of witchbolt",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("witchbolt Version: %s\n", version.Version)
			fmt.Printf("Go Version: %s\n", runtime.Version())
			fmt.Printf("Go OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
		},
	}

	return versionCmd
}
