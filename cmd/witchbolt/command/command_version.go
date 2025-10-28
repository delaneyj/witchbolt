package command

import (
	"fmt"
	"runtime"

	"github.com/delaneyj/witchbolt/version"
)

type VersionCmd struct{}

func (v *VersionCmd) Run() error {
	fmt.Printf("witchbolt Version: %s\n", version.Version)
	fmt.Printf("Go Version: %s\n", runtime.Version())
	fmt.Printf("Go OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
	return nil
}
