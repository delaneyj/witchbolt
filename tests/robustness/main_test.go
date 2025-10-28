//go:build linux

package robustness

import (
	"flag"
	"os"
	"testing"

	testutils "github.com/delaneyj/witchbolt/tests/utils"
)

func TestMain(m *testing.M) {
	flag.Parse()
	testutils.RequiresRoot()
	os.Exit(m.Run())
}
