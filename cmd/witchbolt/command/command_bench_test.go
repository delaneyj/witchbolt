package command_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Ensure the "bench" command runs and exits without errors
func TestBenchCommand_Run(t *testing.T) {
	tests := map[string][]string{
		"no-args":    nil,
		"100k count": {"--count", "100000"},
	}

	for name, args := range tests {
		t.Run(name, func(t *testing.T) {
			cliArgs := append([]string{"bench"}, args...)
			res := runCLI(t, cliArgs...)
			require.NoError(t, res.err)
			require.Contains(t, res.stderr, "starting write benchmark.")
			require.Contains(t, res.stderr, "starting read benchmark.")
			require.NotContains(t, res.stderr, "iter mismatch")
			require.Contains(t, res.stdout, "# Write")
			require.Contains(t, res.stdout, "# Read")
		})
	}
}
