package command_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/alecthomas/kong"

	"github.com/delaneyj/witchbolt/cmd/witchbolt/command"
)

type cliResult struct {
	stdout string
	stderr string
	err    error
}

func runCLI(t *testing.T, args ...string) (result cliResult) {
	t.Helper()

	outReader, outWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create stdout pipe: %v", err)
	}
	errReader, errWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create stderr pipe: %v", err)
	}

	originalStdout := os.Stdout
	originalStderr := os.Stderr
	os.Stdout = outWriter
	os.Stderr = errWriter

	defer func() {
		outWriter.Close()
		errWriter.Close()
		os.Stdout = originalStdout
		os.Stderr = originalStderr
		var outBuf bytes.Buffer
		var errBuf bytes.Buffer
		_, _ = io.Copy(&outBuf, outReader)
		_, _ = io.Copy(&errBuf, errReader)
		result.stdout = outBuf.String()
		result.stderr = errBuf.String()
		outReader.Close()
		errReader.Close()
	}()

	cli := command.CLI
	parser, err := kong.New(&cli,
		kong.Name("witchbolt"),
		kong.Description("A simple command line tool for inspecting witchbolt databases"),
		kong.UsageOnError(),
		kong.Vars(command.KongVars()),
	)
	if err != nil {
		result.err = err
		return result
	}

	ctx, parseErr := parser.Parse(args)
	if parseErr != nil {
		result.err = parseErr
		return result
	}

	result.err = ctx.Run()

	return result
}
