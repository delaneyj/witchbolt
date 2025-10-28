package main

import (
	"github.com/alecthomas/kong"
	"github.com/delaneyj/witchbolt/cmd/witchbolt/command"
)

func main() {
	ctx := kong.Parse(&command.CLI,
		kong.Name("witchbolt"),
		kong.Description("A simple command line tool for inspecting witchbolt databases"),
		kong.UsageOnError(),
		kong.Vars(command.KongVars()),
	)
	ctx.FatalIfErrorf(ctx.Run())
}
