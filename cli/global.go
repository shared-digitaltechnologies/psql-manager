package cli

import (
	"context"
	"fmt"
	"os"

	psqlmanager "github.com/shared-digitaltechnologies/psql-manager"
)

var defaultName string = "mypg"

func SetName(name string) {
	defaultName = name
}

var globalCli *Cli

func cli() *Cli {
	if globalCli == nil {
		cli := NewCli(defaultName, &psqlmanager.GlobalConfig)
		globalCli = &cli
	}
	return globalCli
}

func Execute() error {
	return cli().Execute()
}

func ExecuteContext(context context.Context) error {
	return cli().ExecuteContext(context)
}

func ExecuteAndExit() {
	err := Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	os.Exit(0)
}

func ExecuteAndExitContext(context context.Context) {
	err := Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	os.Exit(0)
}
