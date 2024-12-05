package main

import (
	"fmt"
	"os"

	p "github.com/shared-digitaltechnologies/psql-manager"
	pcli "github.com/shared-digitaltechnologies/psql-manager/cli"
)

func main() {
	config, err := p.NewConfig()
	if err != nil {
		panic(err)
	}

	cli := pcli.NewCli("minimalpq", config)

	if err := cli.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

}
