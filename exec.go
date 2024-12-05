package psqlmanager

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/shared-digitaltechnologies/psql-manager/db"
)

type ExecActionOpts struct {
	Keep             bool
	KeepAfterSuccess bool
	KeepAfterFailure bool

	Conn         ConnStringExtend
	NoInheritEnv bool
	Env          []string
}

type ExecAction struct {
	Init InitDatabaseAction
	Opts *ExecActionOpts
	Path string
	Args []string
}

type ConnStringExtend struct{ Parts []string }

func (c *ConnStringExtend) String() string {
	return strings.Join(c.Parts, " ")
}

func (c *ConnStringExtend) Type() string {
	return "kvPairs"
}

func (c *ConnStringExtend) Set(val string) error {
	c.Parts = append(c.Parts, val)
	return nil
}

func (a *ExecAction) cmd(ctx context.Context, database *db.Database, config *Config) *exec.Cmd {

	connstr := config.ConnString.Copy()
	connstr.Set("database", database.Name)
	for _, v := range a.Opts.Conn.Parts {
		fmt.Println("CONN", v)
		connstr.LoadConnString(v)
	}

	args := make([]string, len(a.Args))
	for i, arg := range a.Args {
		args[i] = connstr.Substitute(arg)
	}

	cmd := exec.CommandContext(ctx, a.Path, args...)
	if !a.Opts.NoInheritEnv {
		cmd.Env = append(cmd.Env, os.Environ()...)
	}
	cmd.Env = append(cmd.Env, connstr.Env()...)
	cmd.Env = append(cmd.Env, a.Opts.Env...)

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	cmd.Stdin = os.Stdin

	return cmd
}

func (a *ExecAction) RunWithRootConn(ctx context.Context, rootConn *pgx.Conn, config *Config) (int, error) {
	if config == nil {
		config = &GlobalConfig
	}

	execCtx, done := context.WithCancelCause(ctx)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGBUS,
	)

	var signal os.Signal
	go func() {
		s := <-sigc
		signal = s
		done(fmt.Errorf("%v", s))
	}()

	// Setup
	database, err := a.Init.RunWithRootConn(execCtx, rootConn, config)
	if err != nil {
		return 1, err
	}

	success := false
	defer func() {
		keep := a.Opts.Keep || (a.Opts.KeepAfterSuccess && success) || (a.Opts.KeepAfterFailure && !success)
		if !keep {
			_, err := dropDatabaseIfExists(ctx, rootConn, database, config)
			if err != nil {
				fmt.Printf("\n\nWARNING! Failed to drop database \"%s\". You need to clean up by hand!\n   ERR: %v\n\n", database.Name, err)
			}
		}
	}()

	cmd := a.cmd(ctx, database, config)
	cmd.Cancel = func() error {
		if signal == nil {
			return cmd.Process.Kill()
		} else {
			return cmd.Process.Signal(signal)
		}
	}

	if err := cmd.Run(); err != nil {
		return cmd.ProcessState.ExitCode(), err
	}

	exitCode := cmd.ProcessState.ExitCode()
	if exitCode == 0 {
		success = true
	}

	return exitCode, nil
}

func (a *ExecAction) Run(ctx context.Context, config *Config) (int, error) {
	rootConn, err := ConnectRootDB(ctx, config)
	if err != nil {
		return 1, err
	}
	defer rootConn.Close(ctx)

	return a.RunWithRootConn(ctx, rootConn, config)
}
