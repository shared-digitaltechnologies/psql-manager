package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/pressly/goose/v3"
	psqlmanager "github.com/shared-digitaltechnologies/psql-manager"
	"github.com/shared-digitaltechnologies/psql-manager/db"
	psqlmigrate "github.com/shared-digitaltechnologies/psql-manager/migrate"
	"github.com/spf13/cobra"
)

type Cli struct {
	*psqlmanager.Config
	*cobra.Command

	flags struct {
		cli     cliFlags
		connect connectFlags
		seed    seedOpt
	}
}

var (
	migrateGroup = &cobra.Group{ID: "migrate", Title: "Database migration commands:"}
	seedGroup    = &cobra.Group{ID: "seed", Title: "Database seeding commands:"}
	tempGroup    = &cobra.Group{ID: "temp", Title: "Temporary database commands:"}
)

func NewCli(name string, config *psqlmanager.Config) Cli {
	if config == nil {
		config = &psqlmanager.GlobalConfig
	}

	cli := Cli{
		Config: config,
	}

	cli.flags.seed.seed = config.SeederRunner.Seed

	rootCmd := cobra.Command{
		Use:              name + " [OPTIONS] <COMMAND> [ARGS...]",
		TraverseChildren: true,
		PersistentPreRun: func(command *cobra.Command, args []string) {
			_ = config.Extend(
				cli.flags.cli.applyToConfig,
				cli.flags.connect.applyToConfig,
			)
		},
	}
	addCliFlags(rootCmd.PersistentFlags(), &cli.flags.cli)
	addConnectFlags(rootCmd.PersistentFlags(), &cli.flags.connect, cli.Config)
	rootCmd.AddGroup(
		migrateGroup,
		seedGroup,
		tempGroup,
	)

	cli.Command = &rootCmd

	// Migrate commands
	upCmd := &cobra.Command{
		Use:   "up [+DELTA|VERSION]",
		Args:  cobra.MaximumNArgs(1),
		Short: "Migrate database to a newer version",
		Long: `
Migrates the database up to some newer version.

The first argument determines the version to which the database will be migrated.
 - If the argument is '+DELTA', it will use the DELTA version later than the current version.
 - If the argument is 'VERSION' (without +), migrates to that specific version.
 - Migrates to the latest version if no argument is provided.
`,
		Aliases: []string{"u"},
		GroupID: "migrate",
		RunE: func(cmd *cobra.Command, args []string) error {
			return psqlmanager.RunMigrateAction(
				cmd.Context(),
				psqlmigrate.UpToLatestAction,
				cli.Config,
			)
		},
	}

	downCmd := &cobra.Command{
		Use:   "down [-DELTA]",
		Args:  cobra.MaximumNArgs(1),
		Short: "Migrate database to an older version",
		Long: `
Migrates the database down to version lower than the current version.
`,
		Aliases: []string{"d"},
		GroupID: "migrate",
		RunE: func(cmd *cobra.Command, args []string) error {
			return psqlmanager.RunMigrateAction(
				cmd.Context(),
				psqlmigrate.DownByAction(1),
				cli.Config,
			)
		},
	}

	redoCmd := &cobra.Command{
		Use:   "redo [AMOUNT]",
		Args:  cobra.MaximumNArgs(1),
		Short: "Re-applies the last database migration(s)",
		Long: `
Rolls back and then re-applies the last AMOUNT migrations.

Uses AMOUNT=1 if the first argument is omitted.
`,
		Aliases: []string{"r"},
		GroupID: "migrate",
		RunE: func(cmd *cobra.Command, args []string) error {
			return psqlmanager.RunMigrateAction(
				cmd.Context(),
				psqlmigrate.RedoAction(1),
				cli.Config,
			)
		},
	}

	resetCmd := &cobra.Command{
		Use:   "reset",
		Args:  cobra.ExactArgs(0),
		Short: "Rolls back all migrations",
		Long: `
Rolls back all migrations that were applied to the database.
`,
		GroupID: "migrate",
		RunE: func(cmd *cobra.Command, args []string) error {
			return psqlmanager.RunMigrateAction(
				cmd.Context(),
				psqlmigrate.ResetAction,
				cli.Config,
			)
		},
	}

	statusCmd := &cobra.Command{
		Use:     "status",
		Args:    cobra.ExactArgs(0),
		Short:   "Dumps the migration status of the database",
		GroupID: "migrate",
		RunE: func(cmd *cobra.Command, args []string) error {
			xs, err := psqlmanager.MigrationStatus(cmd.Context(), cli.Config)
			if err != nil {
				return err
			}

			for _, x := range xs {
				if x.State == goose.StateApplied {
					fmt.Printf("%05d %-60s APPLIED AT %s\n",
						x.Source.Version,
						x.Source.Path,
						x.AppliedAt.Format("2006-01-02 15:04:05"),
					)
				} else {
					fmt.Printf("%05d %-60s %s\n",
						x.Source.Version,
						x.Source.Path,
						strings.ToUpper(string(x.State)),
					)
				}
			}

			return nil
		},
	}

	migrateCmd := &cobra.Command{
		Use:     "migrate [+/-DELTA | VERSION]",
		Args:    cobra.MaximumNArgs(1),
		Short:   "Migrates the database up or down",
		Aliases: []string{"m"},
		GroupID: "migrate",
		RunE: func(cmd *cobra.Command, args []string) error {
			return psqlmanager.RunMigrateAction(
				cmd.Context(),
				psqlmigrate.MigrateToAction(0),
				cli.Config,
			)
		},
	}

	migrationsCmd := &cobra.Command{
		Use:     "migrations",
		Args:    cobra.ExactArgs(0),
		Short:   "Lists the available migrations.",
		GroupID: "migrate",
		RunE: func(cmd *cobra.Command, args []string) error {
			xs, err := psqlmanager.MigrationSources(cmd.Context(), cli.Config)
			if err != nil {
				return err
			}

			for _, x := range xs {
				if len(x.Path) == 0 {
					fmt.Printf("%05d\n", x.Version)
				} else {
					fmt.Println(x.Path)
				}
			}

			return nil
		},
	}

	// Seeding
	seedCmd := &cobra.Command{
		Use:     "seed [SEED]",
		Args:    cobra.MaximumNArgs(1),
		Short:   "Seeds the database",
		Aliases: []string{"s"},
		GroupID: "seed",
		RunE: func(cmd *cobra.Command, args []string) error {
			return psqlmanager.RunSeeders(cmd.Context(), cli.Config)
		},
	}

	seedersCmd := &cobra.Command{
		Use:     "seeders",
		Args:    cobra.ExactArgs(0),
		Short:   "Lists the currently available seeders",
		GroupID: "seed",
		Run: func(cmd *cobra.Command, args []string) {
			seeders := cli.Config.SeederRunner.Seeders()
			for _, s := range seeders {
				fmt.Println(s.Name())
			}
		},
	}

	// Temporary databases commands
	handleSeedFlag := func(cmd *cobra.Command, args []string) {
		if cli.flags.seed.enable {
			_ = cli.Config.Extend(psqlmanager.WithSeed(cli.flags.seed.seed))
		}
	}

	createCmd := &cobra.Command{
		Use:              "create [NAME][@VERSION]",
		Args:             cobra.MaximumNArgs(1),
		Short:            "Creates and initializes a new database",
		GroupID:          "temp",
		PersistentPreRun: handleSeedFlag,
		RunE: func(cmd *cobra.Command, args []string) error {

			var migrateAction psqlmigrate.MigrateAction = psqlmigrate.UpToLatestAction
			database := cli.Config.TargetDatabase()

			if len(args) > 0 {
				err := parseNameAtVersionArg(args[0], database, migrateAction)
				if err != nil {
					return err
				}
			}

			action := psqlmanager.InitDatabaseAction{
				DropIfExists: false,
				Create:       true,
				Database:     database,
				Migrate:      migrateAction,
				Seed:         cli.flags.seed.enable,
			}
			_, err := action.Run(cmd.Context(), cli.Config)
			return err
		},
	}
	addSeedFlag(createCmd.Flags(), &cli.flags.seed)

	dropCmd := &cobra.Command{
		Use:     "drop [NAME]",
		Args:    cobra.MaximumNArgs(1),
		Short:   "Drops a database if it exists",
		GroupID: "temp",
		RunE: func(cmd *cobra.Command, args []string) error {
			var database *db.Database
			if len(args) > 0 {
				database = &db.Database{
					Name: args[0],
				}
			}
			return psqlmanager.DropDatabaseIfExists(
				cmd.Context(),
				database,
				cli.Config,
			)
		},
	}

	freshCmd := &cobra.Command{
		Use:              "fresh [NAME][@VERSION]",
		Args:             cobra.MaximumNArgs(1),
		Short:            "Drops and then re-initializes the database",
		Aliases:          []string{"f"},
		GroupID:          "temp",
		PersistentPreRun: handleSeedFlag,
		RunE: func(cmd *cobra.Command, args []string) error {

			var migrateAction psqlmigrate.MigrateAction = psqlmigrate.UpToLatestAction
			database := cli.Config.TargetDatabase()

			if len(args) > 0 {
				err := parseNameAtVersionArg(args[0], database, migrateAction)
				if err != nil {
					return err
				}
			}

			action := psqlmanager.InitDatabaseAction{
				DropIfExists: true,
				Database:     database,
				Create:       true,
				Migrate:      migrateAction,
				Seed:         cli.flags.seed.enable,
			}
			_, err := action.Run(cmd.Context(), cli.Config)
			return err
		},
	}
	addSeedFlag(freshCmd.Flags(), &cli.flags.seed)
	cli.AddExecCmd()

	// Add commands to root command
	rootCmd.AddCommand(
		upCmd,
		downCmd,
		redoCmd,
		resetCmd,
		statusCmd,
		migrateCmd,
		migrationsCmd,
		seedCmd,
		seedersCmd,
		createCmd,
		dropCmd,
		freshCmd,
	)
	return cli
}

func (cli *Cli) addSeedFlagTo(cmd *cobra.Command) {
	handleSeedFlag := func(cmd *cobra.Command, args []string) {
		if cli.flags.seed.enable {
			_ = cli.Config.Extend(psqlmanager.WithSeed(cli.flags.seed.seed))
		}
	}

	if cmd.PreRun != nil {
		prevPreRun := cmd.PreRun
		cmd.PreRun = func(cmd *cobra.Command, args []string) {
			prevPreRun(cmd, args)
			handleSeedFlag(cmd, args)
		}
	} else {
		cmd.PreRun = handleSeedFlag
	}

	addSeedFlag(cmd.Flags(), &cli.flags.seed)
}

func (cli *Cli) AddExecCmd() {

	opts := psqlmanager.ExecActionOpts{}

	execCmd := &cobra.Command{
		Use:   "exec [OPTIONS] -- <COMMAND> [ARGS...]",
		Args:  cobra.MinimumNArgs(1),
		Short: "Executes a command with a temporary database",
		Long: `
Creates a new temporary database and then runs the provided COMMAND.
Drops this temporary database after the command has finished executing.

It will substitute '{...}' strings in the arguments and sets environment variables
to help COMMAND to connect to the newly created temporary database:

  - The hostname of the database server:
    * Args:   {h}, {host}
    * Env:    DB_HOST, PGHOST

  - The port number of the database server:
    * Args:   {p}, {port}
    * Env:    DB_PORT, PGPORT

  - The username to use when connecting to the temporary database:
    * Args:   {u}, {user}
    * Env:    DB_USER, PGUSER

  - The password to use when connecting to the temporary database:
    * Args:   {w}, {password}
    * Env:    DB_PASSWORD, PGPASSWORD

  - The name of the temporary database.
    * Args:   {d}, {database}, {dbname}
    * Env:    DB_DATABASE, PGDATABASE
`,
		GroupID: "temp",
		Run: func(cmd *cobra.Command, args []string) {
			action := psqlmanager.ExecAction{
				Init: psqlmanager.InitDatabaseAction{
					Create:     true,
					TempSuffix: true,
					Migrate:    psqlmigrate.UpToLatestAction,
					Seed:       cli.flags.seed.enable,
				},
				Opts: &opts,
				Path: args[0],
				Args: args[1:],
			}
			exitCode, err := action.Run(cmd.Context(), cli.Config)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
			os.Exit(exitCode)
		},
	}
	execActionFlags(execCmd.Flags(), &opts)
	cli.addSeedFlagTo(execCmd)
	cli.Command.AddCommand(execCmd)
}
