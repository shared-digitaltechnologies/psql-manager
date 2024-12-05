package cli

import (
	"strconv"
	"strings"

	psqlmanager "github.com/shared-digitaltechnologies/psql-manager"
	"github.com/shared-digitaltechnologies/psql-manager/seed/fake"
	"github.com/spf13/pflag"
)

type cliFlags struct {
	color   psqlmanager.LogColorMode
	quiet   bool
	verbose bool
}

func addCliFlags(flags *pflag.FlagSet, target *cliFlags) {
	flags.Var(&target.color, "color", "Log using colors")
	flags.BoolVarP(&target.quiet, "quiet", "q", target.quiet, "Minimize logs")
	flags.BoolVarP(&target.verbose, "verbose", "v", target.verbose, "Enable verbose logs")
}

func (flags *cliFlags) applyToConfig(c *psqlmanager.Config) error {
	return nil
}

type connectFlags struct {
	conn   psqlmanager.ConnStringExtend
	dbname string
}

func (flags *connectFlags) applyToConfig(c *psqlmanager.Config) error {
	for _, conn := range flags.conn.Parts {
		err := c.ConnString.LoadConnString(conn)
		if err != nil {
			return err
		}
	}

	if len(flags.dbname) > 0 {
		c.Extend(psqlmanager.WithTargetDBName(flags.dbname))
	}

	return nil
}

func addConnectFlags(flags *pflag.FlagSet, target *connectFlags, config *psqlmanager.Config) {
	flags.VarP(&target.conn, "conn", "c", "Connection parameters for the root database")
	flags.StringVarP(&target.dbname, "database", "d", config.TargetDatabase().Name, "Name of the target database")
}

func execActionFlags(flags *pflag.FlagSet, target *psqlmanager.ExecActionOpts) {
	flags.BoolVar(&target.Keep, "keep", target.Keep, "Do not drop the temporary database afterwards.")
	flags.BoolVar(&target.KeepAfterSuccess, "keep-after-success", target.KeepAfterSuccess, "Do not drop temp database if exit code is 0.")
	flags.BoolVar(&target.KeepAfterFailure, "keep-after-failure", target.KeepAfterFailure, "Do not drop temp database if exit code is non-zero.")
	flags.VarP(&target.Conn, "exec-conn", "o", "Override database connection variables for command.")
	flags.StringSliceVarP(&target.Env, "exec-env", "e", target.Env, "Set env variables [KEY=VAL] for exec command only.")
	flags.BoolVar(&target.NoInheritEnv, "no-inherit-env", target.NoInheritEnv, "Do not inherit the env-variables of this command.")
}

func addSeedFlag(flags *pflag.FlagSet, target *seedOpt) {
	flags.VarP(target, "seed", "s", "Also seed the database.")
	flag := flags.Lookup("seed")
	if target.enable {
		flag.DefValue = target.seed.String()
	} else {
		flag.DefValue = "false"
	}
	flag.NoOptDefVal = "0x00"
}

type seedOpt struct {
	enable bool
	seed   fake.Seed
}

func (o *seedOpt) Set(val string) error {
	val = strings.ToLower(val)

	intVal, err := strconv.ParseUint(val, 0, 64)
	if err != nil {
		boolVal, boolErr := strconv.ParseBool(val)
		if boolErr != nil {
			return err
		}
		o.enable = boolVal
		return nil
	}

	o.enable = true
	o.seed = fake.Seed(intVal)
	return nil
}

func (o *seedOpt) String() string {
	if !o.enable {
		return "false"
	} else {
		return o.seed.String()
	}
}

func (o *seedOpt) Type() string {
	return "seed"
}
