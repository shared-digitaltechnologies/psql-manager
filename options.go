package psqlmanager

import (
	"errors"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"
	psqlinit "github.com/shared-digitaltechnologies/psql-manager/init"
	psqlseed "github.com/shared-digitaltechnologies/psql-manager/seed"
	"github.com/shared-digitaltechnologies/psql-manager/seed/fake"
)

type ConfigOption = func(*Config) error

func (c *Config) extend(options ...ConfigOption) error {
	optionErrors := make([]error, len(options))
	for i, option := range options {
		optionErrors[i] = option(c)
	}
	return errors.Join(optionErrors...)
}

func (c *Config) Extend(options ...ConfigOption) error {
	return c.extend(options...)
}

func Extend(options ...ConfigOption) error {
	return GlobalConfig.extend(options...)
}

func LoadConnEnvVariables() {
	GlobalConfig.ConnString.LoadEnvSettings()
}

func LoadDefaultUserSettings() {
	GlobalConfig.ConnString.LoadDefaultUserSettings()
}

func LoadConnSettings(values map[string]string) {
	GlobalConfig.ConnString.LoadSettings(values)
}

func SetConnSetting(key string, value string) {
	GlobalConfig.ConnString.Set(key, value)
}

func SetTargetDatabaseName(dbname string) {
	GlobalConfig.DatabaseName = dbname
}

func (o *Config) ensureOwnsCurrentInitRepository() {
	if !o.ownsCurrentInitRepository {
		newRepo := o.InitRunner.Repository.Copy()
		o.InitRunner.Repository = newRepo
		o.ownsCurrentInitRepository = true
	}
}

// CONNECT //

// WithRootDbConnString sets the connection string to connect to the
// PostgreSQL server. The database name should be a database that is always
// available on the server.
func WithConnString(connStr string) ConfigOption {
	return func(o *Config) error {
		o.ConnString.LoadConnString(connStr)
		return nil
	}
}

func WithConnSetting(key string, value string) ConfigOption {
	return func(o *Config) error {
		o.ConnString.Set(key, value)
		return nil
	}
}

func WithConnSettings(values map[string]string) ConfigOption {
	return func(o *Config) error {
		o.ConnString.LoadSettings(values)
		return nil
	}
}

// WithTargetDbName sets the name of the database in which the init scripts,
// seeders and migrations are executed.
func WithTargetDBName(dbname string) ConfigOption {
	return func(o *Config) error {
		o.DatabaseName = dbname
		return nil
	}
}

// INIT //

// WithInitRepository sets the used init repository. You can set
// this value to `nil` to use the global init repository.
func WithInitRepository(repository *psqlinit.Repository) ConfigOption {
	return func(o *Config) error {
		o.InitRunner.Repository = repository
		o.ownsCurrentInitRepository = false
		return nil
	}
}

// WithExtraInit adds additional init scripts without changing the
// global init repository.
func WithExtraInit(script psqlinit.InitScript, skipWhen ...psqlinit.Condition) ConfigOption {
	return func(o *Config) error {
		o.ensureOwnsCurrentInitRepository()
		o.InitRunner.Repository.Add(script, skipWhen...)
		return nil
	}
}

// WithIgnoreInitConditions sets whether all init conditions should be
// ignored (and thus, whether each init scripts should always run, regardless
// of the skipWhen conditions defined for the init step).
func WithIgnoreInitConditions(value bool) ConfigOption {
	return func(o *Config) error {
		o.InitRunner.IgnoreConditions = value
		return nil
	}
}

// SEEDERS //

func (c *Config) ensureOwnsCurrentSeederRepository() {
	if !c.ownsCurrentSeederRepository {
		newRepo := c.SeederRunner.Repository.Copy()
		c.SeederRunner.Repository = newRepo
		c.ownsCurrentSeederRepository = true
	}
}

// WithSeed sets the seed that is used by the seeder operations.
//
// Defaults to the 0 seed.
func WithSeed(seed fake.Seed) ConfigOption {
	return func(o *Config) error {
		o.SeederRunner.Seed = seed
		return nil
	}
}

// WithSeederRepository sets the used seeder repository. You can set
// this value to `nil` to use the global seeder repository.
//
// This option will override any preceding options that configure the
// seeder repository, like WithSeeders, WithOnlySeedersNamed and WithOnlySeedersTill.
func WithSeederRepository(repository *psqlseed.Repository) ConfigOption {
	return func(o *Config) error {
		o.SeederRunner.Repository = repository
		o.ownsCurrentSeederRepository = false
		return nil
	}
}

// WithExtraSeeders adds additional seeders to the internal seeder repository
// without changing the global seeder repository.
func WithExtraSeeders(seeders ...psqlseed.Seeder) ConfigOption {
	return func(o *Config) error {
		o.ensureOwnsCurrentSeederRepository()
		o.SeederRunner.Repository.Add(seeders...)
		return nil
	}
}

// WithOnlySeedersNamed limits the available seeders with the exact
// provided names.
func WithOnlySeedersNamed(seederNames ...string) ConfigOption {
	return func(o *Config) error {
		err := o.SeederRunner.SeedersNamed(seederNames...)
		o.ownsCurrentSeederRepository = true
		if err != nil {
			return fmt.Errorf("psqlmanager configuration[WithOnlySeedersNamed]: %w", err)
		}
		return nil
	}
}

// WithOnlySeedersTill limits the available seeders till.
func WithOnlySeedersTill(seederNamePrefix string) ConfigOption {
	return func(o *Config) error {
		o.SeederRunner.SeedersTill(seederNamePrefix)
		o.ownsCurrentSeederRepository = true
		return nil
	}
}

// WithBailOnSeederError sets whether seeding should immediately stop
// after one of the seeders returned a non-nil error.
//
// Defaults to `false`.
func WithBailOnSeederError(value bool) ConfigOption {
	return func(o *Config) error {
		o.SeederRunner.BailOnError = value
		return nil
	}
}

// MIGRATIONS //

// WithMigrationsDir sets the directory where the sql goose migrations
// can be found.
func WithMigrationsDir(fsys fs.FS, dirpath ...string) ConfigOption {
	return func(o *Config) error {
		if o.migrationProviderFactory == nil {
			o.migrationProviderFactory = o.migrationProviderFactory.Copy()
		}

		o.migrationProviderFactory.SetMigrationsDir(fsys, dirpath...)
		return nil
	}
}

// WithGooseProviderOptions appends extra provider options to the
// goose provider. These will also override any goose options that are
// derived from the other options.
func WithGooseProviderOptions(options ...goose.ProviderOption) ConfigOption {
	return func(o *Config) error {
		if o.migrationProviderFactory == nil {
			o.migrationProviderFactory = o.migrationProviderFactory.Copy()
		}

		o.migrationProviderFactory.AppendGooseOptions(options...)
		return nil
	}
}
