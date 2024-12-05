package psqlmanager

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3"
	"github.com/shared-digitaltechnologies/psql-manager/db"
)

func (c *Config) RootConnConfig() (*pgx.ConnConfig, error) {
	if c == nil {
		c = &GlobalConfig
	}
	return pgx.ParseConfig(c.ConnString.StringKeywordValue())
}

func ConnectRootDB(ctx context.Context, config *Config) (*pgx.Conn, error) {
	if config == nil {
		config = &GlobalConfig
	}

	connConfig, err := config.RootConnConfig()
	if err != nil {
		return nil, err
	}

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("ConnectRootDB failed: %w", err)
	}
	return conn, err
}

func (c *Config) TargetConnConfig() (*pgx.ConnConfig, error) {
	if c == nil {
		c = &GlobalConfig
	}

	rootConnConfig, err := c.RootConnConfig()
	if err != nil {
		return nil, err
	}

	targetConnConfig := rootConnConfig.Copy()
	if len(c.DatabaseName) > 0 {
		targetConnConfig.Database = c.DatabaseName
	}

	return targetConnConfig, nil
}

func connectTarget(
	ctx context.Context,
	database *db.Database,
	config *Config,
) (*pgx.Conn, error) {
	var connConfig *pgx.ConnConfig
	var err error

	if database == nil {
		database = config.TargetDatabase()
	}

	connConfig, err = config.RootConnConfig()
	connConfig.Database = database.Name
	if err != nil {
		return nil, err
	}

	return pgx.ConnectConfig(ctx, connConfig)
}

func ConnectTarget(ctx context.Context, config *Config) (*pgx.Conn, error) {
	if config == nil {
		config = &GlobalConfig
	}
	return connectTarget(ctx, nil, config)
}

func ConnectDatabase(ctx context.Context, db *db.Database, config *Config) (*pgx.Conn, error) {
	if config == nil {
		config = &GlobalConfig
	}
	return connectTarget(ctx, db, config)
}

func OpenGooseProvider(ctx context.Context, config *Config) (*goose.Provider, error) {
	if config == nil {
		config = &GlobalConfig
	}

	connConfig, err := config.TargetConnConfig()
	if err != nil {
		return nil, err
	}

	return config.migrationProviderFactory.OpenProvider(ctx, connConfig)
}
