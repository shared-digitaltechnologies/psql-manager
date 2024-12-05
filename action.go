package psqlmanager

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3"
	"github.com/shared-digitaltechnologies/psql-manager/db"
	psqlmigrate "github.com/shared-digitaltechnologies/psql-manager/migrate"
)

func RunMigrateActionInDatabase(ctx context.Context, action psqlmigrate.MigrateAction, database *db.Database, config *Config) error {
	if config == nil {
		config = &GlobalConfig
	}

	if database == nil {
		database = config.TargetDatabase()
	}

	connConfig, err := config.RootConnConfig()
	connConfig.Database = database.Name
	if err != nil {
		return err
	}

	runner, err := config.migrationProviderFactory.OpenRunner(ctx, connConfig)
	if err != nil {
		return err
	}
	defer runner.Close()

	result, err := runner.Run(ctx, action)
	fmt.Println(result.String())
	return err
}

func RunMigrateAction(ctx context.Context, action psqlmigrate.MigrateAction, config *Config) error {
	return RunMigrateActionInDatabase(ctx, action, nil, config)
}

func RunSeedersWithConn(ctx context.Context, conn *pgx.Conn, config *Config) error {
	if config == nil {
		config = &GlobalConfig
	}

	fmt.Printf(">> RUN SEEDERS seed=%s\n", config.SeederRunner.Seed)
	return config.SeederRunner.Run(ctx, conn)
}

func RunSeeders(ctx context.Context, config *Config) error {
	conn, err := ConnectTarget(ctx, config)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	return RunSeedersWithConn(ctx, conn, config)
}

func RunSeedersInDatabase(ctx context.Context, database *db.Database, config *Config) error {
	conn, err := ConnectDatabase(ctx, database, config)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	return RunSeedersWithConn(ctx, conn, config)
}

func MigrationStatus(ctx context.Context, config *Config) ([]*goose.MigrationStatus, error) {
	if config == nil {
		config = &GlobalConfig
	}

	connConfig, err := config.TargetConnConfig()
	if err != nil {
		return nil, err
	}

	provider, err := config.migrationProviderFactory.OpenProvider(ctx, connConfig)
	if err != nil {
		return nil, err
	}
	defer provider.Close()

	return provider.Status(ctx)
}

func MigrationSources(ctx context.Context, config *Config) ([]*goose.Source, error) {
	if config == nil {
		config = &GlobalConfig
	}

	connConfig, err := config.TargetConnConfig()
	if err != nil {
		return nil, err
	}

	provider, err := config.migrationProviderFactory.OpenProvider(ctx, connConfig)
	if err != nil {
		return nil, err
	}
	defer provider.Close()

	return provider.ListSources(), nil
}

func dropDatabaseIfExists(ctx context.Context, rootConn *pgx.Conn, database *db.Database, config *Config) (bool, error) {
	if database == nil {
		database = config.TargetDatabase()
	}

	if rootConn.Config().Database == database.Name {
		return true, fmt.Errorf("Cannot drop root database \"%s\"", database.Name)
	}

	exists, err := database.Exists(ctx, rootConn)
	if err != nil || !exists {
		return exists, err
	}

	if err := database.ForceDrop(ctx, rootConn); err != nil {
		return true, err
	}

	fmt.Printf(">> Dropped Database \"%s\"\n", database.Name)
	return true, nil
}

func DropDatabaseIfExists(ctx context.Context, database *db.Database, config *Config) error {
	if database == nil {
		database = config.TargetDatabase()
	}

	rootConn, err := ConnectRootDB(ctx, config)
	if err != nil {
		return err
	}
	defer rootConn.Close(ctx)

	existed, err := dropDatabaseIfExists(ctx, rootConn, database, config)
	if err != nil {
		return err
	}

	if !existed {
		fmt.Printf(">> SKIP Drop Database \"%s\" (does not exist...)\n", database.Name)
	}

	return nil
}
