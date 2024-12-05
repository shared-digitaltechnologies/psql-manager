package psqlmanager

import (
	"context"
	"fmt"
	"math/rand/v2"

	"github.com/jackc/pgx/v5"
	"github.com/shared-digitaltechnologies/psql-manager/db"
	psqlmigrate "github.com/shared-digitaltechnologies/psql-manager/migrate"
)

type InitDatabaseAction struct {
	*db.Database
	DropIfExists bool
	Create       bool
	Migrate      psqlmigrate.MigrateAction
	Seed         bool
	TempSuffix   bool
}

var letters = []rune("abcdefghijklmnopqrstuvwxyz")

func createRandomSuffix(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return string(b)
}

func (a *InitDatabaseAction) Run(ctx context.Context, config *Config) (*db.Database, error) {
	database := a.Database
	if database == nil {
		database = config.TargetDatabase()
	}

	rootConn, err := ConnectRootDB(ctx, config)
	if err != nil {
		return database, err
	}
	defer rootConn.Close(ctx)

	return a.RunWithRootConn(ctx, rootConn, config)
}

func (a *InitDatabaseAction) RunWithRootConn(ctx context.Context, rootConn *pgx.Conn, config *Config) (*db.Database, error) {
	if config == nil {
		config = &GlobalConfig
	}

	if a.Database == nil {
		a.Database = config.TargetDatabase()
	}

	if a.TempSuffix {
		a.Database.Name += "_" + createRandomSuffix(8)
	}

	database := a.Database
	dbName := database.Name

	// Drop if exists
	if a.DropIfExists {
		_, err := dropDatabaseIfExists(ctx, rootConn, database, config)
		if err != nil {
			return database, fmt.Errorf("Failed InitDatabaseAction \"%s\": DropIfExists: %w", dbName, err)
		}
	}

	// Create
	success := false
	if a.Create {
		err := database.Create(ctx, rootConn)
		if err != nil {
			return database, fmt.Errorf("Failed InitDatabaseAction \"%s\": Create: %w", dbName, err)
		}
		defer func() {
			if !success {
				err := a.Database.ForceDrop(ctx, rootConn)
				if err != nil {
					fmt.Printf("\n\nWARNING! Failed to drop database \"%s\". You need to clean up by hand!\n   ERR: %v\n\n", a.Database.Name, err)
				}
			}
		}()

		fmt.Printf(">> Created database \"%s\".\n", dbName)
	}

	// Connect
	connConfig := rootConn.Config()
	connConfig.Database = dbName
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return database, fmt.Errorf("Failed InitDatabaseAction \"%s\": Failed to connect: %w", dbName, err)
	}
	defer conn.Close(ctx)

	// Init
	fmt.Println(">> INITIALIZE DATABASE")
	if err := config.InitRunner.Run(ctx, conn); err != nil {
		return database, fmt.Errorf("Failed InitDatabaseAction \"%s\": Init: %w", dbName, err)
	}

	// Migrate
	if a.Migrate != nil {
		if err := RunMigrateActionInDatabase(ctx, a.Migrate, database, config); err != nil {
			return database, fmt.Errorf("Failed InitDatabaseAction \"%s\": Migrate: %w", dbName, err)
		}
	}

	// Seed
	if a.Seed {
		if err := RunSeedersWithConn(ctx, conn, config); err != nil {
			return database, fmt.Errorf("Failed InitDatabaseAction \"%s\": Seed: %w", dbName, err)
		}
	}

	success = true
	return database, nil
}
