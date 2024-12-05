package psqlmigrate

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

type ProviderFactory struct {
	ProviderOptions []goose.ProviderOption
	MigrationsFsys  fs.FS
}

var globalProviderFactory ProviderFactory

func (r *ProviderFactory) Copy() *ProviderFactory {
	if r == nil {
		r = &globalProviderFactory
	}

	options := make([]goose.ProviderOption, len(r.ProviderOptions))
	copy(options, r.ProviderOptions)
	return &ProviderFactory{
		ProviderOptions: options,
		MigrationsFsys:  r.MigrationsFsys,
	}
}

func (r *ProviderFactory) PrependGooseOptions(options ...goose.ProviderOption) {
	if r == nil {
		r = &globalProviderFactory
	}

	r.ProviderOptions = append(options, r.ProviderOptions...)
}

func (r *ProviderFactory) AppendGooseOptions(options ...goose.ProviderOption) {
	if r == nil {
		r = &globalProviderFactory
	}

	r.ProviderOptions = append(r.ProviderOptions, options...)
}

func AddGooseProviderOptions(options ...goose.ProviderOption) {
	globalProviderFactory.AppendGooseOptions(options...)
}

func (r *ProviderFactory) SetMigrationsDir(fsys fs.FS, dirpath ...string) error {
	if r == nil {
		r = &globalProviderFactory
	}

	if len(dirpath) > 0 {
		dir := filepath.Join(dirpath...)

		var err error
		fsys, err = fs.Sub(fsys, dir)
		if err != nil {
			return fmt.Errorf("Failed to create subfilesystem for dir '%s': %w", dir, err)
		}
	}

	r.MigrationsFsys = fsys
	return nil
}

func SetMigrationsDir(fsys fs.FS, dirpath ...string) error {
	return globalProviderFactory.SetMigrationsDir(fsys, dirpath...)
}

func (r *ProviderFactory) OpenProvider(ctx context.Context, connConfig *pgx.ConnConfig) (provider *goose.Provider, err error) {
	if r == nil {
		r = &globalProviderFactory
	}

	k := len(r.ProviderOptions)
	options := make([]goose.ProviderOption, k)
	copy(options, r.ProviderOptions)

	db := stdlib.OpenDB(*connConfig)

	provider, err = goose.NewProvider(goose.DialectPostgres, db, r.MigrationsFsys, options...)
	if err != nil {
		return provider, fmt.Errorf("Error creating goose Provider for MigrationRunner: %v", err)
	}

	return provider, nil
}

func (r *ProviderFactory) OpenRunner(ctx context.Context, connConfig *pgx.ConnConfig) (runner *Runner, err error) {
	if r == nil {
		r = &globalProviderFactory
	}

	provider, err := r.OpenProvider(ctx, connConfig)
	if err != nil {
		return nil, err
	}

	return &Runner{provider}, nil
}

func LogMigrationResults(results ...*goose.MigrationResult) {
	for _, res := range results {
		fmt.Printf("    %s\n", res)
	}
}
