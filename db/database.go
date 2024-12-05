package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type conn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row
}

type Database struct {
	Name string
}

func (db *Database) Create(ctx context.Context, conn conn) error {
	_, err := conn.Exec(ctx, "CREATE DATABASE "+db.Name)
	if err != nil {
		return fmt.Errorf("Failed to create database \"%s\": %w", db.Name, err)
	}
	return nil
}

func (db *Database) drop(ctx context.Context, conn conn, force bool) error {
	query := "DROP DATABASE " + db.Name
	if force {
		query += " WITH (FORCE)"
	}

	_, err := conn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("Failed to drop database \"%s\": %w", db.Name, err)
	}
	return nil
}

func (db *Database) Drop(ctx context.Context, conn conn) error {
	return db.drop(ctx, conn, false)
}

func (db *Database) ForceDrop(ctx context.Context, conn conn) error {
	return db.drop(ctx, conn, true)
}

func (db *Database) Exists(ctx context.Context, conn conn) (bool, error) {
	var res bool
	err := conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT * FROM pg_catalog.pg_database WHERE datname = $1)",
		db.Name,
	).Scan(&res)

	if err != nil {
		return res, fmt.Errorf("Failed to check if database \"%s\" exists: %w", db.Name, err)
	}

	return res, nil
}
