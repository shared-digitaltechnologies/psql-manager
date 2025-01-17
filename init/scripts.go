package psqlinit

import (
	"context"
	"io/fs"

	"github.com/jackc/pgx/v5"
	"github.com/shared-digitaltechnologies/psql-manager/db"
)

type InitScript interface {
	Name() string
	Apply(ctx context.Context, conn *pgx.Conn) error
}

// Init SQL
type initSql struct {
	name string
	sql  string
}

func (v *initSql) Name() string {
	return v.name
}

func (v *initSql) String() string {
	return v.name
}

func (v *initSql) Apply(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, v.sql)
	return db.ErrWithPgRowCol(err, v.name, v.sql)
}

func InitSql(name string, sql string) InitScript {
	return &initSql{
		name: name,
		sql:  sql,
	}
}

// Init SQL File
type initSqlFile struct {
	fs       fs.FS
	filename string
}

func (v *initSqlFile) Name() string {
	return v.filename
}

func (v *initSqlFile) String() string {
	return v.filename
}

func (v *initSqlFile) Apply(ctx context.Context, conn *pgx.Conn) error {
	contents, err := fs.ReadFile(v.fs, v.filename)
	if err != nil {
		return err
	}
	sql := string(contents)

	_, err = conn.Exec(ctx, sql)
	if err != nil {
		return db.ErrWithPgRowCol(err, v.filename, sql)
	}

	return nil
}

func InitSqlFile(fs fs.FS, filename string) InitScript {
	return &initSqlFile{
		fs:       fs,
		filename: filename,
	}
}

// Function
type initFn struct {
	name string
	impl func(context.Context, *pgx.Conn) error
}

func (v *initFn) Name() string {
	return v.name
}

func (v *initFn) String() string {
	return v.name
}

func (v *initFn) Apply(ctx context.Context, conn *pgx.Conn) error {
	return v.impl(ctx, conn)
}

func InitFn(name string, impl func(context.Context, *pgx.Conn) error) InitScript {
	return &initFn{
		name: name,
		impl: impl,
	}
}
