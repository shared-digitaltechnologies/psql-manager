package psqlseed

import (
	"context"
	"io/fs"

	"github.com/google/uuid"
	"github.com/shared-digitaltechnologies/psql-manager/db"
	"github.com/shared-digitaltechnologies/psql-manager/seed/fake"
)

type deterministicSqlSeeder struct {
	name string
	sql  string
}

func DetrSqlSeeder(name string, sql string) Seeder {
	return &deterministicSqlSeeder{
		name: name,
		sql:  sql,
	}
}

func (v *deterministicSqlSeeder) Id() uuid.UUID {
	return uuid.NewSHA1(DeterministicSeederIdNs, []byte(v.name))
}

func (v *deterministicSqlSeeder) Name() string {
	return v.name
}

func (v *deterministicSqlSeeder) String() string {
	return v.name
}

func (v *deterministicSqlSeeder) RunSeederTx(ctx context.Context, seed fake.Seed, tx db.Tx) error {
	_, err := tx.Exec(ctx, v.sql)
	return db.ErrWithPgRowCol(err, v.name, v.sql)
}

func (v *deterministicSqlSeeder) IsDeterministic() bool {
	return true
}

func (v *deterministicSqlSeeder) Prepare(ctx context.Context, seed fake.Seed) {
}

type detrSqlFileSeeder struct {
	fsys     fs.FS
	filename string
}

func (v *detrSqlFileSeeder) Id() uuid.UUID {
	return uuid.NewSHA1(DeterministicSeederIdNs, []byte(v.filename))
}

func (v *detrSqlFileSeeder) Name() string {
	return v.filename
}

func (v *detrSqlFileSeeder) String() string {
	return v.filename
}

func (v *detrSqlFileSeeder) Prepare(ctx context.Context, seed fake.Seed) {
}

func (v *detrSqlFileSeeder) RunSeederTx(ctx context.Context, seed fake.Seed, tx db.Tx) error {
	content, err := fs.ReadFile(v.fsys, v.filename)
	sql := string(content)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, sql)
	return db.ErrWithPgRowCol(err, v.filename, sql)
}

func (v *detrSqlFileSeeder) IsDeterministic() bool {
	return true
}

func DetrSqlFileSeeder(fsys fs.FS, filename string) Seeder {
	return &detrSqlFileSeeder{fsys, filename}
}
