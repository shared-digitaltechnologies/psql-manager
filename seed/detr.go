package psqlseed

import (
	"context"
	"path/filepath"
	"runtime"

	"github.com/google/uuid"
	"github.com/shared-digitaltechnologies/psql-manager/db"
	"github.com/shared-digitaltechnologies/psql-manager/seed/fake"
)

type DeterministicSeederFn = func(ctx context.Context, tx db.Tx) error

type deterministicSeeder struct {
	name string
	impl DeterministicSeederFn
}

func DetrFnSeeder(name string, impl DeterministicSeederFn) Seeder {
	return &deterministicSeeder{
		name: name,
		impl: impl,
	}
}

func (v *deterministicSeeder) Id() uuid.UUID {
	return uuid.NewSHA1(DeterministicSeederIdNs, []byte(v.name))
}

func (v *deterministicSeeder) Name() string {
	return v.name
}

func (v *deterministicSeeder) String() string {
	return v.name
}

func (v *deterministicSeeder) RunSeederTx(ctx context.Context, seed fake.Seed, tx db.Tx) error {
	return v.impl(ctx, tx)
}

func (v *deterministicSeeder) Prepare(ctx context.Context, seed fake.Seed) {
}

func (s *Repository) AddDetrFn(fn DeterministicSeederFn) {
	_, filename, _, _ := runtime.Caller(1)
	s.Add(DetrFnSeeder(filepath.Base(filename), fn))
}
func AddDetrFn(fn DeterministicSeederFn) {
	_, filename, _, _ := runtime.Caller(1)
	globalRepository.Add(DetrFnSeeder(filepath.Base(filename), fn))
}
