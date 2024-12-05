package psqlseed

import (
	"context"
	"hash/fnv"
	"path/filepath"
	"runtime"

	"github.com/google/uuid"
	"github.com/shared-digitaltechnologies/psql-manager/db"
	"github.com/shared-digitaltechnologies/psql-manager/seed/fake"
)

type RandSeederFn = func(ctx context.Context, tx db.Tx, faker fake.Faker) error

type randSeeder struct {
	name       string
	seedOffset uint64
	impl       RandSeederFn
}

func RandFnSeeder(name string, impl RandSeederFn) Seeder {
	hash := fnv.New64()
	hash.Write([]byte(name))
	seedOffset := hash.Sum64()

	return &randSeeder{
		name:       name,
		seedOffset: seedOffset,
		impl:       impl,
	}
}

func (v *randSeeder) Id() uuid.UUID {
	return uuid.NewSHA1(RandSeederIdNs, []byte(v.name))
}

func (v *randSeeder) Name() string {
	return v.name
}

func (v *randSeeder) String() string {
	return v.name
}

func (v *randSeeder) Prepare(ctx context.Context, seed fake.Seed) {
}

func (v *randSeeder) RunSeederTx(ctx context.Context, seed fake.Seed, tx db.Tx) error {
	return v.impl(ctx, tx, seed.NewFakerOffset(v.seedOffset))
}

func (v *randSeeder) IsDeterministic() bool {
	return false
}

func (s *Repository) AddRandFn(impl RandSeederFn) {
	_, filename, _, _ := runtime.Caller(1)
	s.Add(RandFnSeeder(filepath.Base(filename), impl))
}

func AddRandFn(impl RandSeederFn) {
	_, filename, _, _ := runtime.Caller(1)
	globalRepository.Add(RandFnSeeder(filepath.Base(filename), impl))
}
