package psqlseed

import (
	"context"

	"github.com/google/uuid"
	"github.com/shared-digitaltechnologies/psql-manager/db"
	"github.com/shared-digitaltechnologies/psql-manager/seed/fake"
)

type Seeder interface {
	Id() uuid.UUID
	Name() string
	Prepare(context.Context, fake.Seed)
	RunSeederTx(context.Context, fake.Seed, db.Tx) error
}

var (
	RandSeederIdNs                 uuid.UUID = uuid.MustParse("8b8f98b6-eb5c-44e0-a68e-cdcc22e1c366")
	RandSeederRunIdNs              uuid.UUID = uuid.MustParse("7d894063-16a6-416a-a822-1bb0ca4bfe72")
	RandSeederSqlIdNs              uuid.UUID = uuid.MustParse("7a6c8eec-49bb-4186-add8-7c079f936748")
	RandSeederSqlFileIdNs          uuid.UUID = uuid.MustParse("251059bc-86d1-4e6e-b38e-bf89dc0e2b3d")
	RandSeederSqlRunIdNs           uuid.UUID = uuid.MustParse("013f7435-9b2c-4119-a9b2-5ef57eeb79bc")
	DeterministicSeederIdNs        uuid.UUID = uuid.MustParse("ebbef853-5d33-4089-b57c-4704337aa1b9")
	DeterministicSeederRunIdNs     uuid.UUID = uuid.MustParse("764240c0-abcf-44aa-bf29-61c33851a77b")
	DeterministicFileSeederIdNs    uuid.UUID = uuid.MustParse("9cec5e06-87f3-408f-8c03-1ae78128b132")
	DeterministicFileSeederRunIdNs uuid.UUID = uuid.MustParse("31e1534a-5e76-4298-86aa-e6ecda64e499")
)
