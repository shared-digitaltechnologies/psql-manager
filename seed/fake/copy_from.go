package fake

import "github.com/jackc/pgx/v5"

type copyFromFakeSourceGen = func(Faker, int) ([]any, error)

type copyFromFakeSource struct {
	faker Faker
	seed  Seed
	gen   copyFromFakeSourceGen
	idx   int
	len   int
	err   error
}

func (cts *copyFromFakeSource) Next() bool {
	cts.idx++
	return cts.idx < cts.len
}

func (cts *copyFromFakeSource) Values() (values []any, err error) {
	cts.faker.SetSeedOffset(cts.seed, uint64(cts.idx))
	values, err = cts.gen(cts.faker, cts.idx)
	if err != nil {
		cts.err = err
	}
	return
}
func (cts *copyFromFakeSource) Err() error {
	return cts.err
}

func (f Faker) CopyFromSource(count int, gen copyFromFakeSourceGen) pgx.CopyFromSource {
	seed := f.NewSeed()
	return &copyFromFakeSource{
		faker: seed.NewFakerOffset(0),
		seed:  seed,
		gen:   gen,
		idx:   -1,
		len:   count,
	}
}

type copyFromFakeSourceConcatGen = func(Faker, int) ([][]any, error)

type copyFromFakerSourceConcat struct {
	faker Faker
	seed  Seed
	gen   copyFromFakeSourceConcatGen
	rows  [][]any
	idx   int
	j     int
	len   int
	err   error
}

func (cts *copyFromFakerSourceConcat) Next() bool {
	for cts.idx < cts.len {
		cts.j++
		if cts.j < len(cts.rows) {
			return true
		}

		cts.idx++
		if cts.idx >= cts.len {
			return false
		}

		cts.faker.SetSeedOffset(cts.seed, uint64(cts.idx))
		rows, err := cts.gen(cts.faker, cts.idx)
		cts.j = -1
		cts.rows = rows
		cts.err = err
	}
	return false
}

func (cts *copyFromFakerSourceConcat) Values() (values []any, err error) {
	return cts.rows[cts.j], cts.err
}

func (cts *copyFromFakerSourceConcat) Err() error {
	return cts.err
}

func (f Faker) CopyFromSourceConcat(count int, gen copyFromFakeSourceConcatGen) pgx.CopyFromSource {
	seed := f.NewSeed()
	return &copyFromFakerSourceConcat{
		faker: seed.NewFakerOffset(0),
		seed:  seed,
		gen:   gen,
		idx:   -1,
		j:     -1,
		len:   count,
	}
}
