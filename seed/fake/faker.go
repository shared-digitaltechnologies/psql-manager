package fake

import (
	"math/rand/v2"

	"github.com/brianvoe/gofakeit/v7"
)

type Faker struct {
	/**
	 * The wrapped gofakeit faker.
	 */
	*gofakeit.Faker
}

func (f Faker) NewPCG() *rand.PCG {
	return rand.NewPCG(f.Rand.Uint64(), f.Rand.Uint64())
}

func (f Faker) NewRand() *rand.Rand {
	return rand.New(f.NewPCG())
}

func (f Faker) Split() Faker {
	return Faker{gofakeit.NewFaker(f.NewPCG(), false)}
}

func (f Faker) SplitN(count int) []Faker {
	baseSeed := f.Rand.Uint64()
	fakers := make([]Faker, count)
	for i := 0; i < count; i++ {
		fakers[i] = Faker{
			gofakeit.NewFaker(rand.NewPCG(baseSeed, uint64(i)), false),
		}
	}
	return fakers
}

func (f Faker) SetPCG(seed uint64, offset uint64) {
	source, ok := f.Rand.(*rand.PCG)
	if !ok {
		f.Rand = rand.NewPCG(seed, offset)
		return
	}

	source.Seed(seed, offset)
	return
}

func (f Faker) SetSeed(seed Seed, offset []byte) {
	f.SetPCG(uint64(seed), ToSeedOffset(offset))
}

func (f Faker) SetSeedOffset(seed Seed, offset uint64) {
	f.SetPCG(uint64(seed), offset)
}

func (f Faker) Bernoulli(p float32) bool {
	return f.Float32() < p
}
