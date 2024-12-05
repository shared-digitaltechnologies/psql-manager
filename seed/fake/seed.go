package fake

import (
	"fmt"
	"hash/fnv"
	"math/rand/v2"

	"github.com/brianvoe/gofakeit/v7"
)

/**
 * A seed for a faker
 */
type Seed uint64

func (s Seed) String() string {
	return fmt.Sprintf("0x%016x", uint64(s))
}

func (s Seed) GoString() string {
	return fmt.Sprintf("Seed(0x%016x)", uint64(s))
}

func (s Seed) NewSourceOffset(offset uint64) rand.Source {
	return rand.NewPCG(uint64(s), offset)
}

func ToSeedOffset(v []byte) uint64 {
	hash := fnv.New64()
	hash.Write(v)
	return hash.Sum64()
}

func (s Seed) NewSource(offset []byte) rand.Source {
	return s.NewSourceOffset(ToSeedOffset(offset))
}

func (s Seed) NewFakerOffset(offset uint64) Faker {
	return Faker{gofakeit.NewFaker(s.NewSourceOffset(offset), true)}
}

func (s Seed) NewFaker(offset []byte) Faker {
	return s.NewFakerOffset(ToSeedOffset(offset))
}

func (f Faker) NewSeed() Seed {
	return Seed(f.Uint64())
}
