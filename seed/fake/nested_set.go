package fake

import (
	"math/rand/v2"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/shared-digitaltechnologies/psql-manager/db"
)

type fakeNestedSetSource struct {
	Faker
	seed   uint64
	offset uint64
	gen    FakeNestedSetSourceGen
}

type FakeNestedSetSourceGen = func(f Faker, pos int, depth int) (data []any, children int)

var nsSource *rand.PCG = rand.NewPCG(0, 0)
var nsFaker Faker = Faker{gofakeit.NewFaker(nsSource, false)}

func evalFakeNestedSetWith(seed uint64, pos int, depth int, gen FakeNestedSetSourceGen) db.ConstNestedSetSource[[]any] {
	offset := (uint64(pos) & (1<<32 - 1)) | (uint64(depth) << 32)
	nsSource.Seed(seed, offset)
	nextSeed := nsSource.Uint64()

	data, childCount := gen(nsFaker, pos, depth)
	children := make([]db.NestedSetSource[[]any], childCount)

	for i := 0; i < childCount; i++ {
		children[i] = evalFakeNestedSetWith(nextSeed, i, depth+1, gen)
	}

	return db.ConstNestedSetSource[[]any]{Data: data, Children: children}
}

func (f Faker) NestedSet(rootCount int, gen FakeNestedSetSourceGen) db.NestedSet[[]any] {
	seed := f.Uint64()

	result := db.NewNestedSet[[]any]()

	for i := 0; i < rootCount; i++ {
		result.Append(evalFakeNestedSetWith(seed, i, 0, gen))
	}

	return result
}

type FakeNestedSetSourceGenWithoutChildren = func(f Faker, pos int, depth int, childCount int) []any

func (f Faker) NestedSetWithCatDistr(
	rootCount int,
	maxDepth int,
	expectedDeepestChildrenCount float64,
	gen FakeNestedSetSourceGenWithoutChildren,
) db.NestedSet[[]any] {
	return f.NestedSet(rootCount, func(ff Faker, pos int, depth int) (data []any, children int) {
		d := float64(depth + 1)
		m := float64(maxDepth)
		p := (m - d) / ((m - 1) * d) // chance that node has children. (i.e. Chance of stable isotope)
		if ff.Bernoulli(float32(p)) {
			lambda := expectedDeepestChildrenCount * (m - d)
			children = ff.Poisson(lambda)
		}

		data = gen(ff, pos, depth, children)
		return
	})
}
