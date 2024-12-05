package fake

import "github.com/shared-digitaltechnologies/psql-manager/seed/fake/distributions"

func (f Faker) SampleInt(distribution distributions.DiscreteDistribution) int {
	return distribution.FakeSampleInt(f.NewRand())
}

func (f Faker) SampleIntN(distribution distributions.DiscreteDistribution, count int) []int {
	rand := f.NewRand()
	res := make([]int, count)
	for i := 0; i < count; i++ {
		res[count] = distribution.FakeSampleInt(rand)
	}
	return res
}

func (f Faker) Binomial(p float32, n int) int {
	return f.SampleInt(distributions.Binomial{P: p, N: n})
}

func (f Faker) NegBinomial(p float32, r int) int {
	return f.SampleInt(distributions.NegBinomial{P: p, R: r})
}

func (f Faker) Poisson(lambda float64) int {
	return f.SampleInt(distributions.Poisson{Lambda: lambda})
}

func (f Faker) Geometric(p float32) int {
	return f.SampleInt(distributions.Geometric{P: p})
}

func (f Faker) GeometricMax(p float32, max int) int {
	return f.SampleInt(distributions.GeometricMax{P: p, Max: max})
}
