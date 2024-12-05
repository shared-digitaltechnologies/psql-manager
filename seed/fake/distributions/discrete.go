package distributions

import (
	"fmt"
	"math"
	"math/rand/v2"
)

/**
 * Interface for structs that represent descrete probabilistic distributions.
 */
type DiscreteDistribution interface {
	FakeSampleInt(rand *rand.Rand) int
}

type ConstInt int

func (d ConstInt) String() string {
	return fmt.Sprintf("Const(%d)", int(d))
}

func (d ConstInt) GoString() string {
	return fmt.Sprintf("ConstInt(%d)", int(d))
}

func (d ConstInt) Mass(value int) float64 {
	if int(d) == value {
		return 1
	} else {
		return 0
	}
}

func (d ConstInt) Mean() float64 {
	return float64(d)
}

func (d ConstInt) Variance() float64 {
	return 0
}

func (d ConstInt) FakeSampleInt(rand *rand.Rand) int {
	return int(d)
}

/**
 * Represents the binomial distribution.
 *
 * @see https://en.wikipedia.org/wiki/Binomial_distribution
 */
type Binomial struct {
	/**
	 * The amount of independent
	 */
	N int
	/**
	 * Chance of an experiment to succeed.
	 */
	P float32
}

func (b Binomial) String() string {
	return fmt.Sprintf("B(n=%d,p=%f)", b.N, b.P)
}

func (b Binomial) GoString() string {
	return fmt.Sprintf("Binomial{N: %d, P: %f}", b.N, b.P)
}

func (b Binomial) FakeSampleInt(rand *rand.Rand) int {
	if b.P <= 0 {
		return 0
	}

	if b.P >= 1 {
		return b.N
	}

	res := 0
	for i := 0; i < b.N; i++ {
		if rand.Float32() < b.P {
			res++
		}
	}
	return res
}

/**
 * Represents a negative binomial distribution.
 *
 * Result is the amount of failures before R successes have been reached.
 */
type NegBinomial struct {
	/**
	 * The amount of successes
	 */
	R int
	/**
	 * The chance of a success.
	 */
	P float32
}

func (d NegBinomial) String() string {
	return fmt.Sprintf("NB(r=%d,p=%f)", d.R, d.P)
}

func (d NegBinomial) GoString() string {
	return fmt.Sprintf("NegBinomial{R: %d, P: %f}", d.R, d.P)
}

func (d NegBinomial) FakeSampleInt(rand *rand.Rand) int {
	if d.P <= 0 {
		panic("NegBinominal with p=0 will take an infinite time to simulate!")
	}

	if d.P >= 1 {
		return 0
	}

	fails := 0
	successes := 0
	for successes < d.R {
		if rand.Float32() < d.P {
			successes++
		} else {
			fails++
		}
	}

	return fails
}

/**
 * Represents the poisson distribution.
 *
 * @see https://en.wikipedia.org/wiki/Poisson_distribution
 */
type Poisson struct {
	/**
	 * Expected amount of events on a fixed interval.
	 */
	Lambda float64
}

func (d Poisson) String() string {
	return fmt.Sprintf("poisson(λ=%f)", d.Lambda)
}

func (d Poisson) GoString() string {
	return fmt.Sprintf("Poisson{Lambda: %f}", d.Lambda)
}

func (d Poisson) Mass(value int) float64 {
	if value < 0 {
		return 0
	}

	if d.Lambda < 0 {
		if value == 1 {
			return 1
		} else {
			return 0
		}
	}

	k := float64(value)
	return (math.Pow(d.Lambda, k) * math.Pow(math.E, -d.Lambda)) / factorial(k)
}

func (d Poisson) Mean() float64 {
	if d.Lambda <= 0 {
		return 0
	}
	return d.Lambda
}

func (d Poisson) Variance() float64 {
	if d.Lambda <= 0 {
		return 0
	}
	return d.Lambda
}

func (d Poisson) FakeEventInterval(rand *rand.Rand) float64 {
	return (-math.Log(1. - rand.Float64())) / d.Lambda
}

func (d Poisson) FakeSampleInt(rand *rand.Rand) int {
	if d.Lambda <= 0 {
		return 0
	}

	res := 0
	for t := d.FakeEventInterval(rand); t < 1; t += d.FakeEventInterval(rand) {
		res += 1
	}
	return res
}

/**
 * Represents a geometric distribution, which can be thought of as the amount
 * of failures before one success.
 */
type Geometric struct {
	P float32
}

func (d Geometric) String() string {
	return fmt.Sprintf("Geo(p=%f)", d.P)
}

func (d Geometric) GoString() string {
	return fmt.Sprintf("Geometric{P: %f}", d.P)
}

func (d Geometric) Mass(value int) float64 {
	if d.P <= 0 {
		return 0
	}

	if d.P >= 1 {
		if value == 0 {
			return 1
		} else {
			return 0
		}
	}

	pSuccess := float64(d.P)
	res := pSuccess
	for i := 1; i < value; i++ {
		res *= 1 - pSuccess
	}
	return res
}

func (d Geometric) Mean() float64 {
	if d.P <= 0 {
		return math.Inf(1)
	}

	if d.P >= 0 {
		return 0
	}

	return 1/float64(d.P) - 1
}

func (d Geometric) Variance() float64 {
	return d.Mean() * float64(d.P)
}

func (d Geometric) FakeSampleInt(rand *rand.Rand) int {
	if d.P <= 0 {
		panic("Geo(p=0) will take an infinite time to simulate!")
	}

	if d.P >= 1 {
		return 0
	}

	i := 0
	for rand.Float32() < d.P {
		i++
	}
	return i
}

type GeometricMax struct {
	P   float32
	Max int
}

func (d GeometricMax) FakeSampleInt(rand *rand.Rand) int {
	if d.P <= 0 {
		return d.Max
	}

	if d.P >= 1 {
		return 0
	}

	for i := 0; i < d.Max; i++ {
		if rand.Float32() < d.P {
			return i
		}
	}
	return d.Max
}
