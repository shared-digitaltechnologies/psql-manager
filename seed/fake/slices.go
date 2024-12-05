package fake

import (
	"fmt"
	"reflect"

	"github.com/google/uuid"
)

func (f Faker) RandomInt64(values []int64) int64 {
	return randomElement(f, values, 0)
}

func (f Faker) RandomFloat32(values []float32) float32 {
	return randomElement(f, values, 0)
}

func (f Faker) RandomFloat64(values []float64) float64 {
	return randomElement(f, values, 0)
}

func (f Faker) RandomUUID(values []uuid.UUID) uuid.UUID {
	return randomElement(f, values, uuid.New())
}

func randomElement[T any](faker Faker, values []T, defaultValue T) T {
	size := len(values)
	if size == 0 {
		return defaultValue
	}
	if size == 1 {
		return values[0]
	}
	return values[faker.IntN(size)]
}

func (f Faker) UniqueIndices(l int, n int) []int {
	if l < n {
		panic(fmt.Errorf("Available indices too small: l=%d < n=%d", l, n))
	}

	if l > n/2 {
		res := make([]int, l)
		for i := 0; i < l; i++ {
			res[i] = i
		}
		f.ShuffleInts(res)
		return res[0:n]
	}

	res := make([]int, n)
	for i := 0; i < n; i++ {
		x := f.IntN(l - i)
		for j := 0; j < i; j++ {
			if x >= res[j] {
				x += 1
			}
		}
		res[i] = x
	}

	return res
}

func randomN[K any](f Faker, source []K, count int) []K {
	ix := f.UniqueIndices(len(source), count)
	res := make([]K, count)
	for i := 0; i < count; i++ {
		res[i] = source[ix[i]]
	}
	return res
}

func (f Faker) RandomN(slice any, n int) any {
	typ := reflect.TypeOf(slice)
	if typ.Kind() != reflect.Slice {
		panic("slice is not a slice")
	}

	s := reflect.ValueOf(slice)
	l := s.Len()

	ix := f.UniqueIndices(l, n)

	result := reflect.MakeSlice(typ, l, l)
	for i := 0; i < l; i++ {
		result.Index(i).Set(s.Index(ix[i]))
	}
	return result
}

func (f Faker) RandomIntN(values []int, n int) []int {
	return randomN(f, values, n)
}

func (f Faker) RandomInt64N(values []int64, n int) []int64 {
	return randomN(f, values, n)
}

func (f Faker) RandomStringN(values []string, n int) []string {
	return randomN(f, values, n)
}

func (f Faker) RandomUUIDN(values []uuid.UUID, n int) []uuid.UUID {
	return randomN(f, values, n)
}

func (f Faker) RandomFloat32N(values []float32, n int) []float32 {
	return randomN(f, values, n)
}

func (f Faker) RandomFloat64N(values []float64, n int) []float64 {
	return randomN(f, values, n)
}
