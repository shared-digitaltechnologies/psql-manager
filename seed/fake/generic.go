package fake

type Rand interface {
	IntN(n int) int
}

func Pick[K any](rand Rand, choices ...K) K {
	if len(choices) <= 0 {
		panic("No valid choices")
	}

	return choices[rand.IntN(len(choices))]
}
