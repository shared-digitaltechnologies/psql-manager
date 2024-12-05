package distributions

func factorial(k float64) float64 {
	res := 1.
	for i := 2.; i <= k; i += 1 {
		res *= i
	}
	return res
}
