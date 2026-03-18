package backoff

import (
	"math"
	"math/rand"
	"time"
)

// backoff params
var (
	baseDelay time.Duration = 100 * time.Millisecond
	maxDelay  time.Duration = 1 * time.Minute
	factor    float64       = 2
	jitter    float64       = 0.1
)

func GetDelay(retryNum int) time.Duration {
	backoffTime := baseDelay * time.Duration(math.Pow(factor, float64(retryNum)))

	// cap delay
	backoffTime = min(backoffTime, maxDelay)

	//add jitter
	backoffTime += time.Duration(rand.Float64() * jitter * float64(backoffTime))

	return backoffTime
}
