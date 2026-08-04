package queue

import (
	"math/rand/v2"
	"time"
)

type Backoff struct {
	Base time.Duration
	Max  time.Duration
}

var DefaultBackoff = Backoff{Base: time.Second, Max: 5 * time.Minute}

// Delay is full-jitter backoff: uniform in [0, min(Max, Base*2^attempt)).
func (b Backoff) Delay(attempt int) time.Duration {
	base, max := b.Base, b.Max
	if base <= 0 {
		base = DefaultBackoff.Base
	}
	if max <= 0 {
		max = DefaultBackoff.Max
	}

	window := max
	// avoid shifting past 62 bits
	if attempt < 62 {
		if scaled := base << attempt; scaled > 0 && scaled < max {
			window = scaled
		}
	}
	return time.Duration(rand.Int64N(int64(window)))
}
