package reader

import (
	"time"

	"golang.org/x/time/rate"
)

// throttle holds a stream to its app's changes_per_second quota: a second's
// worth may come at once, and more waits its turn.
type throttle struct {
	// limit is nil for an app without the quota.
	limit *rate.Limiter
}

func newThrottle(perSecond *int) throttle {
	if perSecond == nil {
		return throttle{}
	}
	return throttle{limit: rate.NewLimiter(rate.Limit(*perSecond), *perSecond)}
}

// take counts n changes taken in at now and returns when the stream may
// read on. A transaction larger than a second's worth is counted in parts.
func (t throttle) take(n int, now time.Time) time.Time {
	if t.limit == nil {
		return now
	}
	readOn := now
	for n > 0 {
		part := min(n, t.limit.Burst())
		readOn = now.Add(t.limit.ReserveN(now, part).DelayFrom(now))
		n -= part
	}
	return readOn
}
