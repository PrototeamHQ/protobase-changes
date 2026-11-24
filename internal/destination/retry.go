package destination

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// retryDelays are the waits before each retry, as Standard Webhooks
// recommends: about three days in all.
var retryDelays = []time.Duration{
	5 * time.Second, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour, 5 * time.Hour,
	10 * time.Hour, 14 * time.Hour, 20 * time.Hour, 24 * time.Hour,
}

// maxTries is the most tries a destination may set: one per wait, plus the first.
var maxTries = len(retryDelays) + 1

// maxRetryAfter bounds how long a receiver's Retry-After can hold a destination.
const maxRetryAfter = 24 * time.Hour

// retryDelay is the wait after the tried-th failed try: the schedule's, or
// longer when the receiver asked with Retry-After.
func retryDelay(schedule []time.Duration, tried int, retryAfter time.Duration) time.Duration {
	scheduled := schedule[min(tried, len(schedule))-1]
	return max(scheduled, min(retryAfter, maxRetryAfter))
}

// parseRetryAfter reads a Retry-After header, in seconds or as an HTTP date,
// as a wait from now; zero when absent or unreadable.
func parseRetryAfter(header string, now time.Time) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(header); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(header); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}
