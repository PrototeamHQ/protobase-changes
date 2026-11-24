package destination

import (
	"net/http"
	"testing"
	"time"
)

func TestRetryDelay(t *testing.T) {
	if d := retryDelay(retryDelays, 1, 0); d != 5*time.Second {
		t.Errorf("first wait %s", d)
	}
	if d := retryDelay(retryDelays, 2, time.Hour); d != time.Hour {
		t.Errorf("Retry-After of an hour gave %s", d)
	}
	if d := retryDelay(retryDelays, 1, 7*24*time.Hour); d != maxRetryAfter {
		t.Errorf("a week's Retry-After gave %s", d)
	}
	if d := retryDelay(retryDelays, maxTries+3, 0); d != 24*time.Hour {
		t.Errorf("past the schedule %s", d)
	}
}

func TestParseRetryAfter(t *testing.T) {
	cases := map[string]time.Duration{
		"":     0,
		"120":  2 * time.Minute,
		"-5":   0,
		"soon": 0,
		now.Add(time.Hour).UTC().Format(http.TimeFormat):  time.Hour,
		now.Add(-time.Hour).UTC().Format(http.TimeFormat): 0,
	}
	for header, want := range cases {
		if got := parseRetryAfter(header, now); got != want {
			t.Errorf("%q: %s, want %s", header, got, want)
		}
	}
}
