package reader

import (
	"testing"
	"time"
)

func TestThrottleHoldsToTheQuota(t *testing.T) {
	perSecond := 100
	th := newThrottle(&perSecond)
	now := time.Unix(1000, 0)
	if got := th.take(100, now); !got.Equal(now) {
		t.Fatalf("a second's worth waits until %v, want now", got)
	}
	if got := th.take(50, now); !got.Equal(now.Add(500 * time.Millisecond)) {
		t.Fatalf("50 more wait until %v, want half a second", got)
	}
	// 250 changes in one transaction are counted in parts of 100.
	if got := th.take(250, now); !got.Equal(now.Add(3 * time.Second)) {
		t.Fatalf("250 more wait until %v, want 3s", got)
	}
}

func TestNoQuotaNeverWaits(t *testing.T) {
	now := time.Unix(1000, 0)
	if got := newThrottle(nil).take(1_000_000, now); !got.Equal(now) {
		t.Fatalf("waits until %v without a quota", got)
	}
}
