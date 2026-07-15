package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) Advance(delta time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(delta)
}

func TestLimiterRefillsAndIsolatesKeys(t *testing.T) {
	t.Parallel()
	clock := &fakeClock{now: time.Unix(100, 0)}
	limiter, err := New(Config{RatePerSecond: 2, Burst: 2, IdleTTL: time.Hour}, clock)
	if err != nil {
		t.Fatal(err)
	}
	if !limiter.Allow("key-a").Allowed || !limiter.Allow("key-a").Allowed {
		t.Fatal("initial burst was not admitted")
	}
	denied := limiter.Allow("key-a")
	if denied.Allowed || denied.RetryAfter != 500*time.Millisecond {
		t.Fatalf("denied decision = %+v", denied)
	}
	if !limiter.Allow("key-b").Allowed {
		t.Fatal("one API key exhausted another key's bucket")
	}
	clock.Advance(250 * time.Millisecond)
	if decision := limiter.Allow("key-a"); decision.Allowed || decision.RetryAfter != 250*time.Millisecond {
		t.Fatalf("partially refilled decision = %+v", decision)
	}
	clock.Advance(250 * time.Millisecond)
	if decision := limiter.Allow("key-a"); !decision.Allowed {
		t.Fatalf("refilled decision = %+v", decision)
	}
}

func TestLimiterConcurrentBurstIsExact(t *testing.T) {
	t.Parallel()
	clock := &fakeClock{now: time.Unix(100, 0)}
	limiter, err := New(Config{RatePerSecond: 1, Burst: 8, IdleTTL: time.Hour}, clock)
	if err != nil {
		t.Fatal(err)
	}
	var allowed atomic.Int64
	var wait sync.WaitGroup
	for range 64 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if limiter.Allow("shared").Allowed {
				allowed.Add(1)
			}
		}()
	}
	wait.Wait()
	if got := allowed.Load(); got != 8 {
		t.Fatalf("allowed = %d, want 8", got)
	}
}

func TestLimiterDefaultsAndIdleEviction(t *testing.T) {
	t.Parallel()
	clock := &fakeClock{now: time.Unix(100, 0)}
	limiter, err := New(Config{RatePerSecond: 1, Burst: 1, IdleTTL: time.Second}, clock)
	if err != nil {
		t.Fatal(err)
	}
	if !limiter.Allow("key").Allowed || limiter.Allow("key").Allowed {
		t.Fatal("unexpected initial bucket decisions")
	}
	clock.Advance(time.Second)
	if decision := limiter.Allow("other"); !decision.Allowed {
		t.Fatalf("other key decision = %+v", decision)
	}
	if _, exists := limiter.buckets["key"]; exists {
		t.Fatal("idle bucket was not evicted")
	}
	// The idle key was evicted and therefore receives a fresh burst.
	if decision := limiter.Allow("key"); !decision.Allowed {
		t.Fatalf("evicted key decision = %+v", decision)
	}

	defaults, err := New(Config{}, clock)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < DefaultBurst; index++ {
		if !defaults.Allow("default").Allowed {
			t.Fatalf("default burst stopped at %d", index)
		}
	}
	if defaults.Allow("default").Allowed {
		t.Fatal("default burst was not bounded")
	}
}

func TestLimiterRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()
	for _, config := range []Config{
		{RatePerSecond: -1},
		{Burst: -1},
		{IdleTTL: -time.Second},
	} {
		if _, err := New(config, nil); err == nil {
			t.Fatalf("New(%+v) succeeded", config)
		}
	}
}
