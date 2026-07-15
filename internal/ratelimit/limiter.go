// Package ratelimit provides an in-memory, per-key admission limiter.
package ratelimit

import (
	"errors"
	"math"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultRatePerSecond permits short bursts while keeping sustained request
	// admission bounded for each API key.
	DefaultRatePerSecond = 2.0
	DefaultBurst         = 20
	DefaultIdleTTL       = 15 * time.Minute
)

// Clock makes token replenishment deterministic in tests.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Config controls a token bucket for each distinct key. Zero values select
// secure, production-friendly defaults; negative values are invalid.
type Config struct {
	RatePerSecond float64
	Burst         int
	IdleTTL       time.Duration
}

func (config Config) withDefaults() Config {
	if config.RatePerSecond == 0 {
		config.RatePerSecond = DefaultRatePerSecond
	}
	if config.Burst == 0 {
		config.Burst = DefaultBurst
	}
	if config.IdleTTL == 0 {
		config.IdleTTL = DefaultIdleTTL
	}
	return config
}

// Decision reports whether one admission token was consumed. RetryAfter is
// set only for denied decisions and is the earliest time another token can be
// available for the same key.
type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
}

type bucket struct {
	tokens   float64
	updated  time.Time
	lastSeen time.Time
}

// Limiter is safe for concurrent use.
type Limiter struct {
	mu        sync.Mutex
	config    Config
	clock     Clock
	buckets   map[string]*bucket
	lastSweep time.Time
}

// New constructs a limiter. A nil clock uses wall time.
func New(config Config, clock Clock) (*Limiter, error) {
	config = config.withDefaults()
	if config.RatePerSecond < 0 || math.IsNaN(config.RatePerSecond) || math.IsInf(config.RatePerSecond, 0) {
		return nil, errors.New("rate per second must be finite and positive")
	}
	if config.Burst < 0 {
		return nil, errors.New("burst must be positive")
	}
	if config.IdleTTL < 0 {
		return nil, errors.New("idle TTL must be positive")
	}
	if clock == nil {
		clock = systemClock{}
	}
	now := clock.Now()
	return &Limiter{
		config: config, clock: clock, buckets: make(map[string]*bucket), lastSweep: now,
	}, nil
}

// Allow attempts to consume one token for key. An empty key is still isolated
// in its own bucket; callers should normally use a stable authenticated key id.
func (limiter *Limiter) Allow(key string) Decision {
	key = strings.TrimSpace(key)
	now := limiter.clock.Now()

	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	limiter.sweep(now)

	current, exists := limiter.buckets[key]
	if !exists {
		current = &bucket{tokens: float64(limiter.config.Burst), updated: now, lastSeen: now}
		limiter.buckets[key] = current
	}
	if elapsed := now.Sub(current.updated); elapsed > 0 {
		current.tokens = math.Min(
			float64(limiter.config.Burst),
			current.tokens+elapsed.Seconds()*limiter.config.RatePerSecond,
		)
		current.updated = now
	}
	current.lastSeen = now
	if current.tokens >= 1 {
		current.tokens--
		return Decision{Allowed: true}
	}

	missing := 1 - current.tokens
	retryNanoseconds := missing / limiter.config.RatePerSecond * float64(time.Second)
	maxDuration := time.Duration(1<<63 - 1)
	var retryAfter time.Duration
	if retryNanoseconds >= float64(maxDuration) {
		retryAfter = maxDuration
	} else {
		retryAfter = time.Duration(math.Ceil(retryNanoseconds))
	}
	if retryAfter < time.Nanosecond {
		retryAfter = time.Nanosecond
	}
	return Decision{RetryAfter: retryAfter}
}

func (limiter *Limiter) sweep(now time.Time) {
	if elapsed := now.Sub(limiter.lastSweep); elapsed < limiter.config.IdleTTL {
		return
	}
	for key, current := range limiter.buckets {
		if now.Sub(current.lastSeen) >= limiter.config.IdleTTL {
			delete(limiter.buckets, key)
		}
	}
	limiter.lastSweep = now
}
