package flow

import (
	"sync"
	"time"

	"eventpush/internal/model"
)

// Limiter is a per-session token bucket.
type Limiter struct {
	mu    sync.Mutex
	rate  float64
	burst float64
	last  map[model.SessionID]bucket
}

type bucket struct {
	tokens float64
	seen   time.Time
}

// NewLimiter builds a limiter from a validated policy.
func NewLimiter(p Policy) *Limiter {
	if err := p.Validate(); err != nil {
		p = DefaultPolicy()
	}
	return &Limiter{
		rate:  p.Rate,
		burst: float64(p.Burst),
		last:  make(map[model.SessionID]bucket),
	}
}

// Allow consumes one token for the session and reports whether the send
// may proceed.
func (l *Limiter) Allow(sid model.SessionID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	b := l.last[sid]
	if b.seen.IsZero() {
		b = bucket{tokens: l.burst, seen: now}
	} else {
		elapsed := now.Sub(b.seen).Seconds()
		b.tokens += elapsed * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.seen = now
	}
	if b.tokens < 1 {
		l.last[sid] = b
		return false
	}
	b.tokens--
	l.last[sid] = b
	return true
}

// Credits returns the current tokens available for the session.
func (l *Limiter) Credits(sid model.SessionID) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.last[sid].tokens
}
