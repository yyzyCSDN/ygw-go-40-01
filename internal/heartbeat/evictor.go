package heartbeat

import (
	"log"
	"time"

	"eventpush/internal/metric"
	"eventpush/internal/session"
)

// Evictor tears down a session whose heartbeat has expired.
type Evictor struct {
	registry *session.SessionRegistry
	metrics  *metric.Metrics
}

// NewEvictor creates an eviction coordinator.
func NewEvictor(registry *session.SessionRegistry, metrics *metric.Metrics) *Evictor {
	return &Evictor{
		registry: registry,
		metrics:  metrics,
	}
}

// Evict closes the session through its writer so an in-flight frame is
// finished or safely aborted before the transport is released.
func (e *Evictor) Evict(s *session.Session) {
	if !s.BeginClose() {
		return
	}
	s.Writer().Shutdown()
	if !s.Writer().Wait(3 * time.Second) {
		log.Printf("writer of session %s did not release within timeout", s.ID())
	}
	e.registry.Remove(s)
	e.metrics.Evicted()
}
