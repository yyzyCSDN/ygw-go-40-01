// Package heartbeat keeps connections alive and evicts stale ones.
package heartbeat

import (
	"context"
	"time"

	"eventpush/internal/metric"
	"eventpush/internal/model"
	"eventpush/internal/session"
)

// Heartbeat scans the session registry on a fixed interval.
type Heartbeat struct {
	registry *session.SessionRegistry
	metrics  *metric.Metrics
	interval time.Duration
	timeout  time.Duration
	suspect  time.Duration
	evictor  *Evictor
}

// New creates a heartbeat controller.
func New(registry *session.SessionRegistry, metrics *metric.Metrics, interval, timeout, suspect time.Duration) *Heartbeat {
	return &Heartbeat{
		registry: registry,
		metrics:  metrics,
		interval: interval,
		timeout:  timeout,
		suspect:  suspect,
		evictor:  NewEvictor(registry, metrics),
	}
}

// Run performs periodic sweeps until the context is cancelled.
func (h *Heartbeat) Run(ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			h.sweep(now)
		}
	}
}

func (h *Heartbeat) sweep(now time.Time) {
	for _, s := range h.registry.Sessions() {
		if !h.registry.IsLive(s.ID()) {
			continue
		}
		last, ok := h.registry.LastSeen(s.ID())
		if !ok {
			continue
		}
		switch h.classify(now.Sub(last)) {
		case model.ConnClosing:
			h.evictor.Evict(s)
		case model.ConnSuspected:
			s.Suspect()
		default:
			_ = h.Renew(s.ID())
		}
	}
}

// classify maps a heartbeat age onto the connection state machine.
func (h *Heartbeat) classify(age time.Duration) model.ConnState {
	switch {
	case h.stale(age):
		return model.ConnClosing
	case h.suspectAge(age):
		return model.ConnSuspected
	default:
		return model.ConnActive
	}
}

// Renew refreshes the heartbeat timestamp of a session.
func (h *Heartbeat) Renew(sid model.SessionID) bool {
	return h.registry.Touch(sid, time.Now().UTC())
}

// Evictor exposes the eviction entry point used by the sweep.
func (h *Heartbeat) Evictor() *Evictor {
	return h.evictor
}
