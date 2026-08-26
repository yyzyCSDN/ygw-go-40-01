// Package fanout broadcasts published events to the subscribers of a
// topic through per-session ordered delivery.
package fanout

import (
	"context"
	"errors"
	"sync"

	"eventpush/internal/metric"
	"eventpush/internal/model"
	"eventpush/internal/session"
	"eventpush/internal/subscription"
)

// ErrOutOfOrder is returned when a batch does not follow ascending
// sequence order.
var ErrOutOfOrder = errors.New("event batch is not in sequence order")

// Fanout takes a subscription snapshot once per batch and delivers the
// batch to every session in that snapshot.
type Fanout struct {
	subs     *subscription.TopicRegistry
	sessions *session.SessionRegistry
	metrics  *metric.Metrics
	mu       sync.Mutex
	cache    map[string]snapshotEntry
	hits     uint64
	misses   uint64
}

// New creates a fanout dispatcher.
func New(subs *subscription.TopicRegistry, sessions *session.SessionRegistry, metrics *metric.Metrics) *Fanout {
	return &Fanout{
		subs:     subs,
		sessions: sessions,
		metrics:  metrics,
		cache:    make(map[string]snapshotEntry),
	}
}

// Dispatch delivers one batch of events to the topic's subscribers. The
// subscriber snapshot is taken once at the start of the batch so a
// subscription that joins mid-batch never receives a partial batch.
func (f *Fanout) Dispatch(ctx context.Context, topic string, events []model.Event) error {
	sessions := f.takeSnapshot(topic)
	if len(sessions) == 0 {
		return nil
	}
	var previous int64
	for _, ev := range events {
		if ev.Sequence > 0 && previous > 0 && ev.Sequence <= previous {
			return ErrOutOfOrder
		}
		if ev.Sequence > 0 {
			previous = ev.Sequence
		}
	}
	var firstErr error
	var failed int
	for _, ev := range events {
		for _, sid := range sessions {
			if err := f.deliver(ctx, sid, ev); err != nil {
				failed++
				if firstErr == nil {
					firstErr = err
				}
			}
		}
	}
	if firstErr != nil {
		return &DispatchError{Failed: failed, Cause: firstErr}
	}
	return firstErr
}

func (f *Fanout) deliver(ctx context.Context, sid model.SessionID, ev model.Event) error {
	sess := f.sessions.Get(sid)
	if sess == nil {
		return errNoSession(sid)
	}
	if err := sess.Deliver(ev); err != nil {
		return err
	}
	f.metrics.Delivered()
	return nil
}
