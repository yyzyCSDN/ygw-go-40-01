// Package publish implements the publish pipeline: append to the event
// store, dispatch through fanout, then commit the cursor.
package publish

import (
	"context"

	"eventpush/internal/event"
	"eventpush/internal/metric"
	"eventpush/internal/model"
)

// Dispatcher is the broadcast boundary used by the broker.
type Dispatcher interface {
	Dispatch(ctx context.Context, topic string, events []model.Event) error
}

// Broker owns the publish pipeline.
type Broker struct {
	store    *event.Store
	fanout   Dispatcher
	metrics  *metric.Metrics
	progress *event.Cursor
}

// New creates a broker over a store and a dispatcher.
func New(store *event.Store, fanout Dispatcher, metrics *metric.Metrics) *Broker {
	return &Broker{
		store:    store,
		fanout:   fanout,
		metrics:  metrics,
		progress: event.NewCursor(store),
	}
}

// Publish appends, dispatches and commits. The cursor advances only
// after a successful dispatch, so a failed broadcast stays retryable.
// It returns the assigned sequence of the event.
func (b *Broker) Publish(ctx context.Context, ev model.Event) (int64, error) {
	seq, err := b.store.Append(ev)
	if err != nil {
		return 0, err
	}
	ev.Sequence = seq
	if err := b.fanout.Dispatch(ctx, ev.Topic, []model.Event{ev}); err != nil {
		b.metrics.Failed()
		return 0, err
	}
	if err := b.store.Commit(seq); err != nil {
		return 0, err
	}
	b.progress.Advance(seq)
	b.metrics.Published()
	return seq, nil
}

// Progress returns the highest committed sequence the broker has
// published successfully.
func (b *Broker) Progress() int64 {
	return b.progress.Position()
}
