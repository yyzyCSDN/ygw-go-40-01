package publish

import (
	"context"
	"errors"
	"testing"

	"eventpush/internal/event"
	"eventpush/internal/metric"
	"eventpush/internal/model"
)

type okDispatcher struct{}

func (okDispatcher) Dispatch(ctx context.Context, topic string, events []model.Event) error {
	return nil
}

type failDispatcher struct{}

func (failDispatcher) Dispatch(ctx context.Context, topic string, events []model.Event) error {
	return errors.New("broadcast failed")
}

func TestPublishCommitsAfterDispatch(t *testing.T) {
	store := event.NewStore(16)
	metrics := metric.New()
	b := New(store, okDispatcher{}, metrics)
	seq, err := b.Publish(context.Background(), model.NewEvent("t", []byte("x")))
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Fatalf("sequence = %d, want 1", seq)
	}
	committed, err := store.Since(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(committed) != 1 || committed[0].Sequence != seq {
		t.Fatalf("committed events = %v, want sequence %d", committed, seq)
	}
	if metrics.Snapshot().Published != 1 {
		t.Fatalf("published metric = %d, want 1", metrics.Snapshot().Published)
	}
}

func TestPublishDispatchFailure(t *testing.T) {
	store := event.NewStore(16)
	metrics := metric.New()
	b := New(store, failDispatcher{}, metrics)
	if _, err := b.Publish(context.Background(), model.NewEvent("t", []byte("x"))); err == nil {
		t.Fatal("publish should fail when dispatch fails")
	}
	if metrics.Snapshot().Failed != 1 {
		t.Fatalf("failed metric = %d, want 1", metrics.Snapshot().Failed)
	}
}
