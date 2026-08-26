package event_test

import (
	"context"
	"errors"
	"testing"

	"eventpush/internal/event"
	"eventpush/internal/metric"
	"eventpush/internal/model"
	"eventpush/internal/publish"
)

type failingDispatcher struct{}

func (failingDispatcher) Dispatch(ctx context.Context, topic string, events []model.Event) error {
	return errors.New("broadcast failed")
}

func TestCursorAdvanceAfterFanoutSuccess(t *testing.T) {
	store := event.NewStore(16)
	metrics := metric.New()
	b := publish.New(store, failingDispatcher{}, metrics)
	if _, err := b.Publish(context.Background(), model.NewEvent("t", []byte("x"))); err == nil {
		t.Fatal("publish must fail when fanout fails")
	}
	if cur := store.Cursor(); cur != 0 {
		t.Fatalf("cursor advanced to %d before fanout succeeded", cur)
	}
	if pending := len(store.Pending()); pending != 1 {
		t.Fatalf("failed event is not retryable: pending=%d", pending)
	}
}
