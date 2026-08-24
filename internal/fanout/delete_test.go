package fanout

import (
	"context"
	"testing"
	"time"

	"eventpush/internal/metric"
	"eventpush/internal/model"
	"eventpush/internal/session"
	"eventpush/internal/subscription"
)

// TestFanoutStopsAfterTopicDelete reproduces the bug where deleting a
// topic left the fanout broadcasting to the pre-deletion subscriber
// list because the cached snapshot was never invalidated.
func TestFanoutStopsAfterTopicDelete(t *testing.T) {
	topics := subscription.NewTopicRegistry()
	sessions := session.NewSessionRegistry()
	metrics := metric.New()
	f := New(topics, sessions, metrics)

	sid := model.SessionID("client-1")
	c := &recordConn{}
	sessions.Bind(sid, c, session.DefaultConfig())
	topics.Subscribe(sid, "device/a")

	ev := model.NewEvent("device/a", []byte("hello"))
	ev.Sequence = 1
	// First dispatch populates the snapshot cache for the topic.
	if err := f.Dispatch(context.Background(), ev.Topic, []model.Event{ev}); err != nil {
		t.Fatal(err)
	}
	if got := waitForRecords(c); got != 1 {
		t.Fatalf("after first dispatch: records = %d, want 1", got)
	}

	// Delete the topic. The cached snapshot must be invalidated so the
	// next dispatch sees no subscribers instead of the stale list.
	topics.DeleteTopic("device/a")

	c.mu.Lock()
	c.records = nil
	c.mu.Unlock()

	ev2 := model.NewEvent("device/a", []byte("again"))
	ev2.Sequence = 2
	if err := f.Dispatch(context.Background(), ev2.Topic, []model.Event{ev2}); err != nil {
		t.Fatalf("dispatch after delete: %v", err)
	}
	// Give the (zero) deliveries a moment; nothing should arrive.
	if got := waitForRecords(c); got != 0 {
		t.Fatalf("after delete: records = %d, want 0 (stale snapshot delivered to deleted topic)", got)
	}
}

// TestFanoutRebuildsAfterUnsubscribe ensures membership changes between
// dispatches are reflected rather than served from a stale cache.
func TestFanoutRebuildsAfterUnsubscribe(t *testing.T) {
	topics := subscription.NewTopicRegistry()
	sessions := session.NewSessionRegistry()
	metrics := metric.New()
	f := New(topics, sessions, metrics)

	sid := model.SessionID("client-1")
	c := &recordConn{}
	sessions.Bind(sid, c, session.DefaultConfig())
	topics.Subscribe(sid, "device/a")

	ev := model.NewEvent("device/a", []byte("hi"))
	ev.Sequence = 1
	_ = f.Dispatch(context.Background(), ev.Topic, []model.Event{ev})
	if got := waitForRecords(c); got != 1 {
		t.Fatalf("records = %d, want 1", got)
	}

	topics.Unsubscribe(sid, "device/a")
	c.mu.Lock()
	c.records = nil
	c.mu.Unlock()

	ev2 := model.NewEvent("device/a", []byte("hi2"))
	ev2.Sequence = 2
	_ = f.Dispatch(context.Background(), ev2.Topic, []model.Event{ev2})
	if got := waitForRecords(c); got != 0 {
		t.Fatalf("after unsubscribe: records = %d, want 0", got)
	}
}

// TestFanoutCacheStats exercises the hit/miss counters that previously
// stayed at zero because the cache never compared versions.
func TestFanoutCacheStats(t *testing.T) {
	topics := subscription.NewTopicRegistry()
	sessions := session.NewSessionRegistry()
	metrics := metric.New()
	f := New(topics, sessions, metrics)

	sid := model.SessionID("client-1")
	sessions.Bind(sid, &recordConn{}, session.DefaultConfig())
	topics.Subscribe(sid, "device/a")

	ev := model.NewEvent("device/a", []byte("x"))
	_ = f.Dispatch(context.Background(), ev.Topic, []model.Event{ev}) // miss
	_ = f.Dispatch(context.Background(), ev.Topic, []model.Event{ev}) // hit
	_ = f.Dispatch(context.Background(), ev.Topic, []model.Event{ev}) // hit

	stats := f.CacheStats()
	if stats.Misses != 1 {
		t.Fatalf("misses = %d, want 1", stats.Misses)
	}
	if stats.Hits != 2 {
		t.Fatalf("hits = %d, want 2", stats.Hits)
	}
}

// waitForRecords polls the connection for up to a short deadline and
// returns the record count observed.
func waitForRecords(c *recordConn) int {
	deadline := time.Now().Add(300 * time.Millisecond)
	for {
		n := len(c.Records())
		if n > 0 || time.Now().After(deadline) {
			return n
		}
		time.Sleep(5 * time.Millisecond)
	}
}
