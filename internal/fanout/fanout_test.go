package fanout

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"eventpush/internal/metric"
	"eventpush/internal/model"
	"eventpush/internal/session"
	"eventpush/internal/subscription"
)

type recordConn struct {
	mu      sync.Mutex
	records []string
}

func (c *recordConn) WriteMessage(kind int, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, string(data))
	return nil
}

func (c *recordConn) Records() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.records))
	copy(out, c.records)
	return out
}

// Lock/Unlock let tests wait on the connection without racing the
// session writer goroutine.
func (c *recordConn) Lock()   { c.mu.Lock() }
func (c *recordConn) Unlock() { c.mu.Unlock() }

func (c *recordConn) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("closed")
}

func (c *recordConn) Close() error { return nil }

func (c *recordConn) RemoteAddr() string { return "test" }

func (c *recordConn) SetReadDeadline(time.Time) error { return nil }

func TestFanoutDeliversToSubscriber(t *testing.T) {
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
	if err := f.Dispatch(context.Background(), ev.Topic, []model.Event{ev}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(c.records) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("records = %d, want 1", len(c.records))
		}
		time.Sleep(5 * time.Millisecond)
	}
	want := "1|device/a|hello"
	if c.records[0] != want {
		t.Fatalf("record = %q, want %q", c.records[0], want)
	}
	if metrics.Snapshot().Delivered != 1 {
		t.Fatalf("delivered metric = %d, want 1", metrics.Snapshot().Delivered)
	}
}
