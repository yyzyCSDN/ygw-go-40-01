package fanout_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"eventpush/internal/fanout"
	"eventpush/internal/metric"
	"eventpush/internal/model"
	"eventpush/internal/session"
	"eventpush/internal/subscription"
)

type recConn struct {
	mu      sync.Mutex
	records []int64
}

func (c *recConn) WriteMessage(kind int, data []byte) error {
	parts := strings.SplitN(string(data), "|", 2)
	seq, _ := strconv.ParseInt(parts[0], 10, 64)
	c.mu.Lock()
	c.records = append(c.records, seq)
	c.mu.Unlock()
	return nil
}

func (c *recConn) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("closed")
}

func (c *recConn) Close() error { return nil }

func (c *recConn) RemoteAddr() string { return "test" }

func (c *recConn) SetReadDeadline(time.Time) error { return nil }

func (c *recConn) Sequences() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int64, len(c.records))
	copy(out, c.records)
	return out
}

func TestFanoutUsesFreshSubscriptionAfterDelete(t *testing.T) {
	topics := subscription.NewTopicRegistry()
	sessions := session.NewSessionRegistry()
	metrics := metric.New()
	f := fanout.New(topics, sessions, metrics)

	sid := model.SessionID("s1")
	rec := &recConn{}
	sessions.Bind(sid, rec, session.DefaultConfig())
	topics.Subscribe(sid, "t")

	ev1 := model.NewEvent("t", []byte("one"))
	ev1.Sequence = 1
	if err := f.Dispatch(context.Background(), "t", []model.Event{ev1}); err != nil {
		t.Fatal(err)
	}
	waitRecords := func(n int) bool {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if len(rec.Sequences()) >= n {
				return true
			}
			time.Sleep(5 * time.Millisecond)
		}
		return false
	}
	if !waitRecords(1) {
		t.Fatal("first event was not delivered")
	}

	topics.DeleteTopic("t")
	ev2 := model.NewEvent("t", []byte("two"))
	ev2.Sequence = 2
	if err := f.Dispatch(context.Background(), "t", []model.Event{ev2}); err != nil {
		t.Fatal(err)
	}
	if waitRecords(2) {
		t.Fatal("subscriber received an event after the topic was deleted")
	}
}
