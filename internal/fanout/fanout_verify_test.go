package fanout_test

import (
	"context"
	"errors"
	"fmt"
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

type gateConn struct {
	mu      sync.Mutex
	started chan struct{}
	release chan struct{}
	first   bool
}

func newGateConn() *gateConn {
	return &gateConn{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		first:   true,
	}
}

func (c *gateConn) WriteMessage(kind int, data []byte) error {
	c.mu.Lock()
	first := c.first
	if first {
		c.first = false
	}
	c.mu.Unlock()
	if first {
		select {
		case c.started <- struct{}{}:
		default:
		}
		<-c.release
	}
	return nil
}

func (c *gateConn) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("closed")
}

func (c *gateConn) Close() error { return nil }

func (c *gateConn) RemoteAddr() string { return "test" }

func (c *gateConn) SetReadDeadline(time.Time) error { return nil }

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

func TestFanoutSkipsMidBatchSubscriber(t *testing.T) {
	topics := subscription.NewTopicRegistry()
	sessions := session.NewSessionRegistry()
	metrics := metric.New()
	f := fanout.New(topics, sessions, metrics)

	sid1 := model.SessionID("sub-1")
	block := newGateConn()
	cfg := session.DefaultConfig()
	cfg.QueueSize = 1
	sessions.Bind(sid1, block, cfg)
	topics.Subscribe(sid1, "batch/t")

	sid2 := model.SessionID("sub-2")
	late := &recConn{}
	sessions.Bind(sid2, late, session.DefaultConfig())

	events := make([]model.Event, 5)
	for i := range events {
		events[i] = model.NewEvent("batch/t", []byte(fmt.Sprintf("e%d", i+1)))
		events[i].Sequence = int64(i + 1)
	}
	done := make(chan error, 1)
	go func() {
		done <- f.Dispatch(context.Background(), "batch/t", events)
	}()

	select {
	case <-block.started:
	case <-time.After(3 * time.Second):
		t.Fatal("batch dispatch did not start writing")
	}

	// The subscription joins while the batch is being broadcast.
	topics.Subscribe(sid2, "batch/t")
	close(block.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("batch dispatch did not finish")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(late.Sequences()) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	seqs := late.Sequences()
	for i, seq := range seqs {
		if seq != int64(i+1) {
			t.Fatalf("late subscriber received a partial batch starting mid-way: %v", seqs)
		}
	}
}
