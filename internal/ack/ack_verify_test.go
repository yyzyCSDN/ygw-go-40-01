package ack_test

import (
	"errors"
	"testing"
	"time"

	"eventpush/internal/model"
	"eventpush/internal/session"
)

type stuckConn struct {
	started chan struct{}
	release chan struct{}
}

func newStuckConn() *stuckConn {
	return &stuckConn{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (c *stuckConn) WriteMessage(kind int, data []byte) error {
	select {
	case c.started <- struct{}{}:
	default:
	}
	<-c.release
	return nil
}

func (c *stuckConn) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("closed")
}

func (c *stuckConn) Close() error { return nil }

func (c *stuckConn) RemoteAddr() string { return "test" }

func (c *stuckConn) SetReadDeadline(time.Time) error { return nil }

func TestBackpressureTimeoutNotMarkedDelivered(t *testing.T) {
	cfg := session.DefaultConfig()
	cfg.QueueSize = 1
	cfg.WriteTimeout = 80 * time.Millisecond
	bc := newStuckConn()
	sess := session.New(model.SessionID("slow-1"), bc, cfg)

	deliver := func(seq int64) error {
		ev := model.NewEvent("t", []byte("x"))
		ev.Sequence = seq
		return sess.Deliver(ev)
	}
	if err := deliver(1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-bc.started:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not start")
	}
	if err := deliver(2); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err := deliver(3)
	if err == nil {
		t.Fatalf("third delivery should hit backpressure but was accepted")
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("backpressure returned too fast: %v", elapsed)
	}
	if sess.Tracker().Pending(3) {
		t.Fatalf("timed-out event was marked as delivered")
	}
	close(bc.release)
}
