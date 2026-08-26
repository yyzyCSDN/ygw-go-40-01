package heartbeat_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"eventpush/internal/heartbeat"
	"eventpush/internal/metric"
	"eventpush/internal/model"
	"eventpush/internal/session"
)

type gatedConn struct {
	mu      sync.Mutex
	started chan struct{}
	release chan struct{}
	first   bool
}

func newGatedConn() *gatedConn {
	return &gatedConn{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		first:   true,
	}
}

func (c *gatedConn) WriteMessage(kind int, data []byte) error {
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

func (c *gatedConn) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("closed")
}

func (c *gatedConn) Close() error { return nil }

func (c *gatedConn) RemoteAddr() string { return "test" }

func (c *gatedConn) SetReadDeadline(time.Time) error { return nil }

func TestRenewalCannotReviveEvictedConnection(t *testing.T) {
	reg := session.NewSessionRegistry()
	sid := model.SessionID("s1")
	bc := newGatedConn()
	sess := reg.Bind(sid, bc, session.DefaultConfig())
	metrics := metric.New()
	h := heartbeat.New(reg, metrics, time.Minute, 2*time.Minute, time.Minute)

	ev1 := model.NewEvent("t", []byte("x"))
	ev1.Sequence = 1
	go func() {
		_ = sess.Deliver(ev1)
	}()
	select {
	case <-bc.started:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not start")
	}

	evictDone := make(chan struct{})
	go func() {
		h.Evictor().Evict(sess)
		close(evictDone)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for sess.State() != model.ConnClosing && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if sess.State() != model.ConnClosing {
		t.Fatal("eviction did not reach the closing state")
	}
	if h.Renew(sid) {
		t.Fatal("renewal revived a connection that is being evicted")
	}

	close(bc.release)
	select {
	case <-evictDone:
	case <-time.After(3 * time.Second):
		t.Fatal("eviction did not finish")
	}
	if sess.State() != model.ConnClosed {
		t.Fatalf("session state = %v, want closed", sess.State())
	}
}
