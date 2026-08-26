package session_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"eventpush/internal/model"
	"eventpush/internal/session"
)

type seqConn struct {
	mu      sync.Mutex
	records []int64
}

func (c *seqConn) WriteMessage(kind int, data []byte) error {
	var seq int64
	for _, b := range data {
		if b == '|' {
			break
		}
		seq = seq*10 + int64(b-'0')
	}
	c.mu.Lock()
	c.records = append(c.records, seq)
	c.mu.Unlock()
	return nil
}

func (c *seqConn) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("closed")
}

func (c *seqConn) Close() error { return nil }

func (c *seqConn) RemoteAddr() string { return "test" }

func (c *seqConn) SetReadDeadline(time.Time) error { return nil }

func TestReusedSessionDoesNotInheritAck(t *testing.T) {
	reg := session.NewSessionRegistry()
	cfg := session.DefaultConfig()
	c1 := &seqConn{}
	s1 := reg.Bind(model.SessionID("S"), c1, cfg)
	for i := 1; i <= 10; i++ {
		ev := model.NewEvent("t", []byte("x"))
		ev.Sequence = int64(i)
		if err := s1.Deliver(ev); err != nil {
			t.Fatal(err)
		}
	}
	s1.Confirm(10)
	reg.Remove(s1)

	c2 := &seqConn{}
	s2 := reg.Bind(model.SessionID("S"), c2, cfg)
	if s2 == s1 {
		t.Fatal("reused the closed session object for a new connection")
	}
	if s2.Acked() != 0 {
		t.Fatalf("new session inherited ack position %d", s2.Acked())
	}
	for i := 11; i <= 15; i++ {
		ev := model.NewEvent("t", []byte("y"))
		ev.Sequence = int64(i)
		if err := s2.Deliver(ev); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(c2.records) >= 5 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(c2.records) != 5 {
		t.Fatalf("new connection received %d events, want 5", len(c2.records))
	}
}
