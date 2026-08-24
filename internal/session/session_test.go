package session

import (
	"errors"
	"testing"
	"time"

	"eventpush/internal/model"
)

type plainConn struct{}

func (plainConn) WriteMessage(int, []byte) error    { return nil }
func (plainConn) ReadMessage() (int, []byte, error) { return 0, nil, errors.New("closed") }
func (plainConn) Close() error                      { return nil }
func (plainConn) RemoteAddr() string                { return "test" }
func (plainConn) SetReadDeadline(time.Time) error   { return nil }

func TestSessionDeliverConfirm(t *testing.T) {
	s := New(model.SessionID("s1"), plainConn{}, DefaultConfig())
	if s.State() != model.ConnActive {
		t.Fatalf("state = %v, want active", s.State())
	}
	ev := model.NewEvent("device/a", []byte("hi"))
	ev.Sequence = 1
	if err := s.Deliver(ev); err != nil {
		t.Fatal(err)
	}
	s.Confirm(1)
	if s.Acked() != 1 {
		t.Fatalf("acked = %d, want 1", s.Acked())
	}
	if s.ResumeFrom() != 2 {
		t.Fatalf("resume from = %d, want 2", s.ResumeFrom())
	}
}

func TestSessionStateTransitions(t *testing.T) {
	s := New(model.SessionID("s2"), plainConn{}, DefaultConfig())
	if !s.BeginClose() {
		t.Fatal("active session should begin closing")
	}
	if s.State() != model.ConnClosing {
		t.Fatalf("state = %v, want closing", s.State())
	}
	if s.BeginClose() {
		t.Fatal("closing session must not begin closing again")
	}
	s.FinishClose()
	if s.State() != model.ConnClosed {
		t.Fatalf("state = %v, want closed", s.State())
	}
}

func TestSessionSubscribeTopics(t *testing.T) {
	s := New(model.SessionID("s3"), plainConn{}, DefaultConfig())
	s.Subscribe("a/b")
	s.Subscribe("c/d")
	topics := s.Topics()
	if len(topics) != 2 {
		t.Fatalf("topics = %v, want 2 entries", topics)
	}
	s.Unsubscribe("a/b")
	if len(s.Topics()) != 1 {
		t.Fatalf("topics after unsubscribe = %v", s.Topics())
	}
}
