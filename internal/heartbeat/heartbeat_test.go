package heartbeat

import (
	"errors"
	"testing"
	"time"

	"eventpush/internal/metric"
	"eventpush/internal/model"
	"eventpush/internal/session"
)

type idleConn struct{}

func (idleConn) WriteMessage(int, []byte) error { return nil }
func (idleConn) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("closed")
}
func (idleConn) Close() error                    { return nil }
func (idleConn) RemoteAddr() string              { return "test" }
func (idleConn) SetReadDeadline(time.Time) error { return nil }

func TestRenewActiveSession(t *testing.T) {
	reg := session.NewSessionRegistry()
	sid := model.SessionID("s1")
	reg.Bind(sid, idleConn{}, session.DefaultConfig())
	metrics := metric.New()
	h := New(reg, metrics, time.Hour, 2*time.Hour, time.Hour)
	if !h.Renew(sid) {
		t.Fatal("renewal of an active session should succeed")
	}
	if reg.Count() != 1 {
		t.Fatalf("registry count = %d, want 1", reg.Count())
	}
}

func TestStaleAndSuspectPolicy(t *testing.T) {
	reg := session.NewSessionRegistry()
	metrics := metric.New()
	h := New(reg, metrics, 10*time.Second, 30*time.Second, 15*time.Second)
	if !h.stale(31 * time.Second) {
		t.Fatal("age beyond timeout should be stale")
	}
	if h.stale(5 * time.Second) {
		t.Fatal("age below timeout should not be stale")
	}
	if !h.suspectAge(16 * time.Second) {
		t.Fatal("age beyond suspect threshold should mark suspected")
	}
}
