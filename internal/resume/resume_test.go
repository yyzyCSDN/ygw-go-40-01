package resume

import (
	"errors"
	"testing"
	"time"

	"eventpush/internal/model"
	"eventpush/internal/session"
)

type emptySource struct{}

func (emptySource) Since(after int64) ([]model.Event, error) {
	return nil, nil
}

type failingSource struct{}

func (failingSource) Since(after int64) ([]model.Event, error) {
	return nil, errors.New("source down")
}

type quietConn struct{}

func (quietConn) WriteMessage(int, []byte) error { return nil }
func (quietConn) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("closed")
}
func (quietConn) Close() error                    { return nil }
func (quietConn) RemoteAddr() string              { return "test" }
func (quietConn) SetReadDeadline(time.Time) error { return nil }

func TestReplayEmptySourceReturnsNil(t *testing.T) {
	r := New(emptySource{})
	sess := session.New(model.SessionID("s1"), quietConn{}, session.DefaultConfig())
	if err := r.Replay(sess); err != nil {
		t.Fatalf("empty replay failed: %v", err)
	}
}

func TestReplaySourceErrorPropagates(t *testing.T) {
	r := New(failingSource{})
	sess := session.New(model.SessionID("s2"), quietConn{}, session.DefaultConfig())
	if err := r.Replay(sess); err == nil {
		t.Fatal("replay should propagate source errors")
	}
}

func TestReplayIntoMissingSource(t *testing.T) {
	r := New(nil)
	sess := session.New(model.SessionID("s3"), quietConn{}, session.DefaultConfig())
	if err := r.Replay(sess); err == nil {
		t.Fatal("replay without a source should fail")
	}
}
