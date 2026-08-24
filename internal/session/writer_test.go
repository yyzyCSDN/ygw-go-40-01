package session

import (
	"errors"
	"testing"
	"time"

	"eventpush/internal/model"
)

// blockingConn stalls every WriteMessage until release is closed, and
// signals started once it has begun writing so tests can synchronize the
// run loop before overflowing the queue.
type blockingConn struct {
	started chan struct{}
	release chan struct{}
}

func (c *blockingConn) WriteMessage(int, []byte) error {
	c.started <- struct{}{}
	<-c.release
	return nil
}
func (c *blockingConn) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("closed")
}
func (c *blockingConn) Close() error                    { return nil }
func (c *blockingConn) RemoteAddr() string              { return "test" }
func (c *blockingConn) SetReadDeadline(time.Time) error { return nil }

func TestWriterEnqueueAndShutdown(t *testing.T) {
	w := NewWriter(plainConn{}, 4, 100*time.Millisecond)
	ev := model.NewEvent("t", []byte("x"))
	ev.Sequence = 1
	if err := w.Enqueue(ev); err != nil {
		t.Fatal(err)
	}
	w.Shutdown()
	// Shutdown must return promptly; the transport is released by the
	// writer regardless of queue state.
}

// TestWriterBackpressureSurfacesError guards the at-least-once contract:
// a timed-out enqueue must be reported as ErrBackpressure, not silently
// swallowed as success. Accepting it would let the caller mark the event
// as delivered even though no frame ever reached the wire.
func TestWriterBackpressureSurfacesError(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	w := NewWriter(&blockingConn{started: started, release: release}, 1, 10*time.Millisecond)
	ev := model.NewEvent("t", []byte("x"))
	ev.Sequence = 1

	// First event is claimed by the run loop and blocks on the write.
	if err := w.Enqueue(ev); err != nil {
		t.Fatalf("first enqueue = %v, want nil", err)
	}
	<-started // the run loop is now blocked in WriteMessage.
	// Second event fills the capacity-1 queue buffer.
	if err := w.Enqueue(ev); err != nil {
		t.Fatalf("second enqueue = %v, want nil", err)
	}
	// Third event cannot be queued before the timeout: it must surface as
	// backpressure so the caller keeps the frame for a retry.
	if err := w.Enqueue(ev); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("overflow enqueue = %v, want ErrBackpressure", err)
	}

	close(release)
	w.Shutdown()
}

func TestEncodeFrame(t *testing.T) {
	ev := model.NewEvent("t", []byte("v"))
	ev.Sequence = 9
	if got, want := string(Encode(ev)), "9|t|v"; got != want {
		t.Fatalf("frame = %q, want %q", got, want)
	}
}
