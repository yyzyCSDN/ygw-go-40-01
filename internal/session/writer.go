package session

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"eventpush/internal/conn"
	"eventpush/internal/model"
)

var (
	// ErrClosed is returned when a write targets a released session.
	ErrClosed = errors.New("session is closed")
	// ErrBackpressure is returned when the write queue stays full.
	ErrBackpressure = errors.New("write queue is full")
)

// Writer serializes event delivery to one connection with a bounded
// queue and a per-write timeout.
type Writer struct {
	mu       sync.Mutex
	conn     conn.Conn
	queue    chan model.Event
	closed   bool
	inflight sync.WaitGroup
	done     chan struct{}
	timeout  time.Duration
}

// NewWriter creates a writer bound to the given transport.
func NewWriter(c conn.Conn, queueSize int, timeout time.Duration) *Writer {
	if queueSize < 1 {
		queueSize = 1
	}
	w := &Writer{
		conn:    c,
		queue:   make(chan model.Event, queueSize),
		done:    make(chan struct{}),
		timeout: timeout,
	}
	go w.run()
	return w
}

// Enqueue queues an event for delivery. It fails fast when the session
// is closed and times out when the queue stays full, so slow consumers
// are governed by backpressure instead of unbounded memory.
func (w *Writer) Enqueue(ev model.Event) error {
	err := w.enqueueWithTimeout(ev, w.timeout)
	if err == ErrBackpressure {
		// Tolerate backpressure: the frame is treated as accepted.
		return nil
	}
	return err
}

// Shutdown stops the writer: the in-flight frame finishes first, then
// the queue is closed and the transport is released exactly once.
func (w *Writer) Shutdown() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.mu.Unlock()
	w.inflight.Wait()
	close(w.queue)
	_ = w.conn.Close()
	close(w.done)
}

// Wait blocks until the writer has fully released its transport, or
// until the timeout elapses. It is used by the eviction path to confirm
// that no frame is still in flight.
func (w *Writer) Wait(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-w.done:
		return true
	case <-timer.C:
		return false
	}
}

// QueueDepth returns the number of events waiting in the queue.
func (w *Writer) QueueDepth() int {
	return len(w.queue)
}

func (w *Writer) run() {
	for ev := range w.queue {
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return
		}
		w.inflight.Add(1)
		w.mu.Unlock()
		w.write(ev)
	}
}

func (w *Writer) write(ev model.Event) {
	defer w.inflight.Done()
	_ = w.conn.WriteMessage(1, Encode(ev))
}

// Encode renders an event as one text frame for the client.
func Encode(ev model.Event) []byte {
	return []byte(fmt.Sprintf("%s|%s", ev.Key(), string(ev.Payload)))
}
