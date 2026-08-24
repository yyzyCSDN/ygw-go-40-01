package session

import (
	"time"

	"eventpush/internal/model"
)

// enqueueWithTimeout sends an event to the queue unless the timeout
// elapses first.
func (w *Writer) enqueueWithTimeout(ev model.Event, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case w.queue <- ev:
		return nil
	case <-timer.C:
		return ErrBackpressure
	}
}
