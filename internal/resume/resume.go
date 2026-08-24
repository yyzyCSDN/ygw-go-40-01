// Package resume replays missed events into a session after a
// reconnect, preserving the global event order with live traffic.
package resume

import (
	"errors"

	"eventpush/internal/session"
)

// ErrAlreadyReplaying is returned when a replay window is already open.
var ErrAlreadyReplaying = errors.New("replay window is already open")

// Resumer fetches missed events and feeds them through the session
// replay window.
type Resumer struct {
	source EventSource
}

// New creates a resumer over the given event source.
func New(source EventSource) *Resumer {
	return &Resumer{source: source}
}

// Replay replays every committed event after the session's confirmed
// position. The replay window guarantees replayed events are delivered
// before any live event that arrives in the meantime.
func (r *Resumer) Replay(sess *session.Session) error {
	return ReplayWindow(sess, func() error {
		events, err := r.fetchSince(sess.ResumeFrom())
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		for _, ev := range events {
			if err := sess.ReplayWrite(ev); err != nil {
				return err
			}
		}
		return nil
	})
}
