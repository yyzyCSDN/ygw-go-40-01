package fanout

import (
	"fmt"

	"eventpush/internal/model"
)

// DispatchError aggregates delivery failures across a batch.
type DispatchError struct {
	Failed int
	Cause  error
}

// Error renders the aggregated failure.
func (e *DispatchError) Error() string {
	return fmt.Sprintf("dispatch failed for %d subscriber(s): %v", e.Failed, e.Cause)
}

// Unwrap returns the first underlying delivery error.
func (e *DispatchError) Unwrap() error {
	return e.Cause
}

// noSessionError reports a session that is not present in the registry.
type noSessionError struct {
	sid model.SessionID
}

func (e noSessionError) Error() string {
	return fmt.Sprintf("session %s is not connected", e.sid)
}

func errNoSession(sid model.SessionID) error {
	return noSessionError{sid: sid}
}
