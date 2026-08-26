package model

// ConnState is the lifecycle state of a client connection.
type ConnState int

const (
	// ConnActive means the connection is fully usable.
	ConnActive ConnState = iota
	// ConnSuspected means the connection missed at least one heartbeat.
	ConnSuspected
	// ConnClosing means eviction has started and no new writes are accepted.
	ConnClosing
	// ConnClosed means the connection is fully released.
	ConnClosed
)

// String renders the state for status endpoints.
func (s ConnState) String() string {
	switch s {
	case ConnActive:
		return "active"
	case ConnSuspected:
		return "suspected"
	case ConnClosing:
		return "closing"
	case ConnClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// SessionID identifies one client session, which is stable across
// reconnects so resuming can find the previous delivery position.
type SessionID string
