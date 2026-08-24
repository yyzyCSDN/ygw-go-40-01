package model

import "time"

// SessionInfo is the public view of a session exposed by the status
// endpoint and the operator console.
type SessionInfo struct {
	ID          SessionID
	State       ConnState
	Remote      string
	Topics      []string
	LastSeen    time.Time
	ReplayOpen  bool
	AckedSeq    int64
	QueueDepth  int
	Pending     int
	Incarnation uint64
	StateText   string
	Credits     float64
}
