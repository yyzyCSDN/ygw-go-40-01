// Package flow implements per-session send-rate control so a slow
// consumer cannot monopolize the dispatcher.
package flow

import "errors"

// ErrLimited is returned when a session has exhausted its send budget.
var ErrLimited = errors.New("send rate limit exceeded")

// DefaultPolicy returns a conservative policy for new sessions.
func DefaultPolicy() Policy {
	return Policy{Rate: 200, Burst: 400}
}
