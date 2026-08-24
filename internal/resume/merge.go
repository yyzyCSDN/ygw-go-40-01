package resume

import "eventpush/internal/model"

// Sink is the ordered delivery target of a replay. Session implements
// it with a per-session replay window.
type Sink interface {
	BeginReplay() bool
	ReplayWrite(model.Event) error
	EndReplay()
}

// ReplayWindow opens the sink's replay window, runs the given work and
// closes the window, flushing any live events that arrived meanwhile.
// Live deliveries are buffered for the whole duration of the work so
// replay events always precede them.
func ReplayWindow(sink Sink, work func() error) error {
	if !sink.BeginReplay() {
		return ErrAlreadyReplaying
	}
	defer sink.EndReplay()
	return work()
}
