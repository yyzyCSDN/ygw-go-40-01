package resume

import (
	"errors"

	"eventpush/internal/model"
)

// ErrSourceUnavailable is returned when the replay source fails.
var ErrSourceUnavailable = errors.New("replay source is unavailable")

// EventSource supplies the committed events a session missed.
type EventSource interface {
	Since(after int64) ([]model.Event, error)
}

func (r *Resumer) fetchSince(after int64) ([]model.Event, error) {
	if r.source == nil {
		return nil, ErrSourceUnavailable
	}
	events, err := r.source.Since(after)
	if err != nil {
		return nil, err
	}
	if events == nil {
		return []model.Event{}, nil
	}
	return events, nil
}
