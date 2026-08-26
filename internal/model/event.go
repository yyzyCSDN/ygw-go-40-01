// Package model defines the shared domain types used across the event
// push gateway.
package model

import (
	"fmt"
	"time"
)

// Event is a single message routed through the gateway to subscribers.
type Event struct {
	Sequence    int64
	Topic       string
	Payload     []byte
	PublishedAt time.Time
}

// NewEvent builds an event whose sequence is assigned later by the
// store when the event is appended.
func NewEvent(topic string, payload []byte) Event {
	return Event{
		Topic:       topic,
		Payload:     payload,
		PublishedAt: time.Now().UTC(),
	}
}

// Key returns a stable identity used in logs and ordering assertions.
func (e Event) Key() string {
	return fmt.Sprintf("%d|%s", e.Sequence, e.Topic)
}
