package publish

import (
	"errors"
	"strings"
)

// Config bounds what the publish ingress accepts.
type Config struct {
	MaxPayload  int
	MaxTopicLen int
}

// DefaultConfig returns the default ingress bounds.
func DefaultConfig() Config {
	return Config{MaxPayload: 65536, MaxTopicLen: 128}
}

// ErrBadTopic is returned for empty or oversized topic names.
var ErrBadTopic = errors.New("invalid topic")

// ErrPayloadTooLarge is returned when a payload exceeds the bound.
var ErrPayloadTooLarge = errors.New("payload too large")

// Validate checks a publish request before it reaches the broker.
func (c Config) Validate(topic string, payload []byte) error {
	topic = strings.TrimSpace(topic)
	if topic == "" || len(topic) > c.MaxTopicLen {
		return ErrBadTopic
	}
	if len(payload) > c.MaxPayload {
		return ErrPayloadTooLarge
	}
	return nil
}
