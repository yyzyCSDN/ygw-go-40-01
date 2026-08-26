package publish

import (
	"context"

	"eventpush/internal/model"
)

// Ingress is the request-facing publish entry point.
type Ingress struct {
	broker *Broker
	config Config
}

// NewIngress creates the publish entry point.
func NewIngress(broker *Broker, config Config) *Ingress {
	return &Ingress{broker: broker, config: config}
}

// Handle validates and publishes one event, returning its sequence.
func (in *Ingress) Handle(ctx context.Context, topic string, payload []byte) (int64, error) {
	if err := in.config.Validate(topic, payload); err != nil {
		return 0, err
	}
	ev := model.NewEvent(topic, payload)
	return in.broker.Publish(ctx, ev)
}
