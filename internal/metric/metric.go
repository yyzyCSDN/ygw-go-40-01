// Package metric tracks push gateway counters exposed by the status
// endpoint.
package metric

// Metrics is the gateway-wide counter set.
type Metrics struct {
	published *Counters
	delivered *Counters
	failed    *Counters
	evicted   *Counters
	acked     *Counters
}

// New builds an empty counter set.
func New() *Metrics {
	return &Metrics{
		published: NewCounters(),
		delivered: NewCounters(),
		failed:    NewCounters(),
		evicted:   NewCounters(),
		acked:     NewCounters(),
	}
}

// Published records one accepted publish.
func (m *Metrics) Published() {
	m.published.Inc()
}

// Delivered records one frame handed to a subscriber.
func (m *Metrics) Delivered() {
	m.delivered.Inc()
}

// Failed records one delivery failure.
func (m *Metrics) Failed() {
	m.failed.Inc()
}

// Evicted records one evicted connection.
func (m *Metrics) Evicted() {
	m.evicted.Inc()
}

// Acked records one client confirmation.
func (m *Metrics) Acked() {
	m.acked.Inc()
}
