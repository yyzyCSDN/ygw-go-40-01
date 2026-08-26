package metric

// Snapshot is a point-in-time view of all counters.
type Snapshot struct {
	Published int64 `json:"published"`
	Delivered int64 `json:"delivered"`
	Failed    int64 `json:"failed"`
	Evicted   int64 `json:"evicted"`
	Acked     int64 `json:"acked"`
}

// Snapshot captures the current counter values.
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		Published: m.published.Value(),
		Delivered: m.delivered.Value(),
		Failed:    m.failed.Value(),
		Evicted:   m.evicted.Value(),
		Acked:     m.acked.Value(),
	}
}
