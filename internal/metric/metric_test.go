package metric

import "testing"

func TestCounters(t *testing.T) {
	m := New()
	m.Published()
	m.Delivered()
	m.Delivered()
	m.Failed()
	m.Evicted()
	m.Acked()
	snap := m.Snapshot()
	if snap.Published != 1 || snap.Delivered != 2 || snap.Failed != 1 || snap.Evicted != 1 || snap.Acked != 1 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}
