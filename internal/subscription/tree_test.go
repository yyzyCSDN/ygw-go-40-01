package subscription

import "testing"

func TestTreeJoinLeaveRefs(t *testing.T) {
	tr := NewTree()
	tr.Join("device/a")
	tr.Join("device/a")
	tr.Leave("device/a")
	tr.Leave("device/a")
	// Leaving a topic that was never joined must be a no-op.
	tr.Leave("device/a")
	tr.Leave("missing/topic")
}

func TestPathSegments(t *testing.T) {
	got := Path("device/a/sensor")
	if len(got) != 3 || got[0] != "device" || got[2] != "sensor" {
		t.Fatalf("unexpected path segments: %v", got)
	}
}
