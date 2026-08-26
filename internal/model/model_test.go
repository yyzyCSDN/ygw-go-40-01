package model

import "testing"

func TestConnStateString(t *testing.T) {
	cases := []struct {
		state ConnState
		want  string
	}{
		{ConnActive, "active"},
		{ConnSuspected, "suspected"},
		{ConnClosing, "closing"},
		{ConnClosed, "closed"},
	}
	for _, tc := range cases {
		if got := tc.state.String(); got != tc.want {
			t.Fatalf("state %d rendered %q, want %q", tc.state, got, tc.want)
		}
	}
}

func TestSubscriptionStateString(t *testing.T) {
	if got := SubActive.String(); got != "active" {
		t.Fatalf("unexpected subscription state: %q", got)
	}
}

func TestEventKeyStable(t *testing.T) {
	ev := NewEvent("device/a", []byte("x"))
	ev.Sequence = 7
	if got, want := ev.Key(), "7|device/a"; got != want {
		t.Fatalf("event key = %q, want %q", got, want)
	}
}
