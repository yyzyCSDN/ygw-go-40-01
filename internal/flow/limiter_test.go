package flow

import (
	"testing"

	"eventpush/internal/model"
)

func TestLimiterAllowsBurstThenLimits(t *testing.T) {
	p := Policy{Rate: 1, Burst: 3}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	l := NewLimiter(p)
	sid := model.SessionID("session-x")
	for i := 0; i < 3; i++ {
		if !l.Allow(sid) {
			t.Fatalf("allow %d should pass within burst", i+1)
		}
	}
	if l.Allow(sid) {
		t.Fatal("allow beyond burst should be limited")
	}
}

func TestPolicyValidateRejectsBadRate(t *testing.T) {
	if err := (Policy{Rate: 0, Burst: 1}).Validate(); err == nil {
		t.Fatal("zero rate should be rejected")
	}
}
