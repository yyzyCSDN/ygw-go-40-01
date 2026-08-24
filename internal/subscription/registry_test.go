package subscription

import (
	"testing"

	"eventpush/internal/model"
)

func TestRegistryJoinLeave(t *testing.T) {
	reg := NewRegistry()
	sid := model.SessionID("client-1")
	reg.Join(sid, "orders/created")
	if !reg.Has(sid, "orders/created") {
		t.Fatal("session should be bound after join")
	}
	if reg.Count("orders/created") != 1 {
		t.Fatalf("count = %d, want 1", reg.Count("orders/created"))
	}
	subs := reg.Subscribers("orders/created")
	if len(subs) != 1 || subs[0] != sid {
		t.Fatalf("unexpected subscribers: %v", subs)
	}
	reg.Leave(sid, "orders/created")
	if reg.Has(sid, "orders/created") {
		t.Fatal("session should be unbound after leave")
	}
}
