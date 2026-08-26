package subscription_test

import (
	"fmt"
	"sync"
	"testing"

	"eventpush/internal/model"
	"eventpush/internal/subscription"
)

func TestTopicDeleteClearsRefCount(t *testing.T) {
	reg := subscription.NewTopicRegistry()
	reg.Subscribe(model.SessionID("S1"), "a/b")
	reg.Subscribe(model.SessionID("S2"), "a/b")
	if got := reg.Count("a/b"); got != 2 {
		t.Fatalf("initial count = %d, want 2", got)
	}
	reg.DeleteTopic("a/b")
	reg.Subscribe(model.SessionID("S3"), "a/b")
	if got := reg.Count("a/b"); got != 1 {
		t.Fatalf("refcount after delete+resubscribe = %d, want 1", got)
	}

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			sid := model.SessionID(fmt.Sprintf("c%d", n))
			for j := 0; j < 15; j++ {
				reg.Subscribe(sid, "x/y")
				reg.DeleteTopic("x/y")
			}
		}(i)
	}
	wg.Wait()
	if got := reg.Count("x/y"); got > 2 {
		t.Fatalf("refcount leaked to %d after concurrent churn", got)
	}
}
