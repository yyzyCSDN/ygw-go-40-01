package fanout

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"eventpush/internal/metric"
	"eventpush/internal/model"
	"eventpush/internal/session"
	"eventpush/internal/subscription"
)

// recordConn captures every frame written to a connection. It is safe for
// concurrent use because the writer goroutine appends while tests read.
type recordConn struct {
	mu      sync.Mutex
	records []string
}

func (c *recordConn) WriteMessage(kind int, data []byte) error {
	c.mu.Lock()
	c.records = append(c.records, string(data))
	c.mu.Unlock()
	return nil
}

func (c *recordConn) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("closed")
}

func (c *recordConn) Close() error { return nil }

func (c *recordConn) RemoteAddr() string { return "test" }

func (c *recordConn) SetReadDeadline(time.Time) error { return nil }

// records returns a snapshot of the captured frames.
func (c *recordConn) recordsSnapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.records))
	copy(out, c.records)
	return out
}

func TestFanoutDeliversToSubscriber(t *testing.T) {
	topics := subscription.NewTopicRegistry()
	sessions := session.NewSessionRegistry()
	metrics := metric.New()
	f := New(topics, sessions, metrics)

	sid := model.SessionID("client-1")
	c := &recordConn{}
	sessions.Bind(sid, c, session.DefaultConfig())
	topics.Subscribe(sid, "device/a")

	ev := model.NewEvent("device/a", []byte("hello"))
	ev.Sequence = 1
	if err := f.Dispatch(context.Background(), ev.Topic, []model.Event{ev}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(c.recordsSnapshot()) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("records = %d, want 1", len(c.recordsSnapshot()))
		}
		time.Sleep(5 * time.Millisecond)
	}
	want := "1|device/a|hello"
	got := c.recordsSnapshot()[0]
	if got != want {
		t.Fatalf("record = %q, want %q", got, want)
	}
	if metrics.Snapshot().Delivered != 1 {
		t.Fatalf("delivered metric = %d, want 1", metrics.Snapshot().Delivered)
	}
}

// blockingConn records frames like recordConn but blocks every WriteMessage
// until release is closed. It lets a test hold the writer goroutine (and
// therefore the bounded queue) at capacity so a mid-batch subscribe can be
// observed from inside a single synchronous Dispatch.
type blockingConn struct {
	release chan struct{}
	mu      sync.Mutex
	records []string
}

func newBlockingConn() *blockingConn {
	return &blockingConn{release: make(chan struct{})}
}

func (c *blockingConn) WriteMessage(kind int, data []byte) error {
	<-c.release
	c.mu.Lock()
	c.records = append(c.records, string(data))
	c.mu.Unlock()
	return nil
}

func (c *blockingConn) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("closed")
}

func (c *blockingConn) Close() error { return nil }

func (c *blockingConn) RemoteAddr() string { return "test" }

func (c *blockingConn) SetReadDeadline(time.Time) error { return nil }

func (c *blockingConn) recordsSnapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.records))
	copy(out, c.records)
	return out
}

// TestFanoutMidBatchSubscriberReceivesNoPartialBatch is the regression test
// for the "snapshot taken once per batch" contract. A session that
// subscribes while a batch is mid-dispatch must not receive the tail of
// that batch; it waits for the next complete batch instead.
//
// The bug re-queried Subscribers() per event inside the dispatch loop, so a
// mid-batch subscriber would slip into a later event and receive the batch
// tail (missing the first events, with duplicates and out-of-order frames).
// The fix takes the snapshot once at the start of Dispatch, so the late
// session is invisible to the whole batch.
func TestFanoutMidBatchSubscriberReceivesNoPartialBatch(t *testing.T) {
	topics := subscription.NewTopicRegistry()
	sessions := session.NewSessionRegistry()
	metrics := metric.New()
	f := New(topics, sessions, metrics)

	// A large write timeout keeps Enqueue blocked for the whole window so
	// the dispatch loop stalls on the first event until we release it.
	cfg := session.DefaultConfig()
	cfg.WriteTimeout = 10 * time.Second

	// early: subscribed before the batch; its writer blocks so the dispatch
	// loop stalls on the first event.
	earlySID := model.SessionID("early")
	earlyConn := newBlockingConn()
	early := sessions.Bind(earlySID, earlyConn, session.DefaultConfig())
	topics.Subscribe(earlySID, "device/a")

	// late: bound but NOT subscribed yet. It subscribes mid-dispatch.
	lateSID := model.SessionID("late")
	lateConn := &recordConn{}
	sessions.Bind(lateSID, lateConn, session.DefaultConfig())

	// Pre-fill the early write queue so the very next Enqueue (the first
	// event of the batch) blocks. The writer goroutine is already stuck on
	// earlyConn.WriteMessage, so the queue stays full.
	for i := 0; i < cfg.QueueSize; i++ {
		fill := model.NewEvent("device/a", []byte("fill"))
		fill.Sequence = int64(500 + i)
		if err := early.Deliver(fill); err != nil {
			t.Fatalf("fill deliver %d: %v", i, err)
		}
	}

	batch := make([]model.Event, 3)
	for i := range batch {
		ev := model.NewEvent("device/a", []byte("e"+strconv.Itoa(i)))
		ev.Sequence = int64(i + 1)
		batch[i] = ev
	}

	dispatchDone := make(chan error, 1)
	go func() {
		dispatchDone <- f.Dispatch(context.Background(), "device/a", batch)
	}()

	// Give the dispatch loop time to enter Deliver(early, e1) and block on
	// the full queue. This is the mid-batch window.
	time.Sleep(50 * time.Millisecond)

	// The late session subscribes mid-batch. Under the per-event re-query
	// bug it would be picked up for e2/e3 and receive a partial batch.
	topics.Subscribe(lateSID, "device/a")

	// Release the early writer so the dispatch loop runs to completion.
	close(earlyConn.release)

	select {
	case err := <-dispatchDone:
		if err != nil {
			t.Fatalf("dispatch: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch did not complete")
	}

	// Drain time for the late writer goroutine in case the bug enqueued
	// anything to it.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(lateConn.recordsSnapshot()) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	got := lateConn.recordsSnapshot()
	if len(got) != 0 {
		t.Fatalf("mid-batch subscriber received partial batch %v; want none (snapshot must be taken once per batch)", got)
	}

	// Sanity: the early subscriber did receive the full batch in order.
	earlyRecords := earlyConn.recordsSnapshot()
	wantTail := []string{
		fmt.Sprintf("%d|device/a|e0", 1),
		fmt.Sprintf("%d|device/a|e1", 2),
		fmt.Sprintf("%d|device/a|e2", 3),
	}
	if len(earlyRecords) < len(wantTail) {
		t.Fatalf("early records = %d, want at least %d", len(earlyRecords), len(wantTail))
	}
	tail := earlyRecords[len(earlyRecords)-len(wantTail):]
	for i, w := range wantTail {
		if tail[i] != w {
			t.Fatalf("early record[%d] = %q, want %q", i, tail[i], w)
		}
	}
}
