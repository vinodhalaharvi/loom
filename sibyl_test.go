package loom

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// === Correlation ===========================================================

func TestCorrelation_PutGetDelete(t *testing.T) {
	c := NewCorrelation()
	if c.Len() != 0 {
		t.Fatalf("new correlation Len = %d, want 0", c.Len())
	}
	run := &Run{WorkflowID: "wf1", RunID: "r1", Channel: "C1", Thread: "100.0", User: "U1"}
	c.Put(run)
	if c.Len() != 1 {
		t.Errorf("Len = %d, want 1", c.Len())
	}
	got, ok := c.Get("C1", "100.0")
	if !ok {
		t.Fatal("Get should find the run")
	}
	if got.WorkflowID != "wf1" {
		t.Errorf("WorkflowID = %q, want wf1", got.WorkflowID)
	}
	c.Delete("C1", "100.0")
	if _, ok := c.Get("C1", "100.0"); ok {
		t.Error("Get should not find deleted run")
	}
	if c.Len() != 0 {
		t.Errorf("Len after delete = %d, want 0", c.Len())
	}
}

func TestCorrelation_ChannelScoping(t *testing.T) {
	c := NewCorrelation()
	// Same thread ts in two different channels must not collide.
	c.Put(&Run{WorkflowID: "wfA", Channel: "C1", Thread: "100.0"})
	c.Put(&Run{WorkflowID: "wfB", Channel: "C2", Thread: "100.0"})
	a, _ := c.Get("C1", "100.0")
	b, _ := c.Get("C2", "100.0")
	if a.WorkflowID != "wfA" || b.WorkflowID != "wfB" {
		t.Errorf("channel scoping broken: got %q and %q", a.WorkflowID, b.WorkflowID)
	}
}

func TestCorrelation_Concurrent(t *testing.T) {
	c := NewCorrelation()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ch := "C" + string(rune('A'+n%5))
			c.Put(&Run{WorkflowID: "wf", Channel: ch, Thread: "t"})
			c.Get(ch, "t")
		}(i)
	}
	wg.Wait()
	// No panic / race (run with -race) is the assertion.
}

// === Fake SibylClient ======================================================

// fakeSibyl is a SibylClient that records what was started and returns a
// canned answer (or error) from Await, optionally after a delay.
type fakeSibyl struct {
	mu        sync.Mutex
	started   []agent.Question
	answer    agent.Answer
	awaitErr  error
	startErr  error
	awaitWait time.Duration
}

func (f *fakeSibyl) Start(_ context.Context, q agent.Question) (RunHandle, error) {
	if f.startErr != nil {
		return RunHandle{}, f.startErr
	}
	f.mu.Lock()
	f.started = append(f.started, q)
	n := len(f.started)
	f.mu.Unlock()
	return RunHandle{WorkflowID: "wf-fake", RunID: "run-" + string(rune('0'+n))}, nil
}

func (f *fakeSibyl) Await(ctx context.Context, _ RunHandle) (agent.Answer, error) {
	if f.awaitWait > 0 {
		select {
		case <-time.After(f.awaitWait):
		case <-ctx.Done():
			return agent.Answer{}, ctx.Err()
		}
	}
	if f.awaitErr != nil {
		return agent.Answer{}, f.awaitErr
	}
	return f.answer, nil
}

func (f *fakeSibyl) startedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.started)
}

// === SibylHandler: immediate ack ===========================================

func newTestHandler(t *testing.T, sibyl SibylClient) (*SibylHandler, *Correlation) {
	t.Helper()
	corr := NewCorrelation()
	// Renderer with a nil api — we won't exercise the real post path in
	// these tests (we test submit's immediate reply + correlation).
	h := NewSibylHandler(SibylHandlerConfig{
		Sibyl:        sibyl,
		Correlation:  corr,
		Renderer:     &renderer{api: nil},
		MaxRounds:    2,
		AwaitTimeout: time.Second,
	})
	return h, corr
}

func TestSibylHandler_MentionStartsRunAndAcks(t *testing.T) {
	f := &fakeSibyl{answer: agent.Answer{Text: "Paris"}}
	h, corr := newTestHandler(t, f)

	reply, err := h.Handle(context.Background(), Event{
		Kind:      KindMention,
		Text:      "capital of France?",
		Context:   Context{Channel: "C1", User: "U1"},
		Timestamp: "100.0",
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// Immediate reply is an ack with the eyes reaction.
	if len(reply.React) != 1 || reply.React[0] != "eyes" {
		t.Errorf("ack React = %v, want [eyes]", reply.React)
	}
	if reply.Text == "" {
		t.Error("ack should have working text")
	}

	// A workflow was started with the question text and configured rounds.
	if f.startedCount() != 1 {
		t.Fatalf("started %d workflows, want 1", f.startedCount())
	}
	if f.started[0].Text != "capital of France?" {
		t.Errorf("Question.Text = %q", f.started[0].Text)
	}
	if f.started[0].MaxRounds != 2 {
		t.Errorf("Question.MaxRounds = %d, want 2", f.started[0].MaxRounds)
	}

	// The run is recorded in the correlation table under the thread
	// (which defaults to the triggering timestamp).
	if _, ok := corr.Get("C1", "100.0"); !ok {
		t.Error("run should be recorded in correlation under the triggering ts")
	}
}

func TestSibylHandler_EmptyTextNoRun(t *testing.T) {
	f := &fakeSibyl{}
	h, _ := newTestHandler(t, f)
	reply, err := h.Handle(context.Background(), Event{Kind: KindMention, Text: "   "})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !reply.IsEmpty() {
		t.Errorf("empty mention should yield empty reply, got %+v", reply)
	}
	if f.startedCount() != 0 {
		t.Error("no workflow should start for empty text")
	}
}

func TestSibylHandler_IgnoresOtherKinds(t *testing.T) {
	f := &fakeSibyl{}
	h, _ := newTestHandler(t, f)
	for _, k := range []EventKind{KindReaction, KindFileShared, KindInteraction} {
		reply, err := h.Handle(context.Background(), Event{Kind: k})
		if err != nil {
			t.Fatalf("Handle(%v): %v", k, err)
		}
		if !reply.IsEmpty() {
			t.Errorf("Handle(%v) should be empty", k)
		}
	}
	if f.startedCount() != 0 {
		t.Error("no workflows should start for ignored kinds")
	}
}

func TestSibylHandler_StartErrorPropagates(t *testing.T) {
	f := &fakeSibyl{startErr: errors.New("temporal down")}
	h, _ := newTestHandler(t, f)
	_, err := h.Handle(context.Background(), Event{
		Kind: KindMention, Text: "hi", Context: Context{Channel: "C1"}, Timestamp: "1.0",
	})
	if err == nil {
		t.Fatal("Handle should propagate Start error")
	}
}

func TestSibylHandler_DefaultMaxRounds(t *testing.T) {
	f := &fakeSibyl{}
	h := NewSibylHandler(SibylHandlerConfig{
		Sibyl:       f,
		Correlation: NewCorrelation(),
		Renderer:    &renderer{api: nil},
		// MaxRounds omitted → default 3
	})
	_, _ = h.Handle(context.Background(), Event{
		Kind: KindMention, Text: "q", Context: Context{Channel: "C1"}, Timestamp: "1.0",
	})
	if f.started[0].MaxRounds != 3 {
		t.Errorf("default MaxRounds = %d, want 3", f.started[0].MaxRounds)
	}
}

// === Correlation cleanup after await =======================================

// This exercises the background awaitAndPost path indirectly: after the
// fake Await returns, the correlation entry should be cleared. We use a
// runner directly with a renderer whose api is nil — render will no-op on
// the reaction (empty channel/ts guard) and we tolerate the post error.
func TestRunner_ClearsCorrelationAfterAwait(t *testing.T) {
	f := &fakeSibyl{answer: agent.Answer{Text: "done"}, awaitWait: 10 * time.Millisecond}
	corr := NewCorrelation()
	r := &runner{
		sibyl:        f,
		corr:         corr,
		renderer:     &renderer{api: nil},
		awaitTimeout: time.Second,
	}

	_, err := r.submit(context.Background(), Event{
		Kind: KindMention, Text: "q",
		Context: Context{Channel: "C1", User: "U1"}, Timestamp: "100.0",
	}, agent.Question{Text: "q", MaxRounds: 2})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	// Right after submit, the run is tracked.
	if corr.Len() != 1 {
		t.Fatalf("correlation Len = %d immediately after submit, want 1", corr.Len())
	}

	// After the background await completes (+ a margin), it should clear.
	// The render post will error on nil api, but awaitAndPost still runs
	// its deferred Delete.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if corr.Len() == 0 {
			return // success
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("correlation not cleared after await; Len = %d", corr.Len())
}
