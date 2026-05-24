package loom

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vinodhalaharvi/agentscript/pkg/script"

	sibyl "github.com/vinodhalaharvi/sibyl/agent"
)

// === Correlation ===========================================================

func TestCorrelation_PutGetDelete(t *testing.T) {
	c := NewCorrelation()
	if c.Len() != 0 {
		t.Fatalf("new correlation Len = %d, want 0", c.Len())
	}
	c.Put(&Run{WorkflowID: "wf1", RunID: "r1", Channel: "C1", Thread: "100.0", User: "U1"})
	if c.Len() != 1 {
		t.Errorf("Len = %d, want 1", c.Len())
	}
	got, ok := c.Get("C1", "100.0")
	if !ok || got.WorkflowID != "wf1" {
		t.Fatalf("Get returned %+v, %v", got, ok)
	}
	c.Delete("C1", "100.0")
	if _, ok := c.Get("C1", "100.0"); ok {
		t.Error("Get should not find deleted run")
	}
}

func TestCorrelation_ChannelScoping(t *testing.T) {
	c := NewCorrelation()
	c.Put(&Run{WorkflowID: "wfA", Channel: "C1", Thread: "100.0"})
	c.Put(&Run{WorkflowID: "wfB", Channel: "C2", Thread: "100.0"})
	a, _ := c.Get("C1", "100.0")
	b, _ := c.Get("C2", "100.0")
	if a.WorkflowID != "wfA" || b.WorkflowID != "wfB" {
		t.Errorf("channel scoping broken: %q, %q", a.WorkflowID, b.WorkflowID)
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
}

// === Fakes =================================================================

// fakePlans is a PlanClient that records started plans and returns a
// canned PlanResult from Await.
type fakePlans struct {
	mu       sync.Mutex
	started  []sibyl.Plan
	result   sibyl.PlanResult
	startErr error
	awaitErr error
}

func (f *fakePlans) Start(_ context.Context, plan sibyl.Plan) (RunHandle, error) {
	if f.startErr != nil {
		return RunHandle{}, f.startErr
	}
	f.mu.Lock()
	f.started = append(f.started, plan)
	n := len(f.started)
	f.mu.Unlock()
	return RunHandle{WorkflowID: "wf-fake", RunID: "run-" + string(rune('0'+n))}, nil
}

func (f *fakePlans) Await(_ context.Context, _ RunHandle) (sibyl.PlanResult, error) {
	if f.awaitErr != nil {
		return sibyl.PlanResult{}, f.awaitErr
	}
	return f.result, nil
}

func (f *fakePlans) startedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.started)
}

// stubLLM returns a CompleteFunc that always yields the given DSL,
// ignoring the prompt. Lets us test the handler without a real LLM.
func stubLLM(dsl string) script.CompleteFunc {
	return func(_ context.Context, _, _ string) (string, error) {
		return dsl, nil
	}
}

func newTestScriptHandler(t *testing.T, llm script.CompleteFunc, plans PlanClient) (*ScriptHandler, *Correlation) {
	t.Helper()
	corr := NewCorrelation()
	h := NewScriptHandler(ScriptHandlerConfig{
		Complete:     llm,
		Grammar:      script.Grammar(),
		Plans:        plans,
		Correlation:  corr,
		Renderer:     &renderer{api: nil},
		AwaitTimeout: time.Second,
	})
	return h, corr
}

// === ScriptHandler: happy path ============================================

func TestScriptHandler_MentionCompilesAndSubmits(t *testing.T) {
	f := &fakePlans{result: sibyl.PlanResult{
		Outputs: map[string]string{"n0": "hello"},
		Leaves:  []string{"n0"},
	}}
	llm := stubLLM(`temporal static ( echo "hello" )`)
	h, corr := newTestScriptHandler(t, llm, f)
	reply, err := h.Handle(context.Background(), Event{
		Kind:      KindMention,
		Text:      "say hello",
		Context:   Context{Channel: "C1", User: "U1"},
		Timestamp: "100.0",
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(reply.React) != 1 || reply.React[0] != "eyes" {
		t.Errorf("ack React = %v, want [eyes]", reply.React)
	}
	if f.startedCount() != 1 {
		t.Fatalf("started %d plans, want 1", f.startedCount())
	}
	// The submitted plan should be the compiled echo (one Echo node).
	if got := f.started[0]; len(got.Nodes) != 1 || got.Nodes[0].Activity != sibyl.EchoActivityName {
		t.Errorf("submitted plan = %+v, want one Echo node", got.Nodes)
	}
	if _, ok := corr.Get("C1", "100.0"); !ok {
		t.Error("run should be recorded in correlation")
	}
}

func TestScriptHandler_EmptyTextNoSubmit(t *testing.T) {
	f := &fakePlans{}
	h, _ := newTestScriptHandler(t, stubLLM(`temporal static ( echo "x" )`), f)
	reply, err := h.Handle(context.Background(), Event{Kind: KindMention, Text: "   "})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !reply.IsEmpty() {
		t.Errorf("empty mention should be empty reply, got %+v", reply)
	}
	if f.startedCount() != 0 {
		t.Error("no plan should start for empty text")
	}
}

func TestScriptHandler_IgnoresOtherKinds(t *testing.T) {
	f := &fakePlans{}
	h, _ := newTestScriptHandler(t, stubLLM(`temporal static ( echo "x" )`), f)
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
		t.Error("no plans for ignored kinds")
	}
}

// === ScriptHandler: the safety net ========================================

// When the LLM emits DSL that names a command that doesn't exist, the
// compiler must reject it and NOTHING should be submitted.
func TestScriptHandler_RejectsUnknownCommand(t *testing.T) {
	f := &fakePlans{}
	llm := stubLLM(`temporal static ( teleport "mars" )`) // teleport isn't a builtin
	h, _ := newTestScriptHandler(t, llm, f)

	reply, err := h.Handle(context.Background(), Event{
		Kind: KindMention, Text: "teleport me to mars",
		Context: Context{Channel: "C1"}, Timestamp: "1.0",
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if f.startedCount() != 0 {
		t.Fatal("SAFETY NET FAILED: a plan was submitted for invalid DSL")
	}
	if len(reply.React) == 0 || reply.React[0] != "warning" {
		t.Errorf("expected a warning reaction, got %+v", reply)
	}
	if !strings.Contains(strings.ToLower(reply.Text), "couldn't") {
		t.Errorf("expected a friendly rejection, got %q", reply.Text)
	}
}

// With discovery, loom advertises the full vocabulary, so the LLM may
// legitimately emit a historical verb like hf_summarize. On the temporal
// backend that verb isn't implemented yet — loom must reject it with an
// HONEST "known but not on this backend" reply, and submit NOTHING. This
// is the behavior the complete-registry + discovery work unlocks: the
// failure is honest, not a misleading "unknown command".
// A memory-backend program must route to in-process execution, NOT to a
// temporal submission. loom posts the result directly; no plan is started
// and no correlation is recorded. (We use a memory verb that fails fast
// without credentials — the routing is the assertion: zero temporal
// submits, and the reply is synchronous, not a "working on it…" ack.)
func TestScriptHandler_MemoryRoutesNoSubmit(t *testing.T) {
	f := &fakePlans{}
	llm := stubLLM(`memory static ( hf_summarize "the thread" )`)
	h, corr := newTestScriptHandler(t, llm, f)

	reply, err := h.Handle(context.Background(), Event{
		Kind: KindMention, Text: "summarize in memory",
		Context: Context{Channel: "C1", User: "U1"}, Timestamp: "100.0",
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	// Memory must never submit a temporal plan or correlate a workflow.
	if f.startedCount() != 0 {
		t.Errorf("memory program must not submit a temporal plan; started %d", f.startedCount())
	}
	if _, ok := corr.Get("C1", "100.0"); ok {
		t.Error("memory program must not record a temporal correlation")
	}
	// The reply must NOT be the temporal "working on it…" ack with an
	// eyes reaction — memory replies synchronously (with the result, or a
	// friendly error if the verb needs credentials we didn't supply).
	for _, r := range reply.React {
		if r == "eyes" {
			t.Error("memory reply should be synchronous, not the temporal ack")
		}
	}
}

func TestScriptHandler_KnownVerbNotOnBackend_NoSubmit(t *testing.T) {
	f := &fakePlans{}
	llm := stubLLM(`temporal static ( hf_summarize "the thread" )`)
	h, _ := newTestScriptHandler(t, llm, f)

	reply, err := h.Handle(context.Background(), Event{
		Kind: KindMention, Text: "summarize the thread",
		Context: Context{Channel: "C1"}, Timestamp: "1.0",
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if f.startedCount() != 0 {
		t.Fatal("a known-but-unavailable verb must not submit anything")
	}
	if len(reply.React) == 0 || reply.React[0] != "warning" {
		t.Errorf("expected a warning reaction, got %+v", reply)
	}
	if !strings.Contains(strings.ToLower(reply.Text), "backend") {
		t.Errorf("expected an honest not-on-this-backend message, got %q", reply.Text)
	}
}

// When the LLM emits syntactically broken DSL, compile also rejects it.
func TestScriptHandler_RejectsMalformedDSL(t *testing.T) {
	f := &fakePlans{}
	llm := stubLLM(`this is not agentscript at all`)
	h, _ := newTestScriptHandler(t, llm, f)
	reply, err := h.Handle(context.Background(), Event{
		Kind: KindMention, Text: "do something weird",
		Context: Context{Channel: "C1"}, Timestamp: "1.0",
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if f.startedCount() != 0 {
		t.Error("malformed DSL must not be submitted")
	}
	if reply.IsEmpty() {
		t.Error("should reply with a rejection, not silence")
	}
}

func TestScriptHandler_TranslatorErrorRepliesGracefully(t *testing.T) {
	f := &fakePlans{}
	llm := func(_ context.Context, _, _ string) (string, error) {
		return "", errors.New("network down")
	}
	h, _ := newTestScriptHandler(t, llm, f)
	reply, err := h.Handle(context.Background(), Event{
		Kind: KindMention, Text: "hello", Context: Context{Channel: "C1"}, Timestamp: "1.0",
	})
	if err != nil {
		t.Fatalf("Handle should not error, should reply gracefully: %v", err)
	}
	if f.startedCount() != 0 {
		t.Error("nothing should submit when translation fails")
	}
	if reply.IsEmpty() {
		t.Error("should reply about the translator being unreachable")
	}
}

// === runner: correlation cleanup after await ==============================

func TestRunner_ClearsCorrelationAfterAwait(t *testing.T) {
	f := &fakePlans{result: sibyl.PlanResult{Outputs: map[string]string{"n0": "done"}, Leaves: []string{"n0"}}}
	corr := NewCorrelation()
	r := &runner{plans: f, corr: corr, renderer: &renderer{api: nil}, awaitTimeout: time.Second}

	plan := sibyl.Plan{Nodes: []sibyl.PlanNode{{ID: "n0", Activity: sibyl.EchoActivityName, Args: []string{"x"}}}}
	_, err := r.submit(context.Background(), Event{
		Kind: KindMention, Context: Context{Channel: "C1", User: "U1"}, Timestamp: "100.0",
	}, plan)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if corr.Len() != 1 {
		t.Fatalf("correlation Len = %d after submit, want 1", corr.Len())
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if corr.Len() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("correlation not cleared after await; Len = %d", corr.Len())
}

func TestPlanResultText(t *testing.T) {
	res := sibyl.PlanResult{Outputs: map[string]string{"n0": "first", "n1": "final"}, Leaves: []string{"n1"}}
	if got := planResultText(res); got != "final" {
		t.Errorf("planResultText = %q, want final", got)
	}
	if got := planResultText(sibyl.PlanResult{}); got != "(no output)" {
		t.Errorf("empty result = %q", got)
	}
}
