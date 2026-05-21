package loom

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"

	sibyl "github.com/vinodhalaharvi/sibyl/agent"
)

// PlanClient is the seam between loom and Sibyl's plan execution. It is
// the only place loom crosses the repo boundary into the execution
// engine. Defining it as an interface lets the handler be tested with a
// fake that doesn't need a running Temporal cluster.
//
// Start submits a compiled Plan and returns immediately with a handle
// (B1: fire, don't block). Await blocks until the plan's workflow
// completes and returns its PlanResult — it is called from a background
// goroutine, off the event-handling critical path.
type PlanClient interface {
	Start(ctx context.Context, plan sibyl.Plan) (RunHandle, error)
	Await(ctx context.Context, handle RunHandle) (sibyl.PlanResult, error)
}

// RunHandle identifies a started workflow so it can be awaited later.
type RunHandle struct {
	WorkflowID string
	RunID      string
}

// === Temporal-backed implementation ========================================

// temporalPlanClient is the production PlanClient. It submits a Plan as a
// Sibyl PlanWorkflow via a Temporal client.
type temporalPlanClient struct {
	c         client.Client
	taskQueue string
}

// NewTemporalPlanClient dials Temporal and returns a PlanClient that
// submits to the given task queue (default: sibyl's TaskQueue). The
// caller owns closing the underlying client via Close.
func NewTemporalPlanClient(hostPort, taskQueue string) (*temporalPlanClient, error) {
	opts := client.Options{}
	if hostPort != "" {
		opts.HostPort = hostPort
	}
	c, err := client.Dial(opts)
	if err != nil {
		return nil, fmt.Errorf("loom: dial Temporal: %w", err)
	}
	return &temporalPlanClient{c: c, taskQueue: taskQueue}, nil
}

// Close releases the Temporal client.
func (t *temporalPlanClient) Close() {
	if t.c != nil {
		t.c.Close()
	}
}

// Start submits the Plan as a PlanWorkflow without blocking on the result.
func (t *temporalPlanClient) Start(ctx context.Context, plan sibyl.Plan) (RunHandle, error) {
	we, err := sibyl.SubmitPlan(ctx, t.c, plan, "", t.taskQueue)
	if err != nil {
		return RunHandle{}, fmt.Errorf("loom: submit plan: %w", err)
	}
	return RunHandle{WorkflowID: we.GetID(), RunID: we.GetRunID()}, nil
}

// Await blocks on the workflow result. Uses GetWorkflow to rebuild the
// run handle so it works from any goroutine (not just the starter).
func (t *temporalPlanClient) Await(ctx context.Context, h RunHandle) (sibyl.PlanResult, error) {
	we := t.c.GetWorkflow(ctx, h.WorkflowID, h.RunID)
	var res sibyl.PlanResult
	if err := we.Get(ctx, &res); err != nil {
		return sibyl.PlanResult{}, fmt.Errorf("loom: workflow %s failed: %w", h.WorkflowID, err)
	}
	return res, nil
}

// === The async runner that ties Start → Await → post together ==============

// runner submits a plan and arranges for the result to be posted back to
// the originating thread when the workflow completes (B1). Start returns
// fast (the worker-pool slot frees), the correlation table records the
// run, and a detached goroutine Awaits and posts.
type runner struct {
	plans    PlanClient
	corr     *Correlation
	renderer *renderer
	// awaitTimeout caps how long a background Await waits. 0 = no timeout.
	awaitTimeout time.Duration
}

// submit starts the plan for an event and spawns the background
// await+post. It returns the immediate acknowledgement Reply.
func (r *runner) submit(ctx context.Context, e Event, plan sibyl.Plan) (Reply, error) {
	handle, err := r.plans.Start(ctx, plan)
	if err != nil {
		return Reply{}, err
	}

	thread := e.Context.Thread
	if thread == "" {
		thread = e.Timestamp
	}
	run := &Run{
		WorkflowID: handle.WorkflowID,
		RunID:      handle.RunID,
		Channel:    e.Context.Channel,
		Thread:     thread,
		User:       e.Context.User,
		StartedAt:  time.Now(),
	}
	r.corr.Put(run)

	go r.awaitAndPost(run)

	return Reply{
		Target: TargetSameThread,
		React:  []string{"eyes"},
		Text:   "Working on it…",
	}, nil
}

// awaitAndPost blocks on the workflow result and posts it back to the
// originating thread, then clears the correlation entry.
func (r *runner) awaitAndPost(run *Run) {
	defer r.corr.Delete(run.Channel, run.Thread)
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("loom: awaitAndPost panic for workflow %s: %v", run.WorkflowID, rec)
		}
	}()

	ctx := context.Background()
	if r.awaitTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.awaitTimeout)
		defer cancel()
	}

	res, err := r.plans.Await(ctx, RunHandle{WorkflowID: run.WorkflowID, RunID: run.RunID})

	postEvent := Event{
		Context:   Context{Channel: run.Channel, User: run.User, Thread: run.Thread},
		Timestamp: run.Thread,
	}

	var reply Reply
	if err != nil {
		log.Printf("loom: workflow %s error: %v", run.WorkflowID, err)
		reply = Reply{Target: TargetSameThread, Text: "⚠️ The run failed: " + err.Error()}
	} else {
		reply = Reply{Target: TargetSameThread, Text: planResultText(res)}
	}

	if perr := r.renderer.render(ctx, postEvent, reply); perr != nil {
		log.Printf("loom: posting result for %s: %v", run.WorkflowID, perr)
	}
}

// planResultText renders a PlanResult for Slack: the output(s) of the
// leaf node(s) — the pipeline's final result.
func planResultText(res sibyl.PlanResult) string {
	if len(res.Leaves) == 0 {
		return "(no output)"
	}
	var parts []string
	for _, leaf := range res.Leaves {
		if out := res.Outputs[leaf]; out != "" {
			parts = append(parts, out)
		}
	}
	if len(parts) == 0 {
		return "(empty output)"
	}
	text := parts[0]
	for _, p := range parts[1:] {
		text += "\n" + p
	}
	return text
}
