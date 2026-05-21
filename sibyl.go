package loom

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// SibylClient is the seam between loom and Sibyl. It is the only place
// loom crosses the repo boundary into the execution engine. Defining it
// as an interface lets the handler be tested with a fake that doesn't
// need a running Temporal cluster.
//
// Start begins a workflow and returns immediately with a handle (B1:
// fire, don't block). Await blocks until the workflow identified by the
// handle completes and returns its Answer — it is called from a
// background goroutine, off the event-handling critical path.
type SibylClient interface {
	// Start submits a Question and returns a handle without waiting for
	// the workflow to finish.
	Start(ctx context.Context, q agent.Question) (RunHandle, error)
	// Await blocks until the workflow for handle completes, returning
	// its Answer.
	Await(ctx context.Context, handle RunHandle) (agent.Answer, error)
}

// RunHandle identifies a started workflow so it can be awaited later.
type RunHandle struct {
	WorkflowID string
	RunID      string
}

// === Temporal-backed implementation ========================================

// temporalSibyl is the production SibylClient. It submits ConvergeWorkflow
// to a running Sibyl worker via a Temporal client.
type temporalSibyl struct {
	c         client.Client
	taskQueue string
}

// NewTemporalSibyl dials Temporal and returns a SibylClient that submits
// to the given task queue (default: agent.TaskQueue). The caller owns
// closing the underlying client via Close.
func NewTemporalSibyl(hostPort, taskQueue string) (*temporalSibyl, error) {
	opts := client.Options{}
	if hostPort != "" {
		opts.HostPort = hostPort
	}
	c, err := client.Dial(opts)
	if err != nil {
		return nil, fmt.Errorf("loom: dial Temporal: %w", err)
	}
	if taskQueue == "" {
		taskQueue = agent.TaskQueue
	}
	return &temporalSibyl{c: c, taskQueue: taskQueue}, nil
}

// Close releases the Temporal client.
func (t *temporalSibyl) Close() {
	if t.c != nil {
		t.c.Close()
	}
}

// Start submits ConvergeWorkflow without blocking on the result.
func (t *temporalSibyl) Start(ctx context.Context, q agent.Question) (RunHandle, error) {
	opts := client.StartWorkflowOptions{
		TaskQueue: t.taskQueue,
		ID:        fmt.Sprintf("loom-%d", time.Now().UnixNano()),
	}
	we, err := t.c.ExecuteWorkflow(ctx, opts, agent.ConvergeWorkflowName, q)
	if err != nil {
		return RunHandle{}, fmt.Errorf("loom: start workflow: %w", err)
	}
	return RunHandle{WorkflowID: we.GetID(), RunID: we.GetRunID()}, nil
}

// Await blocks on the workflow result. Uses GetWorkflow to rebuild the
// run handle so it works from any goroutine (not just the one that
// started it).
func (t *temporalSibyl) Await(ctx context.Context, h RunHandle) (agent.Answer, error) {
	we := t.c.GetWorkflow(ctx, h.WorkflowID, h.RunID)
	var ans agent.Answer
	if err := we.Get(ctx, &ans); err != nil {
		return agent.Answer{}, fmt.Errorf("loom: workflow %s failed: %w", h.WorkflowID, err)
	}
	return ans, nil
}

// === The async runner that ties Start → Await → post together ==============

// runner submits a question and arranges for the answer to be posted back
// to the originating thread when the workflow completes. This is the B1
// flow: Start returns fast (so the worker-pool slot frees), the
// correlation table records the run, and a detached goroutine Awaits and
// posts. The detached goroutine is not bounded by the handler pool —
// it's mostly blocked on I/O waiting for Temporal, not doing work.
type runner struct {
	sibyl    SibylClient
	corr     *Correlation
	renderer *renderer
	// awaitTimeout caps how long a background Await waits before giving
	// up and posting a timeout message. 0 means no timeout.
	awaitTimeout time.Duration
}

// submit starts the workflow for an event and spawns the background
// await+post. It returns the immediate Reply to send now (an
// acknowledgement); the real answer arrives later via the goroutine.
func (r *runner) submit(ctx context.Context, e Event, q agent.Question) (Reply, error) {
	handle, err := r.sibyl.Start(ctx, q)
	if err != nil {
		return Reply{}, err
	}

	thread := e.Context.Thread
	if thread == "" {
		thread = e.Timestamp // start a thread on the triggering message
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

	// Background await + post. Detached from ctx so a per-event
	// cancellation doesn't kill an in-flight workflow wait; uses its own
	// timeout instead.
	go r.awaitAndPost(run, e)

	// Immediate acknowledgement. The answer follows asynchronously.
	return Reply{
		Target: TargetSameThread,
		React:  []string{"eyes"},
		Text:   "Working on it…",
	}, nil
}

// awaitAndPost blocks on the workflow result and posts it back to the
// originating thread, then clears the correlation entry.
func (r *runner) awaitAndPost(run *Run, e Event) {
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

	ans, err := r.sibyl.Await(ctx, RunHandle{WorkflowID: run.WorkflowID, RunID: run.RunID})

	// Build the reply that posts into the originating thread. We post to
	// the recorded channel/thread regardless of the original event's
	// shape, so use a synthetic Event carrying that target.
	postEvent := Event{
		Context: Context{
			Channel: run.Channel,
			User:    run.User,
			Thread:  run.Thread,
		},
		Timestamp: run.Thread,
	}

	var reply Reply
	if err != nil {
		log.Printf("loom: workflow %s error: %v", run.WorkflowID, err)
		reply = Reply{Target: TargetSameThread, Text: "⚠️ The run failed: " + err.Error()}
	} else {
		reply = Reply{Target: TargetSameThread, Text: ans.Text}
	}

	if perr := r.renderer.render(ctx, postEvent, reply); perr != nil {
		log.Printf("loom: posting result for %s: %v", run.WorkflowID, perr)
	}
}
