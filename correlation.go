package loom

import (
	"sync"
	"time"
)

// Correlation tracks the link between a Slack thread and the Sibyl
// workflow started from it. It is the state that makes the single-turn
// front end feel continuous: each Slack event is an independent
// Arrow[Event, Reply] invocation, but the correlation table remembers
// which workflow belongs to which thread so a long-running run can be
// found again — for posting its result, and (later) for routing a
// human's follow-up reply back into the same workflow.
//
// This is the B1 design: loom owns the correlation, holds the workflow
// handle, and a background goroutine delivers the result. Sibyl is never
// told about threads; it just runs workflows.
//
// The table is safe for concurrent use.
type Correlation struct {
	mu       sync.Mutex
	byThread map[string]*Run
}

// Run records a workflow started from a thread.
type Run struct {
	// WorkflowID and RunID identify the Temporal workflow.
	WorkflowID string
	RunID      string
	// Channel and Thread are where to post the result.
	Channel string
	Thread  string
	// User is the Slack user who started it.
	User string
	// StartedAt is when the run was recorded.
	StartedAt time.Time
}

// NewCorrelation returns an empty correlation table.
func NewCorrelation() *Correlation {
	return &Correlation{byThread: make(map[string]*Run)}
}

// key builds the map key from channel + thread. A thread is unique
// within a channel; combining both avoids collisions across channels.
func corrKey(channel, thread string) string {
	return channel + "\x00" + thread
}

// Put records a run for a (channel, thread). If one already exists it is
// overwritten — the newest run for a thread wins.
func (c *Correlation) Put(r *Run) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byThread[corrKey(r.Channel, r.Thread)] = r
}

// Get returns the run recorded for a (channel, thread), if any.
func (c *Correlation) Get(channel, thread string) (*Run, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.byThread[corrKey(channel, thread)]
	return r, ok
}

// Delete removes the run for a (channel, thread). Called when a run
// completes so the table doesn't grow unbounded.
func (c *Correlation) Delete(channel, thread string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.byThread, corrKey(channel, thread))
}

// Len returns the number of tracked runs.
func (c *Correlation) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.byThread)
}
