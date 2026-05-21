package loom

import (
	"context"
	"strings"
	"time"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// SibylHandler turns Slack events into Sibyl workflow submissions. It is
// the PR-2 replacement for EchoHandler: instead of echoing, it builds a
// Question from the event text and starts a ConvergeWorkflow, returning
// an immediate acknowledgement. The workflow's answer is posted back to
// the thread asynchronously by the runner (B1).
type SibylHandler struct {
	runner    *runner
	maxRounds int
}

// SibylHandlerConfig configures a SibylHandler.
type SibylHandlerConfig struct {
	// Sibyl is the execution seam. Required.
	Sibyl SibylClient
	// Correlation tracks thread↔workflow. Required.
	Correlation *Correlation
	// Renderer posts the async result. Required.
	Renderer *renderer
	// MaxRounds caps the convergence loop. Defaults to 3.
	MaxRounds int
	// AwaitTimeout caps how long the background wait blocks. 0 = none.
	AwaitTimeout time.Duration
}

// NewSibylHandler builds a SibylHandler and its Handler function.
func NewSibylHandler(cfg SibylHandlerConfig) *SibylHandler {
	maxRounds := cfg.MaxRounds
	if maxRounds <= 0 {
		maxRounds = 3
	}
	return &SibylHandler{
		runner: &runner{
			sibyl:        cfg.Sibyl,
			corr:         cfg.Correlation,
			renderer:     cfg.Renderer,
			awaitTimeout: cfg.AwaitTimeout,
		},
		maxRounds: maxRounds,
	}
}

// Handle is the Handler. It builds a Question from the event and submits
// it. Only mentions and direct messages start runs; other event kinds
// are ignored (reactions, file shares, interactions are handled by later
// PRs).
func (h *SibylHandler) Handle(ctx context.Context, e Event) (Reply, error) {
	switch e.Kind {
	case KindMention, KindMessage:
		text := strings.TrimSpace(e.Text)
		if text == "" {
			return Reply{}, nil // nothing to ask
		}
		q := agent.Question{Text: text, MaxRounds: h.maxRounds}
		return h.runner.submit(ctx, e, q)

	case KindSlashCommand:
		text := strings.TrimSpace(e.Text)
		if text == "" {
			return Reply{Target: TargetEphemeral, Text: "Usage: provide a question after the command."}, nil
		}
		q := agent.Question{Text: text, MaxRounds: h.maxRounds}
		return h.runner.submit(ctx, e, q)

	default:
		return Reply{}, nil
	}
}
