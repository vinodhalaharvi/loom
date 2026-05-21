package loom

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/vinodhalaharvi/agentscript/pkg/script"
)

// ScriptHandler is loom's main Handler: it turns a Slack message into an
// AgentScript program (via an LLM), compiles that program to a Sibyl
// Plan, and submits it. The answer is posted back to the thread
// asynchronously by the runner (B1).
//
// The flow realizes the "AgentScript as serialization layer" design:
//
//	prose → Translator (LLM) → DSL → script.Compile (validate) → Plan → submit
//
// The compiler is the safety net. If the LLM emits an unknown command or
// bad arity, script.Compile fails with a typed error and loom replies
// with a friendly "I couldn't turn that into a valid command" message —
// nothing wrong ever executes.
type ScriptHandler struct {
	translator *Translator
	runner     *runner
}

// ScriptHandlerConfig configures a ScriptHandler.
type ScriptHandlerConfig struct {
	// Translator turns prose into AgentScript DSL. Required.
	Translator *Translator
	// Plans is the execution seam (submit/await Plans). Required.
	Plans PlanClient
	// Correlation tracks thread↔workflow. Required.
	Correlation *Correlation
	// Renderer posts the async result. Required.
	Renderer *renderer
	// AwaitTimeout caps how long the background wait blocks. 0 = none.
	AwaitTimeout time.Duration
}

// NewScriptHandler builds a ScriptHandler.
func NewScriptHandler(cfg ScriptHandlerConfig) *ScriptHandler {
	return &ScriptHandler{
		translator: cfg.Translator,
		runner: &runner{
			plans:        cfg.Plans,
			corr:         cfg.Correlation,
			renderer:     cfg.Renderer,
			awaitTimeout: cfg.AwaitTimeout,
		},
	}
}

// Handle is the Handler. It compiles the message text into a Plan and
// submits it. Only mentions, messages, and slash commands carry a
// request; other event kinds are ignored.
func (h *ScriptHandler) Handle(ctx context.Context, e Event) (Reply, error) {
	switch e.Kind {
	case KindMention, KindMessage, KindSlashCommand:
		prose := strings.TrimSpace(e.Text)
		if prose == "" {
			return Reply{}, nil
		}
		return h.handleProse(ctx, e, prose)
	default:
		return Reply{}, nil
	}
}

func (h *ScriptHandler) handleProse(ctx context.Context, e Event, prose string) (Reply, error) {
	// prose → DSL → validated Plan.
	src, err := h.translator.Compile(ctx, prose)
	if err != nil {
		// LLM/translation failure (network, no key, etc.).
		log.Printf("loom: translate failed: %v", err)
		return Reply{
			Target: TargetSameThread,
			Text:   "⚠️ I couldn't reach the translator. Try again in a moment.",
		}, nil
	}

	plan, err := script.Compile(ctx, h.translator.reg, src)
	if err != nil {
		// The LLM emitted DSL that doesn't compile — unknown command,
		// bad arity, malformed graph. This is the safety net working:
		// nothing executes. Tell the user kindly.
		log.Printf("loom: compile rejected DSL %q: %v", string(src), err)
		return Reply{
			Target: TargetSameThread,
			React:  []string{"warning"},
			Text:   "I couldn't turn that into a valid command. " + friendlyCompileError(err),
		}, nil
	}

	// Validated plan → submit + correlate + background await.
	return h.runner.submit(ctx, e, plan)
}

// friendlyCompileError turns a compiler error into a short,
// user-appropriate hint without leaking internals.
func friendlyCompileError(err error) string {
	var unknown *script.UnknownBuiltinError
	if errors.As(err, &unknown) {
		if len(unknown.Known) > 0 {
			return "I can do: " + strings.Join(unknown.Known, ", ") + "."
		}
		return "That command isn't available."
	}
	var arity *script.ArityError
	if errors.As(err, &arity) {
		return "That command got the wrong number of arguments."
	}
	return "Try rephrasing it more simply."
}
