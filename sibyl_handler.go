package loom

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/vinodhalaharvi/agentscript/pkg/script"
	"github.com/vinodhalaharvi/agentscript/pkg/scriptmem"
)

// ScriptHandler is loom's main Handler: it turns a Slack message into an
// AgentScript program (via an LLM) and runs it through the unified
// execution entry. The backend the program targets (memory | temporal)
// is chosen by the DSL the LLM emits, and decides the delivery shape:
//
//   - memory   → the program ran in-process; the result is ready now, so
//     loom posts it immediately (a synchronous reply).
//   - temporal → the program compiled to a durable plan; loom submits it,
//     records the thread↔workflow correlation, and posts the result when
//     the workflow completes (the B1 async path).
//
// loom carries NO grammar knowledge. It calls script.Grammar() once and
// passes the GrammarInfo through opaquely; AgentScript owns translation,
// resolution, backend selection, and (for memory) execution. loom only
// learns the execution *shape* (sync vs async) — never the grammar — and
// branches on it to choose how to deliver the answer.
//
//	prose → scriptmem.Execute → Outcome{ memory: Result | temporal: Plan }
type ScriptHandler struct {
	complete script.CompleteFunc
	grammar  script.GrammarInfo
	memCfg   scriptmem.MemoryConfig
	runner   *runner
}

// ScriptHandlerConfig configures a ScriptHandler.
type ScriptHandlerConfig struct {
	// Complete is the LLM used for prose→DSL translation. Required.
	Complete script.CompleteFunc
	// Grammar is the discovery result from script.Grammar(). loom passes
	// it through opaquely — it never inspects the contents, names a
	// registry, or carries a verb list. Required.
	Grammar script.GrammarInfo
	// MemoryConfig configures the in-process runtime for memory-backend
	// execution (API keys, etc.). Optional — a verb that needs a
	// credential it doesn't supply fails at execution.
	MemoryConfig scriptmem.MemoryConfig
	// Plans is the execution seam for the temporal backend. Required.
	Plans PlanClient
	// Correlation tracks thread↔workflow for temporal runs. Required.
	Correlation *Correlation
	// Renderer posts the async result. Required.
	Renderer *renderer
	// AwaitTimeout caps how long a temporal background wait blocks.
	// 0 = none.
	AwaitTimeout time.Duration
}

// NewScriptHandler builds a ScriptHandler.
func NewScriptHandler(cfg ScriptHandlerConfig) *ScriptHandler {
	return &ScriptHandler{
		complete: cfg.Complete,
		grammar:  cfg.Grammar,
		memCfg:   cfg.MemoryConfig,
		runner: &runner{
			plans:        cfg.Plans,
			corr:         cfg.Correlation,
			renderer:     cfg.Renderer,
			awaitTimeout: cfg.AwaitTimeout,
		},
	}
}

// Handle is the Handler. Only mentions, messages, and slash commands
// carry a request; other event kinds are ignored.
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
	// One call: translate → resolve → route on backend → run (memory) or
	// compile to a plan (temporal). loom never inspects the grammar.
	outcome, err := scriptmem.Execute(ctx, h.complete, h.grammar, h.memCfg, prose)
	if err != nil {
		return h.replyForError(err), nil
	}

	switch outcome.Backend {
	case scriptmem.Memory:
		// Already ran in-process; the result is ready. Post it now.
		return Reply{
			Target: TargetSameThread,
			Text:   memoryResultText(outcome.Result),
		}, nil

	case scriptmem.Temporal:
		// Durable plan: submit + correlate + background await + post later.
		return h.runner.submit(ctx, e, outcome.Plan)

	default:
		log.Printf("loom: unexpected backend %v", outcome.Backend)
		return Reply{
			Target: TargetSameThread,
			Text:   "⚠️ I couldn't figure out how to run that.",
		}, nil
	}
}

// replyForError turns an Execute error into a friendly Slack reply. It
// distinguishes the translator being unreachable (LLM failure) from the
// compiler/safety-net rejecting the program (unknown verb, wrong backend,
// bad arity). In every case nothing ran, so we just explain kindly.
func (h *ScriptHandler) replyForError(err error) Reply {
	// Translation/LLM reachability: a bare error from the LLM seam.
	var unknown *script.UnknownBuiltinError
	var notImpl *script.NotImplementedOnBackendError
	var arity *script.ArityError
	if errors.As(err, &unknown) || errors.As(err, &notImpl) || errors.As(err, &arity) {
		log.Printf("loom: program rejected: %v", err)
		return Reply{
			Target: TargetSameThread,
			React:  []string{"warning"},
			Text:   "I couldn't turn that into a valid command. " + friendlyCompileError(err),
		}
	}
	// Otherwise treat it as an upstream/translator failure.
	log.Printf("loom: execute failed: %v", err)
	return Reply{
		Target: TargetSameThread,
		Text:   "⚠️ I couldn't reach the translator. Try again in a moment.",
	}
}

// memoryResultText renders an in-process result for Slack.
func memoryResultText(result string) string {
	if strings.TrimSpace(result) == "" {
		return "(no output)"
	}
	return result
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
	var notImpl *script.NotImplementedOnBackendError
	if errors.As(err, &notImpl) {
		return "That's a known action but it isn't available on this backend yet."
	}
	var arity *script.ArityError
	if errors.As(err, &arity) {
		return "That command got the wrong number of arguments."
	}
	return "Try rephrasing it more simply."
}
