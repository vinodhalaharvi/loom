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
// loom carries NO grammar knowledge. It calls script.Grammar() once at
// startup and passes the resulting GrammarInfo through opaquely — it
// never inspects the contents, names a registry, or carries a verb list.
// AgentScript owns the grammar, the prose→DSL prompt, and validation;
// when AgentScript's grammar grows, loom reflects it automatically. loom
// leans on the compiler as the safety net: an unknown command, bad arity,
// or a verb not available on the chosen backend fails at compile, and
// loom replies with a friendly message — nothing wrong ever executes.
//
//	prose → script.TranslateGrammar (LLM) → DSL → script.CompileGrammar → Plan → submit
type ScriptHandler struct {
	complete script.CompleteFunc
	grammar  script.GrammarInfo
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
		complete: cfg.Complete,
		grammar:  cfg.Grammar,
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
	// prose → DSL. AgentScript owns the grammar prompt and the verb
	// vocabulary; loom passes its discovery handle (h.grammar) straight
	// through without inspecting it.
	src, err := script.TranslateGrammar(ctx, h.complete, h.grammar, prose)
	if err != nil {
		// LLM/translation failure (network, no key, etc.).
		log.Printf("loom: translate failed: %v", err)
		return Reply{
			Target: TargetSameThread,
			Text:   "⚠️ I couldn't reach the translator. Try again in a moment.",
		}, nil
	}

	// DSL → validated Plan. The compiler is the safety net.
	plan, err := script.CompileGrammar(ctx, h.grammar, src)
	if err != nil {
		// The LLM emitted DSL that doesn't compile — unknown command,
		// bad arity, a verb not available on this backend yet, or a
		// malformed graph. Nothing executes; tell the user.
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
