// Command loom runs the Slack front end: it opens a Socket Mode
// WebSocket to Slack and routes events into Sibyl via AgentScript.
//
// The pipeline (PR-3):
//
//	Slack message (prose)
//	  → script.Translate (LLM) emits AgentScript DSL
//	  → script.Compile validates it (rejects unknown/bad commands safely)
//	  → script.Submit runs it as a durable Sibyl PlanWorkflow
//	  → the result is posted back to the thread (B1 correlation)
//
// Required environment:
//
//	SLACK_BOT_TOKEN    xoxb-...   (Web API: post, react)
//	SLACK_APP_TOKEN    xapp-...   (Socket Mode: connections:write)
//	LOOM_LLM           llm backend: claude-code (default) | anthropic
//	ANTHROPIC_API_KEY  required only when LOOM_LLM=anthropic
//
// Optional environment:
//
//	TEMPORAL_HOSTPORT  Temporal frontend (default 127.0.0.1:7233)
//	SIBYL_TASK_QUEUE   worker task queue (default sibyl-agents)
//	LOOM_DEBUG         verbose slack-go logging
//
// Running end-to-end needs Temporal + a Sibyl worker:
//
//	temporal server start-dev
//	go run ./cmd/worker          # sibyl repo
//	go run ./cmd/loom            # here
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	sibyl "github.com/vinodhalaharvi/sibyl/agent"

	"github.com/vinodhalaharvi/agentscript/pkg/script"
	"github.com/vinodhalaharvi/agentscript/pkg/scriptmem"

	"github.com/vinodhalaharvi/loom"
)

func main() {
	botToken := os.Getenv("SLACK_BOT_TOKEN")
	appToken := os.Getenv("SLACK_APP_TOKEN")
	if botToken == "" || appToken == "" {
		log.Fatal("loom: set SLACK_BOT_TOKEN (xoxb-) and SLACK_APP_TOKEN (xapp-)")
	}

	// LLM for prose→DSL translation. Default to Claude Code (the `claude`
	// CLI as a subprocess), which uses the CLI's own local login — no API
	// key needed. Set LOOM_LLM=anthropic to use the Messages API instead
	// (then ANTHROPIC_API_KEY is required). Either client is just a
	// CompleteFunc, so the handler is unchanged.
	complete, err := pickLLM(os.Getenv("LOOM_LLM"), os.Getenv("LOOM_MODEL"))
	if err != nil {
		log.Fatalf("loom: LLM setup: %v", err)
	}

	// Discovery: ask AgentScript what can be done. loom passes this
	// through opaquely — it never inspects the contents, names a
	// registry, or carries a verb list. When AgentScript's grammar grows,
	// loom reflects it automatically.
	grammar := script.Grammar()

	// Sibyl execution seam.
	plans, err := loom.NewTemporalPlanClient(
		os.Getenv("TEMPORAL_HOSTPORT"),
		os.Getenv("SIBYL_TASK_QUEUE"),
	)
	if err != nil {
		log.Fatalf("loom: %v", err)
	}
	defer plans.Close()

	corr := loom.NewCorrelation()
	renderer := loom.NewRenderer(botToken)

	// Memory-backend execution config: the in-process runtime reads these
	// for verbs that need credentials. All optional — a memory verb that
	// needs a key it doesn't get fails at execution with a friendly note.
	memCfg := scriptmem.MemoryConfig{
		GeminiAPIKey: os.Getenv("GEMINI_API_KEY"),
		ClaudeAPIKey: os.Getenv("ANTHROPIC_API_KEY"),
		SearchAPIKey: os.Getenv("SEARCH_API_KEY"),
		Model:        os.Getenv("AGENTSCRIPT_MODEL"),
	}

	handler := loom.NewScriptHandler(loom.ScriptHandlerConfig{
		Complete:     complete,
		Grammar:      grammar,
		MemoryConfig: memCfg,
		Plans:        plans,
		Correlation:  corr,
		Renderer:     renderer,
	})

	listener, err := loom.NewListener(loom.Options{
		BotToken: botToken,
		AppToken: appToken,
		Handler:  handler.Handle,
		Debug:    os.Getenv("LOOM_DEBUG") != "",
	})
	if err != nil {
		log.Fatalf("loom: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("loom: starting Socket Mode listener…")
	if err := listener.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("loom: listener stopped: %v", err)
	}
	log.Println("loom: shut down")
}

// pickLLM selects the prose→DSL completion backend. It defaults to Claude
// Code (the `claude` CLI subprocess, authenticated by the CLI's own local
// login — no API key). LOOM_LLM=anthropic switches to the Messages API
// (requires ANTHROPIC_API_KEY). Both return a CompleteFunc, so the rest
// of loom is identical regardless of choice.
func pickLLM(kind, model string) (script.CompleteFunc, error) {
	switch kind {
	case "", "claude-code", "claudecode":
		c := sibyl.NewClaudeCodeClient(sibyl.ClaudeCodeConfig{Model: model})
		return c.Complete, nil
	case "anthropic":
		c, err := sibyl.NewAnthropicClient(sibyl.AnthropicConfig{})
		if err != nil {
			return nil, fmt.Errorf("anthropic backend (set ANTHROPIC_API_KEY): %w", err)
		}
		return c.Complete, nil
	default:
		return nil, fmt.Errorf("unknown LOOM_LLM %q (use claude-code or anthropic)", kind)
	}
}
