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
//	ANTHROPIC_API_KEY  the LLM key for prose→DSL translation
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

	// LLM for prose→DSL translation. Reuses Sibyl's Anthropic client,
	// which reads ANTHROPIC_API_KEY.
	llm, err := sibyl.NewAnthropicClient(sibyl.AnthropicConfig{})
	if err != nil {
		log.Fatalf("loom: LLM setup: %v (set ANTHROPIC_API_KEY)", err)
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
		Complete:     llm.Complete,
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
