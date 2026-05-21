// Command loom runs the Slack front end: it opens a Socket Mode
// WebSocket to Slack and routes events into Sibyl.
//
// PR-2 wires the SibylHandler — a Slack mention/message/slash-command
// starts a Sibyl ConvergeWorkflow, and the answer is posted back to the
// originating thread when the workflow completes (the B1 correlation
// model: start fast, await in the background, post on completion).
//
// Required environment:
//
//	SLACK_BOT_TOKEN   xoxb-...   (Web API: post, react, user info)
//	SLACK_APP_TOKEN   xapp-...   (Socket Mode: connections:write)
//
// Optional environment:
//
//	TEMPORAL_HOSTPORT   Temporal frontend address (default: 127.0.0.1:7233)
//	SIBYL_TASK_QUEUE    task queue the Sibyl worker listens on (default: sibyl-agents)
//	LOOM_MAX_ROUNDS     convergence round cap (default: 3)
//	LOOM_DEBUG          set to enable verbose slack-go logging
//
// A running Temporal cluster and a Sibyl worker are required for the
// workflow to actually execute:
//
//	temporal server start-dev          # one terminal
//	go run ./cmd/worker                # in the sibyl repo, another terminal
//	go run ./cmd/loom                  # here
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/vinodhalaharvi/loom"
)

func main() {
	botToken := os.Getenv("SLACK_BOT_TOKEN")
	appToken := os.Getenv("SLACK_APP_TOKEN")
	if botToken == "" || appToken == "" {
		log.Fatal("loom: set SLACK_BOT_TOKEN (xoxb-) and SLACK_APP_TOKEN (xapp-)")
	}

	// Dial Sibyl's execution layer via Temporal.
	sibylClient, err := loom.NewTemporalSibyl(
		os.Getenv("TEMPORAL_HOSTPORT"),
		os.Getenv("SIBYL_TASK_QUEUE"),
	)
	if err != nil {
		log.Fatalf("loom: %v", err)
	}
	defer sibylClient.Close()

	// Shared correlation table + renderer for asynchronous result delivery.
	corr := loom.NewCorrelation()
	renderer := loom.NewRenderer(botToken)

	handler := loom.NewSibylHandler(loom.SibylHandlerConfig{
		Sibyl:       sibylClient,
		Correlation: corr,
		Renderer:    renderer,
		MaxRounds:   envInt("LOOM_MAX_ROUNDS", 3),
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

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
