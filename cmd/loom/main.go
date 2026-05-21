// Command loom runs the Slack front end: it opens a Socket Mode
// WebSocket to Slack and routes events to a handler.
//
// PR-1 wires the EchoHandler — it acknowledges mentions/messages with an
// "eyes" reaction and echoes the text back in-thread. No backend yet.
//
// Required environment:
//
//	SLACK_BOT_TOKEN   xoxb-...   (Web API: post, react, user info)
//	SLACK_APP_TOKEN   xapp-...   (Socket Mode: connections:write)
//
// Setup in the Slack app config:
//   - Enable Socket Mode
//   - Add an App-Level Token with connections:write  -> SLACK_APP_TOKEN
//   - OAuth & Permissions bot scopes: app_mentions:read, chat:write,
//     reactions:write, channels:history, im:history, commands (optional)
//   - Subscribe to bot events: app_mention, message.channels, message.im
//   - Install the app to the workspace                -> SLACK_BOT_TOKEN
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/vinodhalaharvi/loom"
)

func main() {
	botToken := os.Getenv("SLACK_BOT_TOKEN")
	appToken := os.Getenv("SLACK_APP_TOKEN")
	if botToken == "" || appToken == "" {
		log.Fatal("loom: set SLACK_BOT_TOKEN (xoxb-) and SLACK_APP_TOKEN (xapp-)")
	}

	listener, err := loom.NewListener(loom.Options{
		BotToken: botToken,
		AppToken: appToken,
		Handler:  loom.EchoHandler,
		Debug:    os.Getenv("LOOM_DEBUG") != "",
	})
	if err != nil {
		log.Fatalf("loom: %v", err)
	}

	// Cancel on SIGINT/SIGTERM for a clean shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("loom: starting Socket Mode listener…")
	if err := listener.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("loom: listener stopped: %v", err)
	}
	log.Println("loom: shut down")
}
