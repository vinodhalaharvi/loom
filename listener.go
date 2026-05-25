package loom

import (
	"context"
	"log"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
)

// Listener owns the Socket Mode WebSocket connection and the read loop.
// It receives events, acknowledges them immediately (Slack requires an
// ack within ~3s or it redelivers), and dispatches each to a handler on
// a bounded worker pool so a slow handler can't block reading the next
// event or delay acks.
type Listener struct {
	api      *slack.Client
	sm       *socketmode.Client
	handler  Handler
	botID    string
	workers  int
	sem      chan struct{}
	renderer *renderer
}

// Options configures a Listener.
type Options struct {
	// BotToken is the xoxb- token used for Web API calls (posting,
	// reactions, user info).
	BotToken string
	// AppToken is the xapp- app-level token (connections:write scope)
	// used to open the Socket Mode WebSocket.
	AppToken string
	// Handler processes each translated Event into a Reply. Required.
	Handler Handler
	// Workers bounds concurrent handler executions. Defaults to 8.
	Workers int
	// Debug enables slack-go's verbose logging.
	Debug bool
}

// NewListener constructs a Listener from Options. It does not connect;
// call Run to open the WebSocket and start the loop.
func NewListener(opts Options) (*Listener, error) {
	if opts.Handler == nil {
		return nil, errMissing("Handler")
	}
	if opts.BotToken == "" {
		return nil, errMissing("BotToken")
	}
	if opts.AppToken == "" {
		return nil, errMissing("AppToken")
	}
	workers := opts.Workers
	if workers <= 0 {
		workers = 8
	}

	api := slack.New(
		opts.BotToken,
		slack.OptionAppLevelToken(opts.AppToken),
		slack.OptionDebug(opts.Debug),
	)
	sm := socketmode.New(api, socketmode.OptionDebug(opts.Debug))

	return &Listener{
		api:      api,
		sm:       sm,
		handler:  opts.Handler,
		workers:  workers,
		sem:      make(chan struct{}, workers),
		renderer: &renderer{api: api},
	}, nil
}

// Run opens the WebSocket and processes events until ctx is cancelled or
// the connection fails unrecoverably. It resolves the bot's own user ID
// once on connect (used to filter the bot's own activity and strip
// mention tokens).
func (l *Listener) Run(ctx context.Context) error {
	// Resolve our own user ID so translate() can filter self-events.
	auth, err := l.api.AuthTestContext(ctx)
	if err != nil {
		return err
	}
	l.botID = auth.UserID
	log.Printf("loom: connected as %s (user %s) in team %s", auth.User, auth.UserID, auth.Team)

	// Read loop in its own goroutine; RunContext blocks managing the
	// connection.
	go l.readLoop(ctx)

	return l.sm.RunContext(ctx)
}

// readLoop is the single reader of the Socket Mode event channel. It acks
// each acknowledgeable event immediately, then dispatches handling to the
// bounded worker pool. Reading and acking never wait on handler work.
//
// CRITICAL: only data events (EventsAPI, SlashCommand, Interactive) may be
// acked. Lifecycle frames — hello, connecting, connected, disconnect,
// errors — must NOT be acked even though some (e.g. hello) carry a
// non-nil Request. Acking a hello sends a Socket Mode response with an
// empty envelope ID, which Slack rejects by closing the connection
// (1006 abnormal closure). That produced an endless connect → hello →
// ack → 1006 → reconnect loop in which no real events were ever
// delivered. Gating the ack on the event TYPE (not just Request != nil)
// is the fix.
func (l *Listener) readLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-l.sm.Events:
			if !ok {
				return
			}

			// Ack only the acknowledgeable data-event types. Everything
			// else (hello/connecting/connected/disconnect/errors) is a
			// lifecycle frame and must not be acked.
			if ackable(evt.Type) {
				if evt.Request != nil {
					l.sm.Ack(*evt.Request)
				}
			} else {
				// Lifecycle frame: never ack, never dispatch as a user
				// event.
				continue
			}

			loomEvt, handled := translate(evt, l.botID)
			if !handled {
				continue
			}
			l.dispatch(ctx, loomEvt)
		}
	}
}

// ackable reports whether a Socket Mode event type carries a request that
// must be acknowledged. ONLY data events qualify: EventsAPI,
// SlashCommand, Interactive. Lifecycle frames (hello, connecting,
// connected, disconnect, errors) must never be acked — acking a hello, in
// particular, sends a response with an empty envelope ID that Slack
// rejects by closing the socket (1006), causing an endless reconnect
// loop in which no events are ever delivered.
func ackable(t socketmode.EventType) bool {
	switch t {
	case socketmode.EventTypeEventsAPI,
		socketmode.EventTypeSlashCommand,
		socketmode.EventTypeInteractive:
		return true
	default:
		return false
	}
}

// dispatch runs the handler for one event on the bounded pool. If all
// workers are busy it blocks briefly on the semaphore — honest
// backpressure rather than unbounded goroutine growth.
func (l *Listener) dispatch(ctx context.Context, e Event) {
	select {
	case <-ctx.Done():
		return
	case l.sem <- struct{}{}:
	}
	go func() {
		defer func() { <-l.sem }()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("loom: handler panic on %s event: %v", e.Kind, r)
			}
		}()

		reply, err := l.handler(ctx, e)
		if err != nil {
			log.Printf("loom: handler error on %s event: %v", e.Kind, err)
			return
		}
		if reply.IsEmpty() {
			return
		}
		if err := l.renderer.render(ctx, e, reply); err != nil {
			log.Printf("loom: render error: %v", err)
		}
	}()
}
