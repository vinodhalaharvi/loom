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
// each request-bearing event immediately, then dispatches handling to the
// bounded worker pool. Reading and acking never wait on handler work.
func (l *Listener) readLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-l.sm.Events:
			if !ok {
				return
			}
			// Ack first, within the 3s window, regardless of what the
			// handler will do. Events without a Request (lifecycle
			// events) carry no ack.
			if evt.Request != nil {
				l.sm.Ack(*evt.Request)
			}

			loomEvt, handled := translate(evt, l.botID)
			if !handled {
				continue
			}
			l.dispatch(ctx, loomEvt)
		}
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
